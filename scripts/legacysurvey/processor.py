"""Class and utility functions to process original legacy survey data"""

import os.path
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List

import healpy
import numpy as np
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.nddata import Cutout2D
from astropy.table import Row, Table
from astropy.wcs import WCS
from PIL import Image, ImageOps

IMAGE_BANDS = [
    "image-g",
    "image-r",
    "image-i",
    "image-z",
]
INVAR_BANDS = [
    "invvar-g",
    "invvar-r",
    "invvar-i",
    "invvar-z",
]
BANDS = IMAGE_BANDS + INVAR_BANDS + ["maskbits"]
NEARBY_CATALOG_INFORMATION = [
    "FLUX_G",
    "FLUX_R",
    "FLUX_I",
    "FLUX_Z",
    "FLUX_IVAR_G",
    "FLUX_IVAR_R",
    "FLUX_IVAR_I",
    "FLUX_IVAR_Z",
    "TYPE",
    "RA",
    "DEC",
]
OBJECT_TYPE_COLOR = {
    name: i
    for i, name in enumerate(["PSF", "REX", "EXP", "DEV", "SER", "DUP"], start=1)
}
HEALPIX_NDISE = 16
CUTOUT_SIZE = 160


def compute_magnitude(flux: np.ndarray, transmission: np.ndarray):
    magnitude = np.zeros_like(flux)
    positive_flux_indices = flux > 0
    magnitude[positive_flux_indices] = 22.5 - 2.5 * np.log10(
        flux[positive_flux_indices] / transmission[positive_flux_indices]
    )
    return magnitude


def load_blob_model_image(dir: str, brick_name: str) -> np.ndarray:
    brick_group = brick_name[:3]
    image_filename = os.path.join(
        dir, brick_group, brick_name, f"legacysurvey-{brick_name}-blobmodel.jpg"
    )
    image = Image.open(image_filename)
    image = ImageOps.flip(image)
    image = np.array(image)
    # Reshape C, H, W
    image = np.moveaxis(image, -1, 0)
    return image


def load_images(dir: str, brick_name: str) -> Dict[str, fits.ImageHDU]:
    images = {}
    brick_group = brick_name[:3]
    for band in BANDS:
        image_filename = os.path.join(
            dir, brick_group, brick_name, f"legacysurvey-{brick_name}-{band}.fits.fz"
        )
        with fits.open(image_filename) as hdul:
            image = hdul[1].copy()
            images[band] = image

    # Post processing the mask to make it binary
    data = images["maskbits"].data
    maskclean = np.ones_like(data, dtype=bool)
    set_maskbits = [0, 1, 2, 3, 4, 5, 6, 7, 11, 14, 15]
    for bit in set_maskbits:
        maskclean &= (data & 2**bit) == 0
    images["maskbits"].data = maskclean.astype(data.dtype)
    return images


@dataclass
class ObjectInformation:
    id: str
    group_id: int
    image: np.ndarray
    invvar: np.ndarray
    bit_mask: np.ndarray
    image_psf_fwhm: np.ndarray
    image_model: np.ndarray
    object_mask: np.ndarray
    catalog: Dict[str, Any]


class CutoutCatalogSelector:
    """Given a cutout retrieve the brightest elements of the catalog in the cutout."""

    def __init__(self, catalog: Table, cutout: Cutout2D):
        self.cutout = cutout
        center_coordinates = cutout.wcs.wcs_pix2world(*cutout.input_position_cutout, 1)
        self.center_coordinates = SkyCoord(
            ra=center_coordinates[0] * u.deg, dec=center_coordinates[1] * u.deg
        )
        self.original_catalog = catalog
        # Retrieve the objects in the catalog that lie in the cutout
        self.catalog = self.select()

    def get_pixel_separation(self, catalog_coordinates: SkyCoord) -> np.ndarray:
        separations = self.center_coordinates.separation(catalog_coordinates)
        pixel_separations = separations / u.deg * u.deg.to(u.arcsec) / ARCSEC_PER_PIXEL
        return pixel_separations

    def get_within_cutout(self, catalog_coordinates: SkyCoord):
        i, j = self.cutout.wcs.world_to_array_index(catalog_coordinates)
        object_pixel_coordinates = np.array([j, i])
        # Bbox is ((ymin, ymax), (xmin, xmax))
        cutout_bbox = self.cutout.bbox_cutout
        lower = np.logical_and(
            object_pixel_coordinates[0] >= cutout_bbox[1][0],
            object_pixel_coordinates[1] >= cutout_bbox[0][0],
        )
        upper = np.logical_and(
            object_pixel_coordinates[0] < cutout_bbox[1][1],
            object_pixel_coordinates[1] < cutout_bbox[0][1],
        )
        within_cutout = np.logical_and(lower, upper)
        return within_cutout

    def select(self) -> Table:
        catalog_coordinates = SkyCoord(
            ra=self.original_catalog["RA"], dec=self.original_catalog["DEC"]
        )
        # First select coordinates within the circle
        # centered in the cutout and whose raidus equals its diagonal
        pixel_separations = self.get_pixel_separation(catalog_coordinates)
        close_object_indices = pixel_separations < (
            np.sqrt(2) * np.linalg.norm(self.cutout.shape)
        )
        close_object_coordinates = catalog_coordinates[close_object_indices]
        # Then retrieve objects that actually lie in the cutout
        in_cutout_indices = self.get_within_cutout(close_object_coordinates)
        close_object_catalog = self.original_catalog[close_object_indices]
        in_cutout_object_catalog = close_object_catalog[in_cutout_indices]
        return in_cutout_object_catalog

    def get_object_mask(self) -> np.ndarray:
        mask = np.zeros(self.cutout.shape).astype(np.uint8)
        # Get catalob object pixel coordinates in cutout
        object_coordinates = SkyCoord(ra=self.catalog["RA"], dec=self.catalog["DEC"])
        i, j = self.cutout.wcs.world_to_array_index(object_coordinates)
        centers = np.stack([j, i]).T
        radii = self.catalog["SHAPE_R"].value / ARCSEC_PER_PIXEL
        object_types = self.catalog["TYPE"].value.astype(str)
        for c, r, t in zip(centers, radii, object_types):
            # Enforce a minimum size of the disk mask
            r = max(2, r)
            rr, cc = skimage.draw.disk(c, r, shape=self.cutout.shape)
            mask[cc, rr] = OBJECT_TYPE_COLOR[t]
        return mask

    def get_brightest_object_catalog(
        self, n_objects: int = 20
    ) -> Dict[str, List[float]]:
        self.catalog.sort(keys="MAG_Z", reverse=True)
        brightest_object_data = {key: [] for key in NEARBY_CATALOG_INFORMATION}
        brightest_object_catalog = self.catalog[:n_objects]
        # Check there is at least one object
        # The center object should at least be in the catalog
        assert (
            len(brightest_object_catalog) > 0
        ), "The nearby catalog should at least contain one object."

        for obj in brightest_object_catalog:
            for key in NEARBY_CATALOG_INFORMATION:
                if key == "TYPE":
                    data = OBJECT_TYPE_COLOR[obj[key]]
                else:
                    data = obj[key]
                brightest_object_data[key].append(data)

        # Pad with zeros data if necessary
        for _ in range(len(brightest_object_catalog), n_objects):
            for key in NEARBY_CATALOG_INFORMATION:
                brightest_object_data[key].append(0)

        return brightest_object_data


class CatalogProcessor:
    """Given a legacy survey sweep catalog,
    filters objects on their magnitude and yield healpix splits.

    """

    def __init__(self, catalog_filename: str):
        self.catalog_filename = catalog_filename
        self.catalog = Table.read(self.catalog_filename)
        filter_indices = self.filter()
        self.catalog = self.catalog[filter_indices]

    def filter(self, zmag_cut=21.0) -> List[bool]:
        """Selection function applied to retrieve relevant observation from the DECaLS DR10 South catalog.
        Observation are deemed relevant based on minimum magnitude, availability in all bands and bit masks.
        """
        # Magnitude cut
        magnitude = compute_magnitude(
            self.catalog["FLUX_Z"].value, self.catalog["MW_TRANSMISSION_Z"].value
        )
        self.catalog["MAG_Z"] = magnitude
        mask_mag = magnitude < zmag_cut

        # Require observations in all bands
        flux_bands = ["G", "R", "I", "Z"]
        nobs = np.array([self.catalog["NOBS_" + fb] for fb in flux_bands]).T
        mask_obs = ~np.any(nobs == 0, axis=1)

        # Remove point sources
        mask_type = self.catalog["TYPE"] != "PSF"

        # Quality cuts
        # See definition of mask bits here:
        # https://www.legacysurvey.org/dr10/bitmasks/
        # This will remove all objects on the borders of images
        # or directly affected by brigh stars or saturating any of the bands
        maskbits = [0, 1, 2, 3, 4, 5, 6, 7, 11, 14, 15]
        mask_clean = np.ones(len(self.catalog), dtype=bool)
        m = self.catalog["MASKBITS"]
        for bit in maskbits:
            mask_clean &= (m & 2**bit) == 0

        return mask_mag & mask_clean & mask_obs & mask_type

    def generate_healpix(self) -> Iterator[Table]:
        """Returns a Iterator that yields subcatalogs grouped by healpix id."""
        self.catalog["HEALPIX"] = healpy.ang2pix(
            HEALPIX_NDISE,
            self.catalog["RA"],
            self.catalog["DEC"],
            lonlat=True,
            nest=True,
        )
        groups = self.catalog.group_by("HEALPIX").groups
        yield from groups


class HealpixProcessor:
    """Process catalog in a given healpix region."""

    def __init__(self, catalog: Table):
        self.catalog = catalog
        self.id = int(self.catalog["HEALPIX"][0])

    def generate_bricks(self) -> Iterator[Table]:
        bricks = self.catalog.group_by("BRICKNAME").groups
        yield from bricks


class BrickProcessor:
    """Process catalog at the brick level and retrieve associated information."""

    def __init__(self, data_dir: str, catalog: Table):
        self.catalog = catalog
        self.brick_name = str(self.catalog["BRICKNAME"][0])
        self.images = load_images(data_dir, self.brick_name)
        self.blob_model_image = load_blob_model_image(data_dir, self.brick_name)

    def process_object(self, obj: Row):
        """Retrieve all the information relative to a catalog object."""
        # Create a cutout for each band
        ra, dec = obj["RA"], obj["DEC"]
        wcs = WCS(self.images["image-g"].header)
        x, y = wcs.all_world2pix(ra, dec, 1)
        position = (x, y)
        size = (CUTOUT_SIZE, CUTOUT_SIZE)

        # Build image
        image = [
            Cutout2D(self.images[band].data, position, size, wcs=wcs)
            for band in IMAGE_BANDS
        ]
        image = np.stack(image, axis=0)

        # Build inverse variance
        invvar = [
            Cutout2D(self.images[band].data, position, size, wcs=wcs)
            for band in INVAR_BANDS
        ]
        invvar = np.stack(invvar, axis=0)

        # Build cutout catalog and mask
        cutout = Cutout2D(self.images["image-i"].data, position, size, wcs=wcs)
        catalog_selector = CutoutCatalogSelector(self.catalog, cutout)
        cutout_mask = catalog_selector.get_object_mask()
        cutout_catalog = catalog_selector.get_brightest_object_catalog()

        # Build model image
        cutout_model_image = np.stack(
            [
                Cutout2D(channel, position, size, wcs=wcs).data
                for channel in self.blob_model_image
            ]
        )
        cutout_model_image = np.moveaxis(cutout_model_image, 0, -1)

        # Build mask
        bit_mask = Cutout2D(self.images["maskbits"].data, position, size, wcs=wcs).data

        # Image PSF FWHM
        image_psf_fwhm = np.array(
            [obj[f"PSFSIZE_{band}"] for band in ["G", "R", "I", "Z"]]
        )

        return ObjectInformation(
            f"{obj['BRICKNAME']}-{obj['OBJID']}",
            0,
            image,
            invvar,
            bit_mask,
            image_psf_fwhm,
            cutout_model_image,
            cutout_mask,
            cutout_catalog,
        )

    def generate_objects(self) -> Iterator[ObjectInformation]:
        for obj in self.catalog:
            obj_information = self.process_object(obj)
            yield obj_information

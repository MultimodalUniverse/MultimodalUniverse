"""Utils for generating COSMOS cutouts."""

from astropy import coordinates
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.nddata import Cutout2D
import astropy.units as u
import astropy.wcs
from astropy.wcs import WCS
import numpy as np
import pandas as pd

HEALPIX_NSIDE = 16.

def ivar_from_wht_map(
    wht_map, read_noise, sky_brightness, dark_current, pixel_scale, num_exposures
):
    """ Calculate inverse variance from the weight (exposure) map and noise
        in units of e-/s.

    Args:
        wht_map (np.array(float), size=(npix,npix)): exposure time map
        read_noise (float): read noise in e-
        sky_brightness (float): sky brightness in e-/arcsecond^2
        dark_current (float): mean dark current in e-/pixel/sec
        pixel_scale (float): arcsec per pixel
        num_exposure (int): number of exposures added together for image.
    """
    exposure_time = wht_map

    # add readout noise for each time readout happens (i.e. 1 per exposure
    # used). Units of electrons.
    read_noise_tot = num_exposures * (read_noise) ** 2

    # Poisson dark current in e-/pixel
    dark_noise_per_sec = exposure_time * dark_current

    # skybrightness assuming poisson counts and sky brightness
    # is in e-/(1"x1"). Final total in electrons / pixel.
    sky_brightness_tot =  sky_brightness * pixel_scale ** 2

    sigma_bkg = np.sqrt(
        read_noise_tot + sky_brightness_tot + dark_noise_per_sec
    )

    # calculate inverse variance in units of electrons / s.
    ivar_map = (exposure_time / sigma_bkg) ** 2

    return ivar_map


def _extract_header_properties(file_header, data_header, wcs):
    """ Extract noise properties and pixel scale from fits headers.

    Args:
        file_header: fits (metadata) file header.
        data_header: fits data file header.
        wcs (astropy.wcs.WCS): world coordiante system for fits file.

    Returns:
        (float, float, float, float): pixel scale, mdrizsky (sky brightness),
            ccdgain, and readnoise.
    """
    # Get the pixel scale.
    pixel_scale = np.sqrt(np.abs(np.linalg.det(wcs.wcs.cd))) * 3600

    # Get params for noise model
    mdrizsky = data_header['MDRIZSKY']
    ccd_gain  = file_header['CCDGAIN'] # ccd gain in e-/counts
    avg_read_noise = np.mean(
        [file_header['READNSEA'],file_header['READNSEB'],
        file_header['READNSEC'],file_header['READNSED']]
    )

    return pixel_scale, mdrizsky, ccd_gain, avg_read_noise


def get_file_for_targets(galaxy_df, fits_file_list, col_name='fits_file'):
    """
    Args:
        galaxy_df (pd.DataFrame): pandas DataFrame containing the ra and dec for
            every galaxy in the catalog.
        fite_file_list (list(str)): path for fits files to search through for
            matches with the catalog.
        column name to store list of fits files for each object in the catalog.

    Returns:
        (pd.DataFrame) modified dataframe with fits file for each object in the
            catalog. Will also append fits properties (MDRIZSKY, CCDGAIN, and
            average read noise).
    """

    # list of fits files for each object to be filled in.
    fits_paths = np.empty(len(galaxy_df), dtype=object)
    # also fill in pixel scale, and noise params
    pixel_scales = np.empty(len(galaxy_df))
    mdrizsky_vals = np.empty(len(galaxy_df))
    ccdgain_vals = np.empty(len(galaxy_df))
    avg_read_noise_vals = np.empty(len(galaxy_df))

    # all the ra and decs for our catalog.
    ra_list = galaxy_df['RA'].to_numpy()
    dec_list = galaxy_df['DEC'].to_numpy()
    all_coords = SkyCoord(ra=ra_list * u.deg, dec=dec_list * u.deg)

    # go through all the fits files and get the objects that fall in each one.
    for fits_path in fits_file_list:

        # open the file.
        with fits.open(fits_path) as hdu:
            file_header = hdu[0].header
            data_header = hdu[1].header
            wcs = WCS(data_header, hdu)

        # extract fits properties.
        pixel_scale, mdrizsky, ccd_gain, avg_read_noise = (
            _extract_header_properties(file_header, data_header, wcs)
        )

        # get the pixel coordinate for each
        xp, yp = astropy.wcs.utils.skycoord_to_pixel(all_coords, wcs)

        # mask for inside the image.
        mask = np.logical_and(xp > 0.0, xp < data_header['NAXIS1'])
        mask = np.logical_and(
            mask, np.logical_and(yp > 0.0, yp < data_header['NAXIS2'])
        )

        fits_paths[mask] = fits_path
        pixel_scales[mask] = pixel_scale
        mdrizsky_vals[mask] = mdrizsky
        ccdgain_vals[mask] = ccd_gain
        avg_read_noise_vals[mask] = avg_read_noise

    # write the paths.
    galaxy_df[col_name] = fits_paths
    # write pixel_scale + noise params
    galaxy_df['pixel_scale'] = pixel_scales
    galaxy_df['MDRIZSKY'] = mdrizsky_vals
    galaxy_df['CCDGAIN'] = ccdgain_vals
    galaxy_df['AVG_READNSE'] = avg_read_noise_vals

    return galaxy_df



def make_single_band_cutout(
    galaxy_ra: float, galaxy_dec: float, data: np.array, wcs: astropy.wcs.WCS,
    cutout_size: float
):
    """ Given a large tile, makes a cutout centered at galaxy_ra,galaxy_dec

    Note:
        - ra,dec in degrees, cutout_size in arcsec
    Args:
        galaxy_ra (degrees): galaxy ra in degrees.
        galaxy_dec (degrees): galaxy dec in degrees.
        data (array): data array pulled from fits file.
        wcs (astropy.wcs.WCS): world coordinate system for data.
        cutout_size (arcsec): desired cutout size.
    """
    # add astropy units to inputs
    ra = galaxy_ra * u.deg
    dec = galaxy_dec * u.deg
    size = cutout_size * u.arcsec

    # Create a list of SkyCoords.
    coord = SkyCoord(ra = ra, dec = dec, frame = 'icrs', unit='deg')

    # Create the Cutout
    cutout = Cutout2D(
        data, coord, size = (size, size), wcs = wcs, mode = 'partial'
    ).data

    return cutout

def make_cutout_single(
    idx, targets_df, flux_tile, weight_tile, wcs_tile, nan_tolerance,
    zero_tolerance, cutout_size, dark_current, num_exposures=1
):
    """Helper function for map of make_cutouts.

    Args:
        idx (int): index of target to consider.
        targets_df (pd.DataFrame): dataframe with ra and decs for cutouts.
        flux_tile (np.array[float], size=(npix,npix)): image for full tile.
        weight_tile (np.array[float], size=(npix,npix)): weight for full
            tile.
        wcs_tile (astropy.wcs.WCS): wcs of the image / weights tile.
        nan_tolerance (float): tolerance to nans. If 1.0 all values can be
            nans.
        zero_tolerance (float): tolerance to zeros. If 1.0 all values can
            be zeros.
        cutout_size (float): approximate cutout size in arcseconds.
        dark_current (float): mean dark current in e-/pixel/sec
        num_exposures (int): number of exposures added together by drizzle.

    Returns:
        (np.array, np.array, np.array, int): flux cutout, weight cutout, image mask, index=idx.
    """
    ra_target = targets_df.loc[idx, 'RA'].item()
    dec_target = targets_df.loc[idx, 'DEC'].item()

    flux = make_single_band_cutout(
        ra_target, dec_target, flux_tile, wcs_tile, cutout_size
    )
    weights = make_single_band_cutout(
        ra_target, dec_target, weight_tile, wcs_tile, cutout_size
    )

    # calculate ivar from flux.
    ivar = ivar_from_wht_map(
        weights,
        read_noise=targets_df.loc[idx,'AVG_READNSE'],
        sky_brightness=targets_df.loc[idx,'MDRIZSKY'],
        dark_current=dark_current,
        pixel_scale=targets_df.loc[idx,'pixel_scale'],
        num_exposures=num_exposures
    )

    nan_frac = np.sum(np.isnan(flux)) / flux.size
    zeros_frac = np.sum(np.abs(flux) < 1e-6) / flux.size

    # skip invalid cutouts
    if nan_frac > nan_tolerance or zeros_frac > zero_tolerance:
        return None
    
    # compute image mask
    image_mask = (~np.isnan(flux)) & (weights > 1e-6)

    # set nans to zero
    flux = np.nan_to_num(flux)

    return (flux, ivar, image_mask, idx)

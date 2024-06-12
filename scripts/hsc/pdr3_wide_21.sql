--
-- Full query of all objects above mag 22.5 in HSC wide that meet basic quality flags
--
SELECT object_id, ra, dec, tract, patch,
-- Absorption
a_g, a_r, a_i, a_z, a_y,
-- Extendedness
g_extendedness_value, 
r_extendedness_value, 
i_extendedness_value, 
z_extendedness_value, 
y_extendedness_value,
-- Local variance
g_variance_value,
r_variance_value,
i_variance_value,
z_variance_value,
y_variance_value,
-- Magnitudes
g_cmodel_mag, g_cmodel_magerr,
r_cmodel_mag, r_cmodel_magerr,
i_cmodel_mag, i_cmodel_magerr,
z_cmodel_mag, z_cmodel_magerr,
y_cmodel_mag, y_cmodel_magerr,
-- Fluxes
g_cmodel_flux, g_cmodel_fluxerr,
r_cmodel_flux, r_cmodel_fluxerr,
i_cmodel_flux, i_cmodel_fluxerr,
z_cmodel_flux, z_cmodel_fluxerr,
y_cmodel_flux, y_cmodel_fluxerr,
-- PSF information
g_sdssshape_psf_shape11, g_sdssshape_psf_shape22, g_sdssshape_psf_shape12, 
r_sdssshape_psf_shape11, r_sdssshape_psf_shape22, r_sdssshape_psf_shape12, 
i_sdssshape_psf_shape11, i_sdssshape_psf_shape22, i_sdssshape_psf_shape12, 
z_sdssshape_psf_shape11, z_sdssshape_psf_shape22, z_sdssshape_psf_shape12, 
y_sdssshape_psf_shape11, y_sdssshape_psf_shape22, y_sdssshape_psf_shape12,
-- Object shape
g_sdssshape_shape11, g_sdssshape_shape22, g_sdssshape_shape12, 
r_sdssshape_shape11, r_sdssshape_shape22, r_sdssshape_shape12, 
i_sdssshape_shape11, i_sdssshape_shape22, i_sdssshape_shape12, 
z_sdssshape_shape11, z_sdssshape_shape22, z_sdssshape_shape12, 
y_sdssshape_shape11, y_sdssshape_shape22, y_sdssshape_shape12
FROM pdr3_wide.forced forced
LEFT JOIN pdr3_wide.forced2 USING (object_id)
LEFT JOIN pdr3_wide.forced3 USING (object_id)
-- Applying some data quality cuts
WHERE forced.isprimary
AND corrected_z_cmodel_mag < 21
-- Remove point sources
AND i_extendedness_value > 0.5
-- Simple Full Depth Full Colour cuts: At least 3 exposures in each band
AND forced.g_inputcount_value >= 3
AND forced.r_inputcount_value >= 3
AND forced.i_inputcount_value >= 3
AND forced.z_inputcount_value >= 3
AND forced.y_inputcount_value >= 3
-- Remove objects affected by bright stars
AND NOT forced.g_pixelflags_bright_objectcenter
AND NOT forced.r_pixelflags_bright_objectcenter
AND NOT forced.i_pixelflags_bright_objectcenter
AND NOT forced.z_pixelflags_bright_objectcenter
AND NOT forced.y_pixelflags_bright_objectcenter
AND NOT forced.g_pixelflags_bright_object
AND NOT forced.r_pixelflags_bright_object
AND NOT forced.i_pixelflags_bright_object
AND NOT forced.z_pixelflags_bright_object
AND NOT forced.y_pixelflags_bright_object
-- Remove objects intersecting edges
AND NOT forced.g_pixelflags_edge
AND NOT forced.r_pixelflags_edge
AND NOT forced.i_pixelflags_edge
AND NOT forced.z_pixelflags_edge
AND NOT forced.y_pixelflags_edge
-- Remove objects with saturated or interpolated pixels
AND NOT forced.g_pixelflags_saturatedcenter
AND NOT forced.r_pixelflags_saturatedcenter
AND NOT forced.i_pixelflags_saturatedcenter
AND NOT forced.z_pixelflags_saturatedcenter
AND NOT forced.y_pixelflags_saturatedcenter
AND NOT forced.g_pixelflags_crcenter
AND NOT forced.r_pixelflags_crcenter
AND NOT forced.i_pixelflags_crcenter
AND NOT forced.z_pixelflags_crcenter
AND NOT forced.y_pixelflags_crcenter
AND NOT forced.g_pixelflags_interpolatedcenter
AND NOT forced.r_pixelflags_interpolatedcenter
AND NOT forced.i_pixelflags_interpolatedcenter
AND NOT forced.z_pixelflags_interpolatedcenter
AND NOT forced.y_pixelflags_interpolatedcenter
AND NOT forced.g_pixelflags_bad
AND NOT forced.r_pixelflags_bad
AND NOT forced.i_pixelflags_bad
AND NOT forced.z_pixelflags_bad
AND NOT forced.y_pixelflags_bad
-- Remove objects with unreliable cmodel magnitudes
AND NOT forced.g_cmodel_flag
AND NOT forced.r_cmodel_flag
AND NOT forced.i_cmodel_flag
AND NOT forced.z_cmodel_flag
AND NOT forced.y_cmodel_flag
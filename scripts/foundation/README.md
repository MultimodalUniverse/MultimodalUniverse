Cosmologically Useful Type Ia Supernovae from the Foundation First Data Release
===============================================================================

Data curators: D. O. Jones, D. Scolnic, M. Foley

DR1 authors: Foley, R. J., Scolnic, D., Rest, A., Jha, S. W., Pan, Y. -C., Riess, A. G., Challis, P., Chambers, K. C., Coulter, D. A., Dettman, K. G., Foley, M. M., Fox, O. D., Huber, M. E., Jones, D. O., Kilpatrick, C. D., Kirshner, R. P., Schultz, A. S. B., Siebert, M. R., Flewelling, H. A., Gibson, B. Magnier, E. A., Miller, J. A., Primak, N., Smartt, S. J., Smith, K. W., Wainscoat, R. J., Waters, C., Willman, M.

This directory contains the set of cosmologically useful Type Ia supernovae published in the Foundation Supernova Survey First Data Release (Foley et al. 2018).  The full set is 180 SN Ia, 175 of which are at z > 0.015 and pass cosmology cuts (standard shape, color, shape uncertainty, and time of max uncertainty cuts) when fit using the SALT2.JLA-B14 model in SNANA.

We have added a 1.5% error floor in quadrature to the griz photometry following Jones et al. (2019), who found by using survey simulations that uncertainties were likely slightly underestimated at the bright end.  We also suggest using the Pan-STARRS filter functions from Jones et al. (2019) in $SNDATA_ROOT/kcor/Foundation_DR1, who correct for color-dependent biases in the g band due to PSF-fitting photometry (also included in the github repo as kcor_PS1_none.fits).

Peculiar velocities are from the model of Carrick et al. (2015).  Milky Way reddening includes the Schlafly & Finkbeiner (2011) corrections to the dust maps of Schlegel, Finkbeiner & Davis (1998).  The quoted uncertainties on MW E(B-V) are 5%.  Flux zeropoints are the standard SNANA value of 27.5.

Foundation host galaxy masses use ZPEG SED-fitting as described in Jones et al. (2018), the paper referenced below, with the exception that the photometry also includes GALEX, 2MASS, SDSS and WISE data.  Uncertainties are estimated from running ZPEG on Monte Carlo-sampled photometry; as a result, they are model-dependent and often underestimated, particularly for bright galaxies.

When using these data, please cite:

Foley et al. (2018) - https://ui.adsabs.harvard.edu/abs/2018MNRAS.475..193F

Jones et al. (2019) - https://ui.adsabs.harvard.edu/abs/2019ApJ...881...19J

Please contact David Jones with any questions.  You may also raise an issue on github, github.com/djones1040/Foundation_DR1.


Analyses using these data
-------------------------
The Foundation Supernova Survey: motivation, design, implementation, and first data release

Foley, Ryan J., Scolnic, Daniel, Rest, Armin et al., 2018, MNRAS, 475, 193F

Should Type Ia Supernova Distances be Corrected for their Local Environments?

Jones, D. O., Riess, A. G., Scolnic, D. M. et al., 2018, ApJ, 867, 108J

The Foundation Supernova Survey: Measuring Cosmological Parameters using Supernovae from a Single Telescope

Jones, D. O., Scolnic, D. M., Foley, R. J. et al., 2019, ApJ, 881, 19J


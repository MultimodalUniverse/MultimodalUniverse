SDSS_GLOBUS_UUID="f8362eaf-fc40-451c-8c44-50b71ec7f247"
MY_GLOBUS_UUID="XXXXXXXXXXX" # put your globus endpoint uuid here
MY_DEST_PATH="ceph/data/astro/apogee_dr17" # for example...

globus transfer --recursive --include "apStar-*.fits" --exclude "*" --sync-level size --verbose ${SDSS_GLOBUS_UUID}:dr17/apogee/spectro/redux/dr17/stars/apo25m ${MY_GLOBUS_UUID}:${MY_DEST_PATH}/spectro/redux/dr17/stars/apo25m
globus transfer --recursive --include "asStar-*.fits" --exclude "*" --sync-level size --verbose ${SDSS_GLOBUS_UUID}:dr17/apogee/spectro/redux/dr17/stars/lco25m ${MY_GLOBUS_UUID}:${MY_DEST_PATH}/spectro/redux/dr17/stars/lco25m

globus transfer --recursive --include "aspcapStar-*.fits" --exclude "*" --sync-level size --verbose ${SDSS_GLOBUS_UUID}:dr17/apogee/spectro/aspcap/dr17/synspec_rev1/apo25m ${MY_GLOBUS_UUID}:${MY_DEST_PATH}/spectro/aspcap/dr17/synspec_rev1/apo25m
globus transfer --recursive --include "aspcapStar-*.fits" --exclude "*" --sync-level size --verbose ${SDSS_GLOBUS_UUID}:dr17/apogee/spectro/aspcap/dr17/synspec_rev1/lco25m ${MY_GLOBUS_UUID}:${MY_DEST_PATH}/spectro/aspcap/dr17/synspec_rev1/lco25m

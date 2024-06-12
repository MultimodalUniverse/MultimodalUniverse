import numpy as np

import datasets
from datasets import Features, Sequence, Value
from datasets.data_files import DataFilesPatternsDict
import h5py

_CITATION = """\
@ARTICLE{2023A&A...674A...1G,
       author = {{Gaia Collaboration} and {Vallenari}, A. and {Brown}, A.~G.~A. and {Prusti}, T. and {de Bruijne}, J.~H.~J. and {Arenou}, F. and {Babusiaux}, C. and {Biermann}, M. and {Creevey}, O.~L. and {Ducourant}, C. and {Evans}, D.~W. and {Eyer}, L. and {Guerra}, R. and {Hutton}, A. and {Jordi}, C. and {Klioner}, S.~A. and {Lammers}, U.~L. and {Lindegren}, L. and {Luri}, X. and {Mignard}, F. and {Panem}, C. and {Pourbaix}, D. and {Randich}, S. and {Sartoretti}, P. and {Soubiran}, C. and {Tanga}, P. and {Walton}, N.~A. and {Bailer-Jones}, C.~A.~L. and {Bastian}, U. and {Drimmel}, R. and {Jansen}, F. and {Katz}, D. and {Lattanzi}, M.~G. and {van Leeuwen}, F. and {Bakker}, J. and {Cacciari}, C. and {Casta{\~n}eda}, J. and {De Angeli}, F. and {Fabricius}, C. and {Fouesneau}, M. and {Fr{\'e}mat}, Y. and {Galluccio}, L. and {Guerrier}, A. and {Heiter}, U. and {Masana}, E. and {Messineo}, R. and {Mowlavi}, N. and {Nicolas}, C. and {Nienartowicz}, K. and {Pailler}, F. and {Panuzzo}, P. and {Riclet}, F. and {Roux}, W. and {Seabroke}, G.~M. and {Sordo}, R. and {Th{\'e}venin}, F. and {Gracia-Abril}, G. and {Portell}, J. and {Teyssier}, D. and {Altmann}, M. and {Andrae}, R. and {Audard}, M. and {Bellas-Velidis}, I. and {Benson}, K. and {Berthier}, J. and {Blomme}, R. and {Burgess}, P.~W. and {Busonero}, D. and {Busso}, G. and {C{\'a}novas}, H. and {Carry}, B. and {Cellino}, A. and {Cheek}, N. and {Clementini}, G. and {Damerdji}, Y. and {Davidson}, M. and {de Teodoro}, P. and {Nu{\~n}ez Campos}, M. and {Delchambre}, L. and {Dell'Oro}, A. and {Esquej}, P. and {Fern{\'a}ndez-Hern{\'a}ndez}, J. and {Fraile}, E. and {Garabato}, D. and {Garc{\'\i}a-Lario}, P. and {Gosset}, E. and {Haigron}, R. and {Halbwachs}, J. -L. and {Hambly}, N.~C. and {Harrison}, D.~L. and {Hern{\'a}ndez}, J. and {Hestroffer}, D. and {Hodgkin}, S.~T. and {Holl}, B. and {Jan{\ss}en}, K. and {Jevardat de Fombelle}, G. and {Jordan}, S. and {Krone-Martins}, A. and {Lanzafame}, A.~C. and {L{\"o}ffler}, W. and {Marchal}, O. and {Marrese}, P.~M. and {Moitinho}, A. and {Muinonen}, K. and {Osborne}, P. and {Pancino}, E. and {Pauwels}, T. and {Recio-Blanco}, A. and {Reyl{\'e}}, C. and {Riello}, M. and {Rimoldini}, L. and {Roegiers}, T. and {Rybizki}, J. and {Sarro}, L.~M. and {Siopis}, C. and {Smith}, M. and {Sozzetti}, A. and {Utrilla}, E. and {van Leeuwen}, M. and {Abbas}, U. and {{\'A}brah{\'a}m}, P. and {Abreu Aramburu}, A. and {Aerts}, C. and {Aguado}, J.~J. and {Ajaj}, M. and {Aldea-Montero}, F. and {Altavilla}, G. and {{\'A}lvarez}, M.~A. and {Alves}, J. and {Anders}, F. and {Anderson}, R.~I. and {Anglada Varela}, E. and {Antoja}, T. and {Baines}, D. and {Baker}, S.~G. and {Balaguer-N{\'u}{\~n}ez}, L. and {Balbinot}, E. and {Balog}, Z. and {Barache}, C. and {Barbato}, D. and {Barros}, M. and {Barstow}, M.~A. and {Bartolom{\'e}}, S. and {Bassilana}, J. -L. and {Bauchet}, N. and {Becciani}, U. and {Bellazzini}, M. and {Berihuete}, A. and {Bernet}, M. and {Bertone}, S. and {Bianchi}, L. and {Binnenfeld}, A. and {Blanco-Cuaresma}, S. and {Blazere}, A. and {Boch}, T. and {Bombrun}, A. and {Bossini}, D. and {Bouquillon}, S. and {Bragaglia}, A. and {Bramante}, L. and {Breedt}, E. and {Bressan}, A. and {Brouillet}, N. and {Brugaletta}, E. and {Bucciarelli}, B. and {Burlacu}, A. and {Butkevich}, A.~G. and {Buzzi}, R. and {Caffau}, E. and {Cancelliere}, R. and {Cantat-Gaudin}, T. and {Carballo}, R. and {Carlucci}, T. and {Carnerero}, M.~I. and {Carrasco}, J.~M. and {Casamiquela}, L. and {Castellani}, M. and {Castro-Ginard}, A. and {Chaoul}, L. and {Charlot}, P. and {Chemin}, L. and {Chiaramida}, V. and {Chiavassa}, A. and {Chornay}, N. and {Comoretto}, G. and {Contursi}, G. and {Cooper}, W.~J. and {Cornez}, T. and {Cowell}, S. and {Crifo}, F. and {Cropper}, M. and {Crosta}, M. and {Crowley}, C. and {Dafonte}, C. and {Dapergolas}, A. and {David}, M. and {David}, P. and {de Laverny}, P. and {De Luise}, F. and {De March}, R. and {De Ridder}, J. and {de Souza}, R. and {de Torres}, A. and {del Peloso}, E.~F. and {del Pozo}, E. and {Delbo}, M. and {Delgado}, A. and {Delisle}, J. -B. and {Demouchy}, C. and {Dharmawardena}, T.~E. and {Di Matteo}, P. and {Diakite}, S. and {Diener}, C. and {Distefano}, E. and {Dolding}, C. and {Edvardsson}, B. and {Enke}, H. and {Fabre}, C. and {Fabrizio}, M. and {Faigler}, S. and {Fedorets}, G. and {Fernique}, P. and {Fienga}, A. and {Figueras}, F. and {Fournier}, Y. and {Fouron}, C. and {Fragkoudi}, F. and {Gai}, M. and {Garcia-Gutierrez}, A. and {Garcia-Reinaldos}, M. and {Garc{\'\i}a-Torres}, M. and {Garofalo}, A. and {Gavel}, A. and {Gavras}, P. and {Gerlach}, E. and {Geyer}, R. and {Giacobbe}, P. and {Gilmore}, G. and {Girona}, S. and {Giuffrida}, G. and {Gomel}, R. and {Gomez}, A. and {Gonz{\'a}lez-N{\'u}{\~n}ez}, J. and {Gonz{\'a}lez-Santamar{\'\i}a}, I. and {Gonz{\'a}lez-Vidal}, J.~J. and {Granvik}, M. and {Guillout}, P. and {Guiraud}, J. and {Guti{\'e}rrez-S{\'a}nchez}, R. and {Guy}, L.~P. and {Hatzidimitriou}, D. and {Hauser}, M. and {Haywood}, M. and {Helmer}, A. and {Helmi}, A. and {Sarmiento}, M.~H. and {Hidalgo}, S.~L. and {Hilger}, T. and {H{\l}adczuk}, N. and {Hobbs}, D. and {Holland}, G. and {Huckle}, H.~E. and {Jardine}, K. and {Jasniewicz}, G. and {Jean-Antoine Piccolo}, A. and {Jim{\'e}nez-Arranz}, {\'O}. and {Jorissen}, A. and {Juaristi Campillo}, J. and {Julbe}, F. and {Karbevska}, L. and {Kervella}, P. and {Khanna}, S. and {Kontizas}, M. and {Kordopatis}, G. and {Korn}, A.~J. and {K{\'o}sp{\'a}l}, {\'A}. and {Kostrzewa-Rutkowska}, Z. and {Kruszy{\'n}ska}, K. and {Kun}, M. and {Laizeau}, P. and {Lambert}, S. and {Lanza}, A.~F. and {Lasne}, Y. and {Le Campion}, J. -F. and {Lebreton}, Y. and {Lebzelter}, T. and {Leccia}, S. and {Leclerc}, N. and {Lecoeur-Taibi}, I. and {Liao}, S. and {Licata}, E.~L. and {Lindstr{\o}m}, H.~E.~P. and {Lister}, T.~A. and {Livanou}, E. and {Lobel}, A. and {Lorca}, A. and {Loup}, C. and {Madrero Pardo}, P. and {Magdaleno Romeo}, A. and {Managau}, S. and {Mann}, R.~G. and {Manteiga}, M. and {Marchant}, J.~M. and {Marconi}, M. and {Marcos}, J. and {Marcos Santos}, M.~M.~S. and {Mar{\'\i}n Pina}, D. and {Marinoni}, S. and {Marocco}, F. and {Marshall}, D.~J. and {Martin Polo}, L. and {Mart{\'\i}n-Fleitas}, J.~M. and {Marton}, G. and {Mary}, N. and {Masip}, A. and {Massari}, D. and {Mastrobuono-Battisti}, A. and {Mazeh}, T. and {McMillan}, P.~J. and {Messina}, S. and {Michalik}, D. and {Millar}, N.~R. and {Mints}, A. and {Molina}, D. and {Molinaro}, R. and {Moln{\'a}r}, L. and {Monari}, G. and {Mongui{\'o}}, M. and {Montegriffo}, P. and {Montero}, A. and {Mor}, R. and {Mora}, A. and {Morbidelli}, R. and {Morel}, T. and {Morris}, D. and {Muraveva}, T. and {Murphy}, C.~P. and {Musella}, I. and {Nagy}, Z. and {Noval}, L. and {Oca{\~n}a}, F. and {Ogden}, A. and {Ordenovic}, C. and {Osinde}, J.~O. and {Pagani}, C. and {Pagano}, I. and {Palaversa}, L. and {Palicio}, P.~A. and {Pallas-Quintela}, L. and {Panahi}, A. and {Payne-Wardenaar}, S. and {Pe{\~n}alosa Esteller}, X. and {Penttil{\"a}}, A. and {Pichon}, B. and {Piersimoni}, A.~M. and {Pineau}, F. -X. and {Plachy}, E. and {Plum}, G. and {Poggio}, E. and {Pr{\v{s}}a}, A. and {Pulone}, L. and {Racero}, E. and {Ragaini}, S. and {Rainer}, M. and {Raiteri}, C.~M. and {Rambaux}, N. and {Ramos}, P. and {Ramos-Lerate}, M. and {Re Fiorentin}, P. and {Regibo}, S. and {Richards}, P.~J. and {Rios Diaz}, C. and {Ripepi}, V. and {Riva}, A. and {Rix}, H. -W. and {Rixon}, G. and {Robichon}, N. and {Robin}, A.~C. and {Robin}, C. and {Roelens}, M. and {Rogues}, H.~R.~O. and {Rohrbasser}, L. and {Romero-G{\'o}mez}, M. and {Rowell}, N. and {Royer}, F. and {Ruz Mieres}, D. and {Rybicki}, K.~A. and {Sadowski}, G. and {S{\'a}ez N{\'u}{\~n}ez}, A. and {Sagrist{\`a} Sell{\'e}s}, A. and {Sahlmann}, J. and {Salguero}, E. and {Samaras}, N. and {Sanchez Gimenez}, V. and {Sanna}, N. and {Santove{\~n}a}, R. and {Sarasso}, M. and {Schultheis}, M. and {Sciacca}, E. and {Segol}, M. and {Segovia}, J.~C. and {S{\'e}gransan}, D. and {Semeux}, D. and {Shahaf}, S. and {Siddiqui}, H.~I. and {Siebert}, A. and {Siltala}, L. and {Silvelo}, A. and {Slezak}, E. and {Slezak}, I. and {Smart}, R.~L. and {Snaith}, O.~N. and {Solano}, E. and {Solitro}, F. and {Souami}, D. and {Souchay}, J. and {Spagna}, A. and {Spina}, L. and {Spoto}, F. and {Steele}, I.~A. and {Steidelm{\"u}ller}, H. and {Stephenson}, C.~A. and {S{\"u}veges}, M. and {Surdej}, J. and {Szabados}, L. and {Szegedi-Elek}, E. and {Taris}, F. and {Taylor}, M.~B. and {Teixeira}, R. and {Tolomei}, L. and {Tonello}, N. and {Torra}, F. and {Torra}, J. and {Torralba Elipe}, G. and {Trabucchi}, M. and {Tsounis}, A.~T. and {Turon}, C. and {Ulla}, A. and {Unger}, N. and {Vaillant}, M.~V. and {van Dillen}, E. and {van Reeven}, W. and {Vanel}, O. and {Vecchiato}, A. and {Viala}, Y. and {Vicente}, D. and {Voutsinas}, S. and {Weiler}, M. and {Wevers}, T. and {Wyrzykowski}, {\L}. and {Yoldas}, A. and {Yvard}, P. and {Zhao}, H. and {Zorec}, J. and {Zucker}, S. and {Zwitter}, T.},
        title = "{Gaia Data Release 3. Summary of the content and survey properties}",
      journal = {\aap},
     keywords = {techniques: photometric, techniques: spectroscopic, techniques: radial velocities, catalogs, astrometry, parallaxes, Astrophysics - Astrophysics of Galaxies},
         year = 2023,
        month = jun,
       volume = {674},
          eid = {A1},
        pages = {A1},
          doi = {10.1051/0004-6361/202243940},
archivePrefix = {arXiv},
       eprint = {2208.00211},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2023A&A...674A...1G},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_DESCRIPTION = """\
Spectral (BP/RP), photometric, and astrometric dataset based on Gaia DR3.
"""

_HOMEPAGE = "https://www.cosmos.esa.int/web/gaia/dr3"

_LICENSE = ""

_VERSION = "1.0.0"

_SPECTRUM_FEATURES = [
    "coeff",
    "coeff_error",
]

_PHOTOMETRY_FEATURES = [
    "phot_g_mean_mag",
    "phot_g_mean_flux",
    "phot_g_mean_flux_error",
    "phot_bp_mean_mag",
    "phot_bp_mean_flux",
    "phot_bp_mean_flux_error",
    "phot_rp_mean_mag",
    "phot_rp_mean_flux",
    "phot_rp_mean_flux_error",
    "phot_bp_rp_excess_factor",
    "bp_rp",
    "bp_g",
    "g_rp",
]

_ASTROMETRY_FEATURES = [
    "ra",
    "ra_error",
    "dec",
    "dec_error",
    "parallax",
    "parallax_error",
    "pmra",
    "pmra_error",
    "pmdec",
    "pmdec_error",
    "ra_dec_corr",
    "ra_parallax_corr",
    "ra_pmra_corr",
    "ra_pmdec_corr",
    "dec_parallax_corr",
    "dec_pmra_corr",
    "dec_pmdec_corr",
    "parallax_pmra_corr",
    "parallax_pmdec_corr",
    "pmra_pmdec_corr",
]

_RV_FEATURES = [
    "radial_velocity",
    "radial_velocity_error",
    "rv_template_fe_h",
    "rv_template_logg",
    "rv_template_teff",
]

_GSPPHOT_FEATURES = [
    "ag_gspphot",
    "ag_gspphot_lower",
    "ag_gspphot_upper",
    "azero_gspphot",
    "azero_gspphot_lower",
    "azero_gspphot_upper",
    "distance_gspphot",
    "distance_gspphot_lower",
    "distance_gspphot_upper",
    "ebpminrp_gspphot",
    "ebpminrp_gspphot_lower",
    "ebpminrp_gspphot_upper",
    "logg_gspphot",
    "logg_gspphot_lower",
    "logg_gspphot_upper",
    "mh_gspphot",
    "mh_gspphot_lower",
    "mh_gspphot_upper",
    "teff_gspphot",
    "teff_gspphot_lower",
    "teff_gspphot_upper",
]

_FLAG_FEATURES = ["ruwe"]

_CORRECTION_FEATURES = [
    "ecl_lat",
    "ecl_lon",
    "nu_eff_used_in_astrometry",
    "pseudocolour",
    "astrometric_params_solved",
    "rv_template_teff",
    "grvs_mag",
]


class Gaia(datasets.GeneratorBasedBuilder):
    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="gaia_dr3",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["gaia/healpix=*/*.hdf5"]}
            ),
            description="Gaia source table and XP coefficients.",
        ),
    ]

    DEFAULT_CONFIG_NAME = "gaia_dr3"

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to image datasets

        features = {
            "spectral_coefficients": Sequence(
                {f: Value(dtype="float32") for f in _SPECTRUM_FEATURES}
            ),
            "photometry": {f: Value(dtype="float32") for f in _PHOTOMETRY_FEATURES},
            "astrometry": {f: Value(dtype="float32") for f in _ASTROMETRY_FEATURES},
            "radial_velocity": {f: Value(dtype="float32") for f in _RV_FEATURES},
            "gspphot": {f: Value(dtype="float32") for f in _GSPPHOT_FEATURES},
            "flags": {f: Value(dtype="float32") for f in _FLAG_FEATURES},
            "corrections": {f: Value(dtype="float32") for f in _CORRECTION_FEATURES},
            "object_id": Value(dtype="int64"),
            "healpix": Value(dtype="int64"),
            "ra": Value(dtype="float32"),
            "dec": Value(dtype="float32"),
        }

        return datasets.DatasetInfo(
            # This is the description that will appear on the datasets page.
            description=_DESCRIPTION,
            # This defines the different columns of the dataset and their types
            features=Features(features),
            # Homepage of the dataset for documentation
            homepage=_HOMEPAGE,
            # License for the dataset if available
            license=_LICENSE,
            # Citation for the dataset
            citation=_CITATION,
        )

    def _generate_examples(self, files, object_ids=None):
        """Yields examples as (key, example) tuples."""
        for j, file in enumerate(files):
            with h5py.File(file, "r") as data:
                if object_ids is not None:
                    keys = object_ids[j]
                else:
                    keys = data["source_id"][:]

                # Preparing an index for fast searching through the catalog
                sort_index = np.argsort(data["source_id"][:])
                sorted_ids = data["source_id"][:][sort_index]

                for k in keys:
                    # Extract the indices of requested ids in the catalog
                    i = sort_index[np.searchsorted(sorted_ids, k)]

                    s_id = data["source_id"][i]

                    example = {
                        "spectral_coefficients": {
                            f: data[f][i] for f in _SPECTRUM_FEATURES
                        },
                        "photometry": {f: data[f][i] for f in _PHOTOMETRY_FEATURES},
                        "astrometry": {f: data[f][i] for f in _ASTROMETRY_FEATURES},
                        "radial_velocity": {f: data[f][i] for f in _RV_FEATURES},
                        "gspphot": {f: data[f][i] for f in _GSPPHOT_FEATURES},
                        "flags": {f: data[f][i] for f in _FLAG_FEATURES},
                        "corrections": {f: data[f][i] for f in _CORRECTION_FEATURES},
                        "object_id": s_id,
                        "healpix": data["healpix"][i],
                        "ra": data["ra"][i],
                        "dec": data["dec"][i],
                    }

                    yield int(s_id), example

    def _split_generators(self, dl_manager):
        """We handle string, list and dicts in datafiles"""
        if not self.config.data_files:
            raise ValueError(
                f"At least one data file must be specified, but got data_files={self.config.data_files}"
            )
        splits = []
        for split_name, files in self.config.data_files.items():
            if isinstance(files, str):
                files = [files]
            splits.append(
                datasets.SplitGenerator(name=split_name, gen_kwargs={"files": files})
            )
        return splits


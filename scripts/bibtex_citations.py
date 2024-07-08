all_dataset_names = ["apogee","chandra","des_y3_sne_ia","desi","desi_provabgs","foundation","gaia","gz10","hsc","jwst","legacysurvey","plasticc","ps1_sne_ia","sdss","snls","ssl_legacysurvey","swift_sne_ia","tess","vipers","yse"] 

citation_instructions = {
    "apogee": 
        """
        From https://www.sdss4.org/collaboration/citing-sdss/: 

        In addition, the appropriate SDSS acknowledgment(s) for the survey and data releases that were used should be included in the Acknowledgments section: 

        Funding for the Sloan Digital Sky 
        Survey IV has been provided by the 
        Alfred P. Sloan Foundation, the U.S. 
        Department of Energy Office of 
        Science, and the Participating 
        Institutions. 

        SDSS-IV acknowledges support and 
        resources from the Center for High 
        Performance Computing  at the 
        University of Utah. The SDSS 
        website is www.sdss4.org.

        SDSS-IV is managed by the 
        Astrophysical Research Consortium 
        for the Participating Institutions 
        of the SDSS Collaboration including 
        the Brazilian Participation Group, 
        the Carnegie Institution for Science, 
        Carnegie Mellon University, Center for 
        Astrophysics | Harvard \& 
        Smithsonian, the Chilean Participation 
        Group, the French Participation Group, 
        Instituto de Astrof\'isica de 
        Canarias, The Johns Hopkins 
        University, Kavli Institute for the 
        Physics and Mathematics of the 
        Universe (IPMU) / University of 
        Tokyo, the Korean Participation Group, 
        Lawrence Berkeley National Laboratory, 
        Leibniz Institut f\"ur Astrophysik 
        Potsdam (AIP),  Max-Planck-Institut 
        f\"ur Astronomie (MPIA Heidelberg), 
        Max-Planck-Institut f\"ur 
        Astrophysik (MPA Garching), 
        Max-Planck-Institut f\"ur 
        Extraterrestrische Physik (MPE), 
        National Astronomical Observatories of 
        China, New Mexico State University, 
        New York University, University of 
        Notre Dame, Observat\'ario 
        Nacional / MCTI, The Ohio State 
        University, Pennsylvania State 
        University, Shanghai 
        Astronomical Observatory, United 
        Kingdom Participation Group, 
        Universidad Nacional Aut\'onoma 
        de M\'exico, University of Arizona, 
        University of Colorado Boulder, 
        University of Oxford, University of 
        Portsmouth, University of Utah, 
        University of Virginia, University 
        of Washington, University of 
        Wisconsin, Vanderbilt University, 
        and Yale University.
        """,
    "chandra": 
        """
        From https://cxc.cfa.harvard.edu/csc/cite.html :

        Users are kindly requested to acknowledge their use of the Chandra Source Catalog in any resulting publications.

        This will help us greatly to keep track of catalog usage, information that is essential for providing full accountability of our work and services, as well as for planning future services.

        The following language is suggested:

        This research has made use of data obtained from the Chandra Source Catalog, provided by the Chandra X-ray Center (CXC) as part of the Chandra Data Archive.

        """,
    "des_y3_sne_ia": 
        """
        From https://www.darkenergysurvey.org/the-des-project/data-access/ : 

        We request that all papers that use DES public data include the acknowledgement below. In addition, we would appreciate if authors of all such papers would cite the following papers where appropriate:

        DR1
        The Dark Energy Survey Data Release 1, DES Collaboration (2018)
        The Dark Energy Survey Image Processing Pipeline, E. Morganson, et al. (2018)
        The Dark Energy Camera, B. Flaugher, et al, AJ, 150, 150 (2015)

        This project used public archival data from the Dark Energy Survey (DES). Funding for the DES Projects has been provided by the U.S. Department of Energy, the U.S. National Science Foundation, the Ministry of Science and Education of Spain, the Science and Technology FacilitiesCouncil of the United Kingdom, the Higher Education Funding Council for England, the National Center for Supercomputing Applications at the University of Illinois at Urbana-Champaign, the Kavli Institute of Cosmological Physics at the University of Chicago, the Center for Cosmology and Astro-Particle Physics at the Ohio State University, the Mitchell Institute for Fundamental Physics and Astronomy at Texas A\&M University, Financiadora de Estudos e Projetos, Funda{\c c}{\~a}o Carlos Chagas Filho de Amparo {\`a} Pesquisa do Estado do Rio de Janeiro, Conselho Nacional de Desenvolvimento Cient{\'i}fico e Tecnol{\'o}gico and the Minist{\'e}rio da Ci{\^e}ncia, Tecnologia e Inova{\c c}{\~a}o, the Deutsche Forschungsgemeinschaft, and the Collaborating Institutions in the Dark Energy Survey.

        The Collaborating Institutions are Argonne National Laboratory, the University of California at Santa Cruz, the University of Cambridge, Centro de Investigaciones Energ{\'e}ticas, Medioambientales y Tecnol{\'o}gicas-Madrid, the University of Chicago, University College London, the DES-Brazil Consortium, the University of Edinburgh, the Eidgen{\"o}ssische Technische Hochschule (ETH) Z{\"u}rich,  Fermi National Accelerator Laboratory, the University of Illinois at Urbana-Champaign, the Institut de Ci{\`e}ncies de l'Espai (IEEC/CSIC), the Institut de F{\'i}sica d'Altes Energies, Lawrence Berkeley National Laboratory, the Ludwig-Maximilians Universit{\"a}t M{\"u}nchen and the associated Excellence Cluster Universe, the University of Michigan, the National Optical Astronomy Observatory, the University of Nottingham, The Ohio State University, the OzDES Membership Consortium, the University of Pennsylvania, the University of Portsmouth, SLAC National Accelerator Laboratory, Stanford University, the University of Sussex, and Texas A\&M University.

        Based in part on observations at Cerro Tololo Inter-American Observatory, National Optical Astronomy Observatory, which is operated by the Association of Universities for Research in Astronomy (AURA) under a cooperative agreement with the National Science Foundation.
        """,
    "desi": 
        """
        From https://data.desi.lbl.gov/doc/acknowledgments/ : 

        The Dark Energy Spectroscopic Instrument (DESI) data are licensed under the Creative Commons Attribution 4.0 International License (“CC BY 4.0”, Summary, Full Legal Code). Users are free to share, copy, redistribute, adapt, transform and build upon the DESI data available through this website for any purpose, including commercially.

        When DESI data are used, the appropriate credit is required by using both the following reference and acknowledgments text.

        If you are using DESI data, you must cite the following reference and clearly state any changes made to these data:

        DESI Collaboration et al. (2023b), “The Early Data Release of the Dark Energy Spectroscopic Instrument”

        Also consider citing publications from the Technical Papers section if they cover any material used in your work.

        As part of the license attributes, you are obliged to include the following acknowledgments:

        This research used data obtained with the Dark Energy Spectroscopic Instrument (DESI). DESI construction and operations is managed by the Lawrence Berkeley National Laboratory. This material is based upon work supported by the U.S. Department of Energy, Office of Science, Office of High-Energy Physics, under Contract No. DE–AC02–05CH11231, and by the National Energy Research Scientific Computing Center, a DOE Office of Science User Facility under the same contract. Additional support for DESI was provided by the U.S. National Science Foundation (NSF), Division of Astronomical Sciences under Contract No. AST-0950945 to the NSF’s National Optical-Infrared Astronomy Research Laboratory; the Science and Technology Facilities Council of the United Kingdom; the Gordon and Betty Moore Foundation; the Heising-Simons Foundation; the French Alternative Energies and Atomic Energy Commission (CEA); the National Council of Science and Technology of Mexico (CONACYT); the Ministry of Science and Innovation of Spain (MICINN), and by the DESI Member Institutions: www.desi.lbl.gov/collaborating-institutions. The DESI collaboration is honored to be permitted to conduct scientific research on Iolkam Du’ag (Kitt Peak), a mountain with particular significance to the Tohono O’odham Nation. Any opinions, findings, and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the U.S. National Science Foundation, the U.S. Department of Energy, or any of the listed funding agencies.
        """,
    "desi_provabgs": 
        """
        From https://github.com/changhoonhahn/provabgs and https://arxiv.org/abs/2202.01809 :

        This research is supported by the Director, Office of Science, Office of High Energy Physics of the U.S. Department of Energy under Contract No. DE–AC02–05CH11231, and by the National Energy Research Scientific Computing Center, a DOE Office of Science User Facility under the same contract; additional support for DESI is provided by the U.S. National Science Foundation, Division of Astronomical Sciences under Contract No. AST-0950945 to the NSF’s National Optical-Infrared Astronomy Research Laboratory; the Science and Technologies Facilities Council of the United Kingdom; the Gordon and Betty Moore Foundation; the Heising-Simons Foundation; the French Alternative Energies and Atomic Energy Commission (CEA); the National Council of Science and Technology of Mexico; the Ministry of Economy of Spain, and by the DESI Member Institutions.

        The authors are honored to be permitted to conduct scientific research on Iolkam Du’ag (Kitt Peak), a mountain with particular significance to the Tohono O’odham Nation.

        """,
    "foundation": 
        """
        When using these data, please cite:

        Foley et al. (2018) - https://ui.adsabs.harvard.edu/abs/2018MNRAS.475..193F
        Jones et al. (2019) - https://ui.adsabs.harvard.edu/abs/2019ApJ...881...19J

        Please contact David Jones with any questions. You may also raise an issue on github, github.com/djones1040/Foundation_DR1.

        Pan-STARRS is supported in part by the National Aeronautics and Space Administration under Grants
        NNX12AT65G and NNX14AM74G. The Pan-STARRS1
        Surveys (PS1) and the PS1 public science archive have
        been made possible through contributions by the Institute
        for Astronomy, the University of Hawaii, the Pan-STARRS
        Project Office, the Max-Planck Society and its participating institutes, the Max Planck Institute for Astronomy, Heidelberg and the Max Planck Institute for Extraterrestrial
        Physics, Garching, The Johns Hopkins University, Durham
        University, the University of Edinburgh, the Queen’s University Belfast, the Harvard-Smithsonian Center for Astrophysics, the Las Cumbres Observatory Global Telescope
        Network Incorporated, the National Central University of
        Taiwan, the Space Telescope Science Institute, the National
        Aeronautics and Space Administration under Grant No.
        NNX08AR22G issued through the Planetary Science Division of the NASA Science Mission Directorate, the National
        Science Foundation Grant No. AST–1238877, the University of Maryland, Eotvos Lorand University (ELTE), the
        Los Alamos National Laboratory, and the Gordon and Betty Moore Foundation.
        """,
    "gaia": 
        r"""
        If you have used Gaia DR3 data in your research, please use the following acknowledgement:

        This work has made use of data from the European Space Agency (ESA) mission
        {\it Gaia} (\url{https://www.cosmos.esa.int/gaia}), processed by the {\it Gaia}
        Data Processing and Analysis Consortium (DPAC,
        \url{https://www.cosmos.esa.int/web/gaia/dpac/consortium}). Funding for the DPAC
        has been provided by national institutions, in particular the institutions
        participating in the {\it Gaia} Multilateral Agreement.

        """,
    "gz10": 
        """

        The GZ10 catalog from Leung et al. (2018) is a dataset of 17,736 galaxies with labels from the Galaxy Zoo 2 project. The catalog includes the following features for each galaxy: right ascension, declination, redshift, and a label. Galaxy10 DECaLS images come from DESI Legacy Imaging Surveys and labels come from Galaxy Zoo.

        Galaxy10 dataset classification labels come from Galaxy Zoo
        Galaxy10 dataset images come from DESI Legacy Imaging Surveys

        Galaxy Zoo is described in Lintott et al. 2008, the GalaxyZoo Data Release 2 is described in Lintott et al. 2011, Galaxy Zoo DECaLS Campaign is described in Walmsley M. et al. 2021, DESI Legacy Imaging Surveys is described in Dey A. et al., 2019

        The Legacy Surveys consist of three individual and complementary projects: the Dark Energy Camera Legacy Survey (DECaLS; Proposal ID #2014B-0404; PIs: David Schlegel and Arjun Dey), the Beijing-Arizona Sky Survey (BASS; NOAO Prop. ID #2015A-0801; PIs: Zhou Xu and Xiaohui Fan), and the Mayall z-band Legacy Survey (MzLS; Prop. ID #2016A-0453; PI: Arjun Dey). DECaLS, BASS and MzLS together include data obtained, respectively, at the Blanco telescope, Cerro Tololo Inter-American Observatory, NSF’s NOIRLab; the Bok telescope, Steward Observatory, University of Arizona; and the Mayall telescope, Kitt Peak National Observatory, NOIRLab. The Legacy Surveys project is honored to be permitted to conduct astronomical research on Iolkam Du’ag (Kitt Peak), a mountain with particular significance to the Tohono O’odham Nation.

        """,
    "hsc": 
        """
        """,
    "jwst": 
        """
        """,
    "legacysurvey": 
        """
        """,
    "plasticc": 
        """
        """,
    "ps1_sne_ia": 
        """
        """,
    "sdss": 
        """
        """,
    "snls": 
        """
        """,
    "ssl_legacysurvey": 
        """
        """,
    "swift_sne_ia": 
        """
        """,
    "tess": 
        """
        """,
    "vipers": 
        """
        """,
    "yse": 
        """
        """,
}

bibtex_entries = { 
    "apogee": 
        """ 
        @ARTICLE{2017AJ....154...28B,
           author = {{Blanton}, Michael R. and {Bershady}, Matthew A. and {Abolfathi}, Bela and {Albareti}, Franco D. and {Allende Prieto}, Carlos and {Almeida}, Andres and {Alonso-Garc{\'\i}a}, Javier and {Anders}, Friedrich and {Anderson}, Scott F. and {Andrews}, Brett and {Aquino-Ort{\'\i}z}, Erik and {Arag{\'o}n-Salamanca}, Alfonso and {Argudo-Fern{\'a}ndez}, Maria and {Armengaud}, Eric and {Aubourg}, Eric and {Avila-Reese}, Vladimir and {Badenes}, Carles and {Bailey}, Stephen and {Barger}, Kathleen A. and {Barrera-Ballesteros}, Jorge and {Bartosz}, Curtis and {Bates}, Dominic and {Baumgarten}, Falk and {Bautista}, Julian and {Beaton}, Rachael and {Beers}, Timothy C. and {Belfiore}, Francesco and {Bender}, Chad F. and {Berlind}, Andreas A. and {Bernardi}, Mariangela and {Beutler}, Florian and {Bird}, Jonathan C. and {Bizyaev}, Dmitry and {Blanc}, Guillermo A. and {Blomqvist}, Michael and {Bolton}, Adam S. and {Boquien}, M{\'e}d{\'e}ric and {Borissova}, Jura and {van den Bosch}, Remco and {Bovy}, Jo and {Brandt}, William N. and {Brinkmann}, Jonathan and {Brownstein}, Joel R. and {Bundy}, Kevin and {Burgasser}, Adam J. and {Burtin}, Etienne and {Busca}, Nicol{\'a}s G. and {Cappellari}, Michele and {Delgado Carigi}, Maria Leticia and {Carlberg}, Joleen K. and {Carnero Rosell}, Aurelio and {Carrera}, Ricardo and {Chanover}, Nancy J. and {Cherinka}, Brian and {Cheung}, Edmond and {G{\'o}mez Maqueo Chew}, Yilen and {Chiappini}, Cristina and {Choi}, Peter Doohyun and {Chojnowski}, Drew and {Chuang}, Chia-Hsun and {Chung}, Haeun and {Cirolini}, Rafael Fernando and {Clerc}, Nicolas and {Cohen}, Roger E. and {Comparat}, Johan and {da Costa}, Luiz and {Cousinou}, Marie-Claude and {Covey}, Kevin and {Crane}, Jeffrey D. and {Croft}, Rupert A.~C. and {Cruz-Gonzalez}, Irene and {Garrido Cuadra}, Daniel and {Cunha}, Katia and {Damke}, Guillermo J. and {Darling}, Jeremy and {Davies}, Roger and {Dawson}, Kyle and {de la Macorra}, Axel and {Dell'Agli}, Flavia and {De Lee}, Nathan and {Delubac}, Timoth{\'e}e and {Di Mille}, Francesco and {Diamond-Stanic}, Aleks and {Cano-D{\'\i}az}, Mariana and {Donor}, John and {Downes}, Juan Jos{\'e} and {Drory}, Niv and {du Mas des Bourboux}, H{\'e}lion and {Duckworth}, Christopher J. and {Dwelly}, Tom and {Dyer}, Jamie and {Ebelke}, Garrett and {Eigenbrot}, Arthur D. and {Eisenstein}, Daniel J. and {Emsellem}, Eric and {Eracleous}, Mike and {Escoffier}, Stephanie and {Evans}, Michael L. and {Fan}, Xiaohui and {Fern{\'a}ndez-Alvar}, Emma and {Fernandez-Trincado}, J.~G. and {Feuillet}, Diane K. and {Finoguenov}, Alexis and {Fleming}, Scott W. and {Font-Ribera}, Andreu and {Fredrickson}, Alexander and {Freischlad}, Gordon and {Frinchaboy}, Peter M. and {Fuentes}, Carla E. and {Galbany}, Llu{\'\i}s and {Garcia-Dias}, R. and {Garc{\'\i}a-Hern{\'a}ndez}, D.~A. and {Gaulme}, Patrick and {Geisler}, Doug and {Gelfand}, Joseph D. and {Gil-Mar{\'\i}n}, H{\'e}ctor and {Gillespie}, Bruce A. and {Goddard}, Daniel and {Gonzalez-Perez}, Violeta and {Grabowski}, Kathleen and {Green}, Paul J. and {Grier}, Catherine J. and {Gunn}, James E. and {Guo}, Hong and {Guy}, Julien and {Hagen}, Alex and {Hahn}, ChangHoon and {Hall}, Matthew and {Harding}, Paul and {Hasselquist}, Sten and {Hawley}, Suzanne L. and {Hearty}, Fred and {Gonzalez Hern{\'a}ndez}, Jonay I. and {Ho}, Shirley and {Hogg}, David W. and {Holley-Bockelmann}, Kelly and {Holtzman}, Jon A. and {Holzer}, Parker H. and {Huehnerhoff}, Joseph and {Hutchinson}, Timothy A. and {Hwang}, Ho Seong and {Ibarra-Medel}, H{\'e}ctor J. and {da Silva Ilha}, Gabriele and {Ivans}, Inese I. and {Ivory}, KeShawn and {Jackson}, Kelly and {Jensen}, Trey W. and {Johnson}, Jennifer A. and {Jones}, Amy and {J{\"o}nsson}, Henrik and {Jullo}, Eric and {Kamble}, Vikrant and {Kinemuchi}, Karen and {Kirkby}, David and {Kitaura}, Francisco-Shu and {Klaene}, Mark and {Knapp}, Gillian R. and {Kneib}, Jean-Paul and {Kollmeier}, Juna A. and {Lacerna}, Ivan and {Lane}, Richard R. and {Lang}, Dustin and {Law}, David R. and {Lazarz}, Daniel and {Lee}, Youngbae and {Le Goff}, Jean-Marc and {Liang}, Fu-Heng and {Li}, Cheng and {Li}, Hongyu and {Lian}, Jianhui and {Lima}, Marcos and {Lin}, Lihwai and {Lin}, Yen-Ting and {Bertran de Lis}, Sara and {Liu}, Chao and {de Icaza Lizaola}, Miguel Angel C. and {Long}, Dan and {Lucatello}, Sara and {Lundgren}, Britt and {MacDonald}, Nicholas K. and {Deconto Machado}, Alice and {MacLeod}, Chelsea L. and {Mahadevan}, Suvrath and {Geimba Maia}, Marcio Antonio and {Maiolino}, Roberto and {Majewski}, Steven R. and {Malanushenko}, Elena and {Malanushenko}, Viktor and {Manchado}, Arturo and {Mao}, Shude and {Maraston}, Claudia and {Marques-Chaves}, Rui and {Masseron}, Thomas and {Masters}, Karen L. and {McBride}, Cameron K. and {McDermid}, Richard M. and {McGrath}, Brianne and {McGreer}, Ian D. and {Medina Pe{\~n}a}, Nicol{\'a}s and {Melendez}, Matthew and {Merloni}, Andrea and {Merrifield}, Michael R. and {Meszaros}, Szabolcs and {Meza}, Andres and {Minchev}, Ivan and {Minniti}, Dante and {Miyaji}, Takamitsu and {More}, Surhud and {Mulchaey}, John and {M{\"u}ller-S{\'a}nchez}, Francisco and {Muna}, Demitri and {Munoz}, Ricardo R. and {Myers}, Adam D. and {Nair}, Preethi and {Nandra}, Kirpal and {Correa do Nascimento}, Janaina and {Negrete}, Alenka and {Ness}, Melissa and {Newman}, Jeffrey A. and {Nichol}, Robert C. and {Nidever}, David L. and {Nitschelm}, Christian and {Ntelis}, Pierros and {O'Connell}, Julia E. and {Oelkers}, Ryan J. and {Oravetz}, Audrey and {Oravetz}, Daniel and {Pace}, Zach and {Padilla}, Nelson and {Palanque-Delabrouille}, Nathalie and {Alonso Palicio}, Pedro and {Pan}, Kaike and {Parejko}, John K. and {Parikh}, Taniya and {P{\^a}ris}, Isabelle and {Park}, Changbom and {Patten}, Alim Y. and {Peirani}, Sebastien and {Pellejero-Ibanez}, Marcos and {Penny}, Samantha and {Percival}, Will J. and {Perez-Fournon}, Ismael and {Petitjean}, Patrick and {Pieri}, Matthew M. and {Pinsonneault}, Marc and {Pisani}, Alice and {Poleski}, Rados{\l}aw and {Prada}, Francisco and {Prakash}, Abhishek and {Queiroz}, Anna B{\'a}rbara de Andrade and {Raddick}, M. Jordan and {Raichoor}, Anand and {Barboza Rembold}, Sandro and {Richstein}, Hannah and {Riffel}, Rogemar A. and {Riffel}, Rog{\'e}rio and {Rix}, Hans-Walter and {Robin}, Annie C. and {Rockosi}, Constance M. and {Rodr{\'\i}guez-Torres}, Sergio and {Roman-Lopes}, A. and {Rom{\'a}n-Z{\'u}{\~n}iga}, Carlos and {Rosado}, Margarita and {Ross}, Ashley J. and {Rossi}, Graziano and {Ruan}, John and {Ruggeri}, Rossana and {Rykoff}, Eli S. and {Salazar-Albornoz}, Salvador and {Salvato}, Mara and {S{\'a}nchez}, Ariel G. and {Aguado}, D.~S. and {S{\'a}nchez-Gallego}, Jos{\'e} R. and {Santana}, Felipe A. and {Santiago}, Bas{\'\i}lio Xavier and {Sayres}, Conor and {Schiavon}, Ricardo P. and {da Silva Schimoia}, Jaderson and {Schlafly}, Edward F. and {Schlegel}, David J. and {Schneider}, Donald P. and {Schultheis}, Mathias and {Schuster}, William J. and {Schwope}, Axel and {Seo}, Hee-Jong and {Shao}, Zhengyi and {Shen}, Shiyin and {Shetrone}, Matthew and {Shull}, Michael and {Simon}, Joshua D. and {Skinner}, Danielle and {Skrutskie}, M.~F. and {Slosar}, An{\v{z}}e and {Smith}, Verne V. and {Sobeck}, Jennifer S. and {Sobreira}, Flavia and {Somers}, Garrett and {Souto}, Diogo and {Stark}, David V. and {Stassun}, Keivan and {Stauffer}, Fritz and {Steinmetz}, Matthias and {Storchi-Bergmann}, Thaisa and {Streblyanska}, Alina and {Stringfellow}, Guy S. and {Su{\'a}rez}, Genaro and {Sun}, Jing and {Suzuki}, Nao and {Szigeti}, Laszlo and {Taghizadeh-Popp}, Manuchehr and {Tang}, Baitian and {Tao}, Charling and {Tayar}, Jamie and {Tembe}, Mita and {Teske}, Johanna and {Thakar}, Aniruddha R. and {Thomas}, Daniel and {Thompson}, Benjamin A. and {Tinker}, Jeremy L. and {Tissera}, Patricia and {Tojeiro}, Rita and {Hernandez Toledo}, Hector and {de la Torre}, Sylvain and {Tremonti}, Christy and {Troup}, Nicholas W. and {Valenzuela}, Octavio and {Martinez Valpuesta}, Inma and {Vargas-Gonz{\'a}lez}, Jaime and {Vargas-Maga{\~n}a}, Mariana and {Vazquez}, Jose Alberto and {Villanova}, Sandro and {Vivek}, M. and {Vogt}, Nicole and {Wake}, David and {Walterbos}, Rene and {Wang}, Yuting and {Weaver}, Benjamin Alan and {Weijmans}, Anne-Marie and {Weinberg}, David H. and {Westfall}, Kyle B. and {Whelan}, David G. and {Wild}, Vivienne and {Wilson}, John and {Wood-Vasey}, W.~M. and {Wylezalek}, Dominika and {Xiao}, Ting and {Yan}, Renbin and {Yang}, Meng and {Ybarra}, Jason E. and {Y{\`e}che}, Christophe and {Zakamska}, Nadia and {Zamora}, Olga and {Zarrouk}, Pauline and {Zasowski}, Gail and {Zhang}, Kai and {Zhao}, Gong-Bo and {Zheng}, Zheng and {Zheng}, Zheng and {Zhou}, Xu and {Zhou}, Zhi-Min and {Zhu}, Guangtun B. and {Zoccali}, Manuela and {Zou}, Hu},
            title = "{Sloan Digital Sky Survey IV: Mapping the Milky Way, Nearby Galaxies, and the Distant Universe}",
              journal = {\aj},
             keywords = {cosmology: observations, galaxies: general, Galaxy: general, instrumentation: spectrographs, stars: general, surveys, Astrophysics - Astrophysics of Galaxies},
                 year = 2017,
                month = jul,
               volume = {154},
               number = {1},
                  eid = {28},
                pages = {28},
                  doi = {10.3847/1538-3881/aa7567},
            archivePrefix = {arXiv},
                   eprint = {1703.00052},
             primaryClass = {astro-ph.GA},
                   adsurl = {https://ui.adsabs.harvard.edu/abs/2017AJ....154...28B},
                  adsnote = {Provided by the SAO/NASA Astrophysics Data System}
            }

        @ARTICLE{2022ApJS..259...35A, author = {{Abdurro'uf} and et al.}, title = "{The Seventeenth Data Release of the Sloan Digital Sky Surveys: Complete Release of MaNGA, MaStar, and APOGEE-2 Data}", journal = {pjs}, keywords = {Astronomy data acquisition, Astronomy databases, Surveys, 1860, 83, 1671, Astrophysics - Astrophysics of Galaxies, Astrophysics - Instrumentation and Methods for Astrophysics}, year = 2022, month = apr, volume = {259}, number = {2}, eid = {35}, pages = {35}, doi = {10.3847/1538-4365/ac4414}, archivePrefix = {arXiv}, eprint = {2112.02026}, primaryClass = {astro-ph.GA}, adsurl = {https://ui.adsabs.harvard.edu/abs/2022ApJS..259...35A}, adsnote = {Provided by the SAO/NASA Astrophysics Data System} }

        @ARTICLE{2017AJ....154...94M,
           author = {{Majewski}, S.~R. and {Schiavon}, R.~P. and {Frinchaboy}, P.~M. and 
            {Allende Prieto}, C. and {Barkhouser}, R. and {Bizyaev}, D. and 
            {Blank}, B. and {Brunner}, S. and {Burton}, A. and {Carrera}, R. and 
            {Chojnowski}, S.~D. and {Cunha}, K. and {Epstein}, C. and {Fitzgerald}, G. and 
            {Garc{\'{\i}}a P{\'e}rez}, A.~E. and {Hearty}, F.~R. and {Henderson}, C. and 
            {Holtzman}, J.~A. and {Johnson}, J.~A. and {Lam}, C.~R. and 
            {Lawler}, J.~E. and {Maseman}, P. and {M{\'e}sz{\'a}ros}, S. and 
            {Nelson}, M. and {Nguyen}, D.~C. and {Nidever}, D.~L. and {Pinsonneault}, M. and 
            {Shetrone}, M. and {Smee}, S. and {Smith}, V.~V. and {Stolberg}, T. and 
            {Skrutskie}, M.~F. and {Walker}, E. and {Wilson}, J.~C. and 
            {Zasowski}, G. and {Anders}, F. and {Basu}, S. and {Beland}, S. and 
            {Blanton}, M.~R. and {Bovy}, J. and {Brownstein}, J.~R. and 
            {Carlberg}, J. and {Chaplin}, W. and {Chiappini}, C. and {Eisenstein}, D.~J. and 
            {Elsworth}, Y. and {Feuillet}, D. and {Fleming}, S.~W. and {Galbraith-Frew}, J. and 
            {Garc{\'{\i}}a}, R.~A. and {Garc{\'{\i}}a-Hern{\'a}ndez}, D.~A. and 
            {Gillespie}, B.~A. and {Girardi}, L. and {Gunn}, J.~E. and {Hasselquist}, S. and 
            {Hayden}, M.~R. and {Hekker}, S. and {Ivans}, I. and {Kinemuchi}, K. and 
            {Klaene}, M. and {Mahadevan}, S. and {Mathur}, S. and {Mosser}, B. and 
            {Muna}, D. and {Munn}, J.~A. and {Nichol}, R.~C. and {O'Connell}, R.~W. and 
            {Parejko}, J.~K. and {Robin}, A.~C. and {Rocha-Pinto}, H. and 
            {Schultheis}, M. and {Serenelli}, A.~M. and {Shane}, N. and 
            {Silva Aguirre}, V. and {Sobeck}, J.~S. and {Thompson}, B. and 
            {Troup}, N.~W. and {Weinberg}, D.~H. and {Zamora}, O.},
            title = "{The Apache Point Observatory Galactic Evolution Experiment (APOGEE)}",
            journal = {\aj},
            archivePrefix = "arXiv",
            eprint = {1509.05420},
            primaryClass = "astro-ph.IM",
            keywords = {Galaxy: abundances, Galaxy: evolution, Galaxy: formation, Galaxy: kinematics and dynamics, Galaxy: stellar content, Galaxy: structure},
            year = 2017,
            month = sep,
            volume = 154,
              eid = {94},
            pages = {94},
              doi = {10.3847/1538-3881/aa784d},
            adsurl = {http://adsabs.harvard.edu/abs/2017AJ....154...94M},
            adsnote = {Provided by the SAO/NASA Astrophysics Data System}
            }

        @ARTICLE{2019PASP..131e5001W,
               author = {{Wilson}, J.~C. and {Hearty}, F.~R. and {Skrutskie}, M.~F. and {Majewski}, S.~R. and {Holtzman}, J.~A. and {Eisenstein}, D. and {Gunn}, J. and {Blank}, B. and {Henderson}, C. and {Smee}, S. and {Nelson}, M. and {Nidever}, D. and {Arns}, J. and {Barkhouser}, R. and {Barr}, J. and {Beland}, S. and {Bershady}, M.~A. and {Blanton}, M.~R. and {Brunner}, S. and {Burton}, A. and {Carey}, L. and {Carr}, M. and {Colque}, J.~P. and {Crane}, J. and {Damke}, G.~J. and {Davidson}, J.~W., Jr. and {Dean}, J. and {Di Mille}, F. and {Don}, K.~W. and {Ebelke}, G. and {Evans}, M. and {Fitzgerald}, G. and {Gillespie}, B. and {Hall}, M. and {Harding}, A. and {Harding}, P. and {Hammond}, R. and {Hancock}, D. and {Harrison}, C. and {Hope}, S. and {Horne}, T. and {Karakla}, J. and {Lam}, C. and {Leger}, F. and {MacDonald}, N. and {Maseman}, P. and {Matsunari}, J. and {Melton}, S. and {Mitcheltree}, T. and {O'Brien}, T. and {O'Connell}, R.~W. and {Patten}, A. and {Richardson}, W. and {Rieke}, G. and {Rieke}, M. and {Roman-Lopes}, A. and {Schiavon}, R.~P. and {Sobeck}, J.~S. and {Stolberg}, T. and {Stoll}, R. and {Tembe}, M. and {Trujillo}, J.~D. and {Uomoto}, A. and {Vernieri}, M. and {Walker}, E. and {Weinberg}, D.~H. and {Young}, E. and {Anthony-Brumfield}, B. and {Bizyaev}, D. and {Breslauer}, B. and {De Lee}, N. and {Downey}, J. and {Halverson}, S. and {Huehnerhoff}, J. and {Klaene}, M. and {Leon}, E. and {Long}, D. and {Mahadevan}, S. and {Malanushenko}, E. and {Nguyen}, D.~C. and {Owen}, R. and {S{\'a}nchez-Gallego}, J.~R. and {Sayres}, C. and {Shane}, N. and {Shectman}, S.~A. and {Shetrone}, M. and {Skinner}, D. and {Stauffer}, F. and {Zhao}, B.},
                title = "{The Apache Point Observatory Galactic Evolution Experiment (APOGEE) Spectrographs}",
              journal = {\pasp},
             keywords = {Astrophysics - Instrumentation and Methods for Astrophysics},
                 year = 2019,
                month = may,
               volume = {131},
               number = {999},
                pages = {055001},
                  doi = {10.1088/1538-3873/ab0075},
        archivePrefix = {arXiv},
               eprint = {1902.00928},
         primaryClass = {astro-ph.IM},
               adsurl = {https://ui.adsabs.harvard.edu/abs/2019PASP..131e5001W},
              adsnote = {Provided by the SAO/NASA Astrophysics Data System}
        }

        @ARTICLE{2016AJ....151..144G, author = {{Garc{'\i}a P{'e}rez}, Ana E. and {Allende Prieto}, Carlos and {Holtzman}, Jon A. and {Shetrone}, Matthew and {M{'e}sz{'a}ros}, Szabolcs and {Bizyaev}, Dmitry and {Carrera}, Ricardo and {Cunha}, Katia and {Garc{'\i}a-Hern{'a}ndez}, D.~A. and {Johnson}, Jennifer A. and {Majewski}, Steven R. and {Nidever}, David L. and {Schiavon}, Ricardo P. and {Shane}, Neville and {Smith}, Verne V. and {Sobeck}, Jennifer and {Troup}, Nicholas and {Zamora}, Olga and {Weinberg}, David H. and {Bovy}, Jo and {Eisenstein}, Daniel J. and {Feuillet}, Diane and {Frinchaboy}, Peter M. and {Hayden}, Michael R. and {Hearty}, Fred R. and {Nguyen}, Duy C. and {O'Connell}, Robert W. and {Pinsonneault}, Marc H. and {Wilson}, John C. and {Zasowski}, Gail}, title = "{ASPCAP: The APOGEE Stellar Parameter and Chemical Abundances Pipeline}", journal = {j}, keywords = {Galaxy: center, Galaxy: structure, methods: data analysis, stars: abundances, stars: atmospheres, Astrophysics - Solar and Stellar Astrophysics}, year = 2016, month = jun, volume = {151}, number = {6}, eid = {144}, pages = {144}, doi = {10.3847/0004-6256/151/6/144}, archivePrefix = {arXiv}, eprint = {1510.07635}, primaryClass = {astro-ph.SR}, adsurl = {https://ui.adsabs.harvard.edu/abs/2016AJ....151..144G}, adsnote = {Provided by the SAO/NASA Astrophysics Data System} }
        """,
    "chandra": 
        """
        @ARTICLE{2010ApJS..189...37E, author = {{Evans}, Ian N. and {Primini}, Francis A. and {Glotfelty}, Kenny J. and {Anderson}, Craig S. and {Bonaventura}, Nina R. and {Chen}, Judy C. and {Davis}, John E. and {Doe}, Stephen M. and {Evans}, Janet D. and {Fabbiano}, Giuseppina and {Galle}, Elizabeth C. and {Gibbs}, Danny G., II and {Grier}, John D. and {Hain}, Roger M. and {Hall}, Diane M. and {Harbo}, Peter N. and {He}, Xiangqun Helen and {Houck}, John C. and {Karovska}, Margarita and {Kashyap}, Vinay L. and {Lauer}, Jennifer and {McCollough}, Michael L. and {McDowell}, Jonathan C. and {Miller}, Joseph B. and {Mitschang}, Arik W. and {Morgan}, Douglas L. and {Mossman}, Amy E. and {Nichols}, Joy S. and {Nowak}, Michael A. and {Plummer}, David A. and {Refsdal}, Brian L. and {Rots}, Arnold H. and {Siemiginowska}, Aneta and {Sundheim}, Beth A. and {Tibbetts}, Michael S. and {Van Stone}, David W. and {Winkelman}, Sherry L. and {Zografou}, Panagoula}, title = "{The Chandra Source Catalog}", journal = {pjs}, keywords = {catalogs, X-rays: general, Astrophysics - High Energy Astrophysical Phenomena, Astrophysics - Instrumentation and Methods for Astrophysics}, year = 2010, month = jul, volume = {189}, number = {1}, pages = {37-82}, doi = {10.1088/0067-0049/189/1/37}, archivePrefix = {arXiv}, eprint = {1005.4665}, primaryClass = {astro-ph.HE}, adsurl = {https://ui.adsabs.harvard.edu/abs/2010ApJS..189...37E}, adsnote = {Provided by the SAO/NASA Astrophysics Data System} }
        """,
    "des_y3_sne_ia": 
        """
        @ARTICLE{2019ApJ...874..106B, author = {{Brout}, D. and {Sako}, M. and {Scolnic}, D. and {Kessler}, R. and {D'Andrea}, C.B. and {Davis}, T.M. and {Hinton}, S.R. and {Kim}, A.G. and {Lasker}, J. and {Macaulay}, E. and {M{"o}ller}, A. and {Nichol}, R.C. and {Smith}, M. and {Sullivan}, M. and {Wolf}, R.C. and {Allam}, S. and {Bassett}, B.A. and {Brown}, P. and {Castander}, F.J. and {Childress}, M. and {Foley}, R.J. and {Galbany}, L. and {Herner}, K. and {Kasai}, E. and {March}, M. and {Morganson}, E. and {Nugent}, P. and {Pan}, Y. -C. and {Thomas}, R.C. and {Tucker}, B.E. and {Wester}, W. and {Abbott}, T.M.C. and {Annis}, J. and {Avila}, S. and {Bertin}, E. and {Brooks}, D. and {Burke}, D.L. and {Carnero Rosell}, A. and {Carrasco Kind}, M. and {Carretero}, J. and {Crocce}, M. and {Cunha}, C.E. and {da Costa}, L.N. and {Davis}, C. and {De Vicente}, J. and {Desai}, S. and {Diehl}, H.T. and {Doel}, P. and {Eifler}, T.F. and {Flaugher}, B. and {Fosalba}, P. and {Frieman}, J. and {Garc{'\i}a-Bellido}, J. and {Gaztanaga}, E. and {Gerdes}, D.W. and {Goldstein}, D.A. and {Gruen}, D. and {Gruendl}, R.A. and {Gschwend}, J. and {Gutierrez}, G. and {Hartley}, W.G. and {Hollowood}, D.L. and {Honscheid}, K. and {James}, D.J. and {Kuehn}, K. and {Kuropatkin}, N. and {Lahav}, O. and {Li}, T.S. and {Lima}, M. and {Marshall}, J.L. and {Martini}, P. and {Miquel}, R. and {Nord}, B. and {Plazas}, A.A. and {Roodman}, A. and {Rykoff}, E.S. and {Sanchez}, E. and {Scarpine}, V. and {Schindler}, R. and {Schubnell}, M. and {Serrano}, S. and {Sevilla-Noarbe}, I. and {Soares-Santos}, M. and {Sobreira}, F. and {Suchyta}, E. and {Swanson}, M.E.C. and {Tarle}, G. and {Thomas}, D. and {Tucker}, D.L. and {Walker}, A.R. and {Yanny}, B. and {Zhang}, Y. and {DES COLLABORATION}}, title = "{First Cosmology Results Using Type Ia Supernovae from the Dark Energy Survey: Photometric Pipeline and Light-curve Data Release}", journal = {pj}, keywords = {cosmology: observations, supernovae: general, techniques: photometric, Astrophysics - Instrumentation and Methods for Astrophysics}, year = 2019, month = mar, volume = {874}, number = {1}, eid = {106}, pages = {106}, doi = {10.3847/1538-4357/ab06c1}, archivePrefix = {arXiv}, eprint = {1811.02378}, primaryClass = {astro-ph.IM}, adsurl = {https://ui.adsabs.harvard.edu/abs/2019ApJ...874..106B}, adsnote = {Provided by the SAO/NASA Astrophysics Data System} }

        @article{Abbott_2018,
       title={The Dark Energy Survey: Data Release 1},
       volume={239},
       ISSN={1538-4365},
       url={http://dx.doi.org/10.3847/1538-4365/aae9f0},
       DOI={10.3847/1538-4365/aae9f0},
       number={2},
       journal={The Astrophysical Journal Supplement Series},
       publisher={American Astronomical Society},
       author={Abbott, T. M. C. and Abdalla, F. B. and Allam, S. and Amara, A. and Annis, J. and Asorey, J. and Avila, S. and Ballester, O. and Banerji, M. and Barkhouse, W. and Baruah, L. and Baumer, M. and Bechtol, K. and Becker, M. R. and Benoit-Lévy, A. and Bernstein, G. M. and Bertin, E. and Blazek, J. and Bocquet, S. and Brooks, D. and Brout, D. and Buckley-Geer, E. and Burke, D. L. and Busti, V. and Campisano, R. and Cardiel-Sas, L. and Rosell, A. Carnero and Kind, M. Carrasco and Carretero, J. and Castander, F. J. and Cawthon, R. and Chang, C. and Chen, X. and Conselice, C. and Costa, G. and Crocce, M. and Cunha, C. E. and D’Andrea, C. B. and Costa, L. N. da and Das, R. and Daues, G. and Davis, T. M. and Davis, C. and Vicente, J. De and DePoy, D. L. and DeRose, J. and Desai, S. and Diehl, H. T. and Dietrich, J. P. and Dodelson, S. and Doel, P. and Drlica-Wagner, A. and Eifler, T. F. and Elliott, A. E. and Evrard, A. E. and Farahi, A. and Neto, A. Fausti and Fernandez, E. and Finley, D. A. and Flaugher, B. and Foley, R. J. and Fosalba, P. and Friedel, D. N. and Frieman, J. and García-Bellido, J. and Gaztanaga, E. and Gerdes, D. W. and Giannantonio, T. and Gill, M. S. S. and Glazebrook, K. and Goldstein, D. A. and Gower, M. and Gruen, D. and Gruendl, R. A. and Gschwend, J. and Gupta, R. R. and Gutierrez, G. and Hamilton, S. and Hartley, W. G. and Hinton, S. R. and Hislop, J. M. and Hollowood, D. and Honscheid, K. and Hoyle, B. and Huterer, D. and Jain, B. and James, D. J. and Jeltema, T. and Johnson, M. W. G. and Johnson, M. D. and Kacprzak, T. and Kent, S. and Khullar, G. and Klein, M. and Kovacs, A. and Koziol, A. M. G. and Krause, E. and Kremin, A. and Kron, R. and Kuehn, K. and Kuhlmann, S. and Kuropatkin, N. and Lahav, O. and Lasker, J. and Li, T. S. and Li, R. T. and Liddle, A. R. and Lima, M. and Lin, H. and López-Reyes, P. and MacCrann, N. and Maia, M. A. G. and Maloney, J. D. and Manera, M. and March, M. and Marriner, J. and Marshall, J. L. and Martini, P. and McClintock, T. and McKay, T. and McMahon, R. G. and Melchior, P. and Menanteau, F. and Miller, C. J. and Miquel, R. and Mohr, J. J. and Morganson, E. and Mould, J. and Neilsen, E. and Nichol, R. C. and Nogueira, F. and Nord, B. and Nugent, P. and Nunes, L. and Ogando, R. L. C. and Old, L. and Pace, A. B. and Palmese, A. and Paz-Chinchón, F. and Peiris, H. V. and Percival, W. J. and Petravick, D. and Plazas, A. A. and Poh, J. and Pond, C. and Porredon, A. and Pujol, A. and Refregier, A. and Reil, K. and Ricker, P. M. and Rollins, R. P. and Romer, A. K. and Roodman, A. and Rooney, P. and Ross, A. J. and Rykoff, E. S. and Sako, M. and Sanchez, M. L. and Sanchez, E. and Santiago, B. and Saro, A. and Scarpine, V. and Scolnic, D. and Serrano, S. and Sevilla-Noarbe, I. and Sheldon, E. and Shipp, N. and Silveira, M. L. and Smith, M. and Smith, R. C. and Smith, J. A. and Soares-Santos, M. and Sobreira, F. and Song, J. and Stebbins, A. and Suchyta, E. and Sullivan, M. and Swanson, M. E. C. and Tarle, G. and Thaler, J. and Thomas, D. and Thomas, R. C. and Troxel, M. A. and Tucker, D. L. and Vikram, V. and Vivas, A. K. and Walker, A. R. and Wechsler, R. H. and Weller, J. and Wester, W. and Wolf, R. C. and Wu, H. and Yanny, B. and Zenteno, A. and Zhang, Y. and Zuntz, J. and Juneau, S. and Fitzpatrick, M. and Nikutta, R. and Nidever, D. and Olsen, K. and Scott, A.},
       year={2018},
       month=nov, pages={18} }
        """,
    "desi": 
        """
        @ARTICLE{2023arXiv230606308D,
       author = {{DESI Collaboration} and {Adame}, A.~G. and {Aguilar}, J. and {Ahlen}, S. and {Alam}, S. and {Aldering}, G. and {Alexander}, D.~M. and {Alfarsy}, R. and {Allende Prieto}, C. and {Alvarez}, M. and {Alves}, O. and {Anand}, A. and {Andrade-Oliveira}, F. and {Armengaud}, E. and {Asorey}, J. and {Avila}, S. and {Aviles}, A. and {Bailey}, S. and {Balaguera-Antol{\'\i}nez}, A. and {Ballester}, O. and {Baltay}, C. and {Bault}, A. and {Bautista}, J. and {Behera}, J. and {Beltran}, S.~F. and {BenZvi}, S. and {Beraldo e Silva}, L. and {Bermejo-Climent}, J.~R. and {Berti}, A. and {Besuner}, R. and {Beutler}, F. and {Bianchi}, D. and {Blake}, C. and {Blum}, R. and {Bolton}, A.~S. and {Brieden}, S. and {Brodzeller}, A. and {Brooks}, D. and {Brown}, Z. and {Buckley-Geer}, E. and {Burtin}, E. and {Cabayol-Garcia}, L. and {Cai}, Z. and {Canning}, R. and {Cardiel-Sas}, L. and {Carnero Rosell}, A. and {Castander}, F.~J. and {Cervantes-Cota}, J.~L. and {Chabanier}, S. and {Chaussidon}, E. and {Chaves-Montero}, J. and {Chen}, S. and {Chuang}, C. and {Claybaugh}, T. and {Cole}, S. and {Cooper}, A.~P. and {Cuceu}, A. and {Davis}, T.~M. and {Dawson}, K. and {de Belsunce}, R. and {de la Cruz}, R. and {de la Macorra}, A. and {de Mattia}, A. and {Demina}, R. and {Demirbozan}, U. and {DeRose}, J. and {Dey}, A. and {Dey}, B. and {Dhungana}, G. and {Ding}, J. and {Ding}, Z. and {Doel}, P. and {Doshi}, R. and {Douglass}, K. and {Edge}, A. and {Eftekharzadeh}, S. and {Eisenstein}, D.~J. and {Elliott}, A. and {Escoffier}, S. and {Fagrelius}, P. and {Fan}, X. and {Fanning}, K. and {Fawcett}, V.~A. and {Ferraro}, S. and {Ereza}, J. and {Flaugher}, B. and {Font-Ribera}, A. and {Forero-S{\'a}nchez}, D. and {Forero-Romero}, J.~E. and {Frenk}, C.~S. and {G{\"a}nsicke}, B.~T. and {Garc{\'\i}a}, L. {\'A}. and {Garc{\'\i}a-Bellido}, J. and {Garcia-Quintero}, C. and {Garrison}, L.~H. and {Gil-Mar{\'\i}n}, H. and {Golden-Marx}, J. and {Gontcho}, S. Gontcho A and {Gonzalez-Morales}, A.~X. and {Gonzalez-Perez}, V. and {Gordon}, C. and {Graur}, O. and {Green}, D. and {Gruen}, D. and {Guy}, J. and {Hadzhiyska}, B. and {Hahn}, C. and {Han}, J.~J. and {Hanif}, M.~M. S and {Herrera-Alcantar}, H.~K. and {Honscheid}, K. and {Hou}, J. and {Howlett}, C. and {Huterer}, D. and {Ir{\v{s}}i{\v{c}}}, V. and {Ishak}, M. and {Jacques}, A. and {Jana}, A. and {Jiang}, L. and {Jimenez}, J. and {Jing}, Y.~P. and {Joudaki}, S. and {Jullo}, E. and {Juneau}, S. and {Kizhuprakkat}, N. and {Kara{\c{c}}ayl{\i}}, N.~G. and {Karim}, T. and {Kehoe}, R. and {Kent}, S. and {Khederlarian}, A. and {Kim}, S. and {Kirkby}, D. and {Kisner}, T. and {Kitaura}, F. and {Kneib}, J. and {Koposov}, S.~E. and {Kov{\'a}cs}, A. and {Kremin}, A. and {Krolewski}, A. and {L'Huillier}, B. and {Lambert}, A. and {Lamman}, C. and {Lan}, T. -W. and {Landriau}, M. and {Lang}, D. and {Lange}, J.~U. and {Lasker}, J. and {Le Guillou}, L. and {Leauthaud}, A. and {Levi}, M.~E. and {Li}, T.~S. and {Linder}, E. and {Lyons}, A. and {Magneville}, C. and {Manera}, M. and {Manser}, C.~J. and {Margala}, D. and {Martini}, P. and {McDonald}, P. and {Medina}, G.~E. and {Medina-Varela}, L. and {Meisner}, A. and {Mena-Fern{\'a}ndez}, J. and {Meneses-Rizo}, J. and {Mezcua}, M. and {Miquel}, R. and {Montero-Camacho}, P. and {Moon}, J. and {Moore}, S. and {Moustakas}, J. and {Mueller}, E. and {Mundet}, J. and {Mu{\~n}oz-Guti{\'e}rrez}, A. and {Myers}, A.~D. and {Nadathur}, S. and {Napolitano}, L. and {Neveux}, R. and {Newman}, J.~A. and {Nie}, J. and {Nikutta}, R. and {Niz}, G. and {Norberg}, P. and {Noriega}, H.~E. and {Paillas}, E. and {Palanque-Delabrouille}, N. and {Palmese}, A. and {Zhiwei}, P. and {Parkinson}, D. and {Penmetsa}, S. and {Percival}, W.~J. and {P{\'e}rez-Fern{\'a}ndez}, A. and {P{\'e}rez-R{\`a}fols}, I. and {Pieri}, M. and {Poppett}, C. and {Porredon}, A. and {Pothier}, S. and {Prada}, F. and {Pucha}, R. and {Raichoor}, A. and {Ram{\'\i}rez-P{\'e}rez}, C. and {Ramirez-Solano}, S. and {Rashkovetskyi}, M. and {Ravoux}, C. and {Rocher}, A. and {Rockosi}, C. and {Ross}, A.~J. and {Rossi}, G. and {Ruggeri}, R. and {Ruhlmann-Kleider}, V. and {Sabiu}, C.~G. and {Said}, K. and {Saintonge}, A. and {Samushia}, L. and {Sanchez}, E. and {Saulder}, C. and {Schaan}, E. and {Schlafly}, E.~F. and {Schlegel}, D. and {Scholte}, D. and {Schubnell}, M. and {Seo}, H. and {Shafieloo}, A. and {Sharples}, R. and {Sheu}, W. and {Silber}, J. and {Sinigaglia}, F. and {Siudek}, M. and {Slepian}, Z. and {Smith}, A. and {Sprayberry}, D. and {Stephey}, L. and {Su{\'a}rez-P{\'e}rez}, J. and {Sun}, Z. and {Tan}, T. and {Tarl{\'e}}, G. and {Tojeiro}, R. and {Ure{\~n}a-L{\'o}pez}, L.~A. and {Vaisakh}, R. and {Valcin}, D. and {Valdes}, F. and {Valluri}, M. and {Vargas-Maga{\~n}a}, M. and {Variu}, A. and {Verde}, L. and {Walther}, M. and {Wang}, B. and {Wang}, M.~S. and {Weaver}, B.~A. and {Weaverdyck}, N. and {Wechsler}, R.~H. and {White}, M. and {Xie}, Y. and {Yang}, J. and {Y{\`e}che}, C. and {Yu}, J. and {Yuan}, S. and {Zhang}, H. and {Zhang}, Z. and {Zhao}, C. and {Zheng}, Z. and {Zhou}, R. and {Zhou}, Z. and {Zou}, H. and {Zou}, S. and {Zu}, Y.},
        title = "{The Early Data Release of the Dark Energy Spectroscopic Instrument}",
      journal = {arXiv e-prints},
     keywords = {Astrophysics - Cosmology and Nongalactic Astrophysics},
         year = 2023,
        month = jun,
          eid = {arXiv:2306.06308},
        pages = {arXiv:2306.06308},
          doi = {10.48550/arXiv.2306.06308},
        archivePrefix = {arXiv},
               eprint = {2306.06308},
         primaryClass = {astro-ph.CO},
               adsurl = {https://ui.adsabs.harvard.edu/abs/2023arXiv230606308D},
              adsnote = {Provided by the SAO/NASA Astrophysics Data System}
        }

        """,
    "desi_provabgs": 
        """
        @article{Hahn_2023,
        title={The DESI PRObabilistic Value-added Bright Galaxy Survey (PROVABGS) Mock Challenge},
        volume={945},
        ISSN={1538-4357},
        url={http://dx.doi.org/10.3847/1538-4357/ac8983},
        DOI={10.3847/1538-4357/ac8983},
        number={1},
        journal={The Astrophysical Journal},
        publisher={American Astronomical Society},
        author={Hahn, ChangHoon and Kwon, K. J. and Tojeiro, Rita and Siudek, Malgorzata and Canning, Rebecca E. A. and Mezcua, Mar and Tinker, Jeremy L. and Brooks, David and Doel, Peter and Fanning, Kevin and Gaztañaga, Enrique and Kehoe, Robert and Landriau, Martin and Meisner, Aaron and Moustakas, John and Poppett, Claire and Tarle, Gregory and Weiner, Benjamin and Zou, Hu},
        year={2023},
        month=mar, pages={16} }

        """,
    "foundation": 
        """
        @ARTICLE{2019ApJ...881...19J,
               author = {{Jones}, D.~O. and {Scolnic}, D.~M. and {Foley}, R.~J. and {Rest}, A. and {Kessler}, R. and {Challis}, P.~M. and {Chambers}, K.~C. and {Coulter}, D.~A. and {Dettman}, K.~G. and {Foley}, M.~M. and {Huber}, M.~E. and {Jha}, S.~W. and {Johnson}, E. and {Kilpatrick}, C.~D. and {Kirshner}, R.~P. and {Manuel}, J. and {Narayan}, G. and {Pan}, Y. -C. and {Riess}, A.~G. and {Schultz}, A.~S.~B. and {Siebert}, M.~R. and {Berger}, E. and {Chornock}, R. and {Flewelling}, H. and {Magnier}, E.~A. and {Smartt}, S.~J. and {Smith}, K.~W. and {Wainscoat}, R.~J. and {Waters}, C. and {Willman}, M.},
                title = "{The Foundation Supernova Survey: Measuring Cosmological Parameters with Supernovae from a Single Telescope}",
              journal = {\apj},
             keywords = {cosmology: observations, dark energy, supernovae: general, Astrophysics - Cosmology and Nongalactic Astrophysics},
                 year = 2019,
                month = aug,
               volume = {881},
               number = {1},
                  eid = {19},
                pages = {19},
                  doi = {10.3847/1538-4357/ab2bec},
        archivePrefix = {arXiv},
               eprint = {1811.09286},
         primaryClass = {astro-ph.CO},
               adsurl = {https://ui.adsabs.harvard.edu/abs/2019ApJ...881...19J},
              adsnote = {Provided by the SAO/NASA Astrophysics Data System}
        }

            @ARTICLE{2018MNRAS.475..193F,
               author = {{Foley}, Ryan J. and {Scolnic}, Daniel and {Rest}, Armin and {Jha}, S.~W. and {Pan}, Y. -C. and {Riess}, A.~G. and {Challis}, P. and {Chambers}, K.~C. and {Coulter}, D.~A. and {Dettman}, K.~G. and {Foley}, M.~M. and {Fox}, O.~D. and {Huber}, M.~E. and {Jones}, D.~O. and {Kilpatrick}, C.~D. and {Kirshner}, R.~P. and {Schultz}, A.~S.~B. and {Siebert}, M.~R. and {Flewelling}, H.~A. and {Gibson}, B. and {Magnier}, E.~A. and {Miller}, J.~A. and {Primak}, N. and {Smartt}, S.~J. and {Smith}, K.~W. and {Wainscoat}, R.~J. and {Waters}, C. and {Willman}, M.},
                title = "{The Foundation Supernova Survey: motivation, design, implementation, and first data release}",
              journal = {\mnras},
             keywords = {surveys, supernovae: general, dark energy, distance scale, cosmology: observations, Astrophysics - High Energy Astrophysical Phenomena, Astrophysics - Cosmology and Nongalactic Astrophysics},
                 year = 2018,
                month = mar,
               volume = {475},
               number = {1},
                pages = {193-219},
                  doi = {10.1093/mnras/stx3136},
        archivePrefix = {arXiv},
               eprint = {1711.02474},
         primaryClass = {astro-ph.HE},
               adsurl = {https://ui.adsabs.harvard.edu/abs/2018MNRAS.475..193F},
              adsnote = {Provided by the SAO/NASA Astrophysics Data System}
        }
        """,
    "gaia": 
        """
        @ARTICLE{2023A&A...674A..38G,
        author = {{Gaia Collaboration} and {Recio-Blanco}, A. and {Kordopatis}, G. and {de Laverny}, P. and {Palicio}, P.~A. and {Spagna}, A. and {Spina}, L. and {Katz}, D. and {Re Fiorentin}, P. and {Poggio}, E. and {McMillan}, P.~J. and {Vallenari}, A. and {Lattanzi}, M.~G. and {Seabroke}, G.~M. and {Casamiquela}, L. and {Bragaglia}, A. and {Antoja}, T. and {Bailer-Jones}, C.~A.~L. and {Schultheis}, M. and {Andrae}, R. and {Fouesneau}, M. and {Cropper}, M. and {Cantat-Gaudin}, T. and {Bijaoui}, A. and {Heiter}, U. and {Brown}, A.~G.~A. and {Prusti}, T. and {de Bruijne}, J.~H.~J. and {Arenou}, F. and {Babusiaux}, C. and {Biermann}, M. and {Creevey}, O.~L. and {Ducourant}, C. and {Evans}, D.~W. and {Eyer}, L. and {Guerra}, R. and {Hutton}, A. and {Jordi}, C. and {Klioner}, S.~A. and {Lammers}, U.~L. and {Lindegren}, L. and {Luri}, X. and {Mignard}, F. and {Panem}, C. and {Pourbaix}, D. and {Randich}, S. and {Sartoretti}, P. and {Soubiran}, C. and {Tanga}, P. and {Walton}, N.~A. and {Bastian}, U. and {Drimmel}, R. and {Jansen}, F. and {van Leeuwen}, F. and {Bakker}, J. and {Cacciari}, C. and {Casta{\~n}eda}, J. and {De Angeli}, F. and {Fabricius}, C. and {Fr{\'e}mat}, Y. and {Galluccio}, L. and {Guerrier}, A. and {Masana}, E. and {Messineo}, R. and {Mowlavi}, N. and {Nicolas}, C. and {Nienartowicz}, K. and {Pailler}, F. and {Panuzzo}, P. and {Riclet}, F. and {Roux}, W. and {Sordo}, R. and {Th{\'e}venin}, F. and {Gracia-Abril}, G. and {Portell}, J. and {Teyssier}, D. and {Altmann}, M. and {Audard}, M. and {Bellas-Velidis}, I. and {Benson}, K. and {Berthier}, J. and {Blomme}, R. and {Burgess}, P.~W. and {Busonero}, D. and {Busso}, G. and {C{\'a}novas}, H. and {Carry}, B. and {Cellino}, A. and {Cheek}, N. and {Clementini}, G. and {Damerdji}, Y. and {Davidson}, M. and {de Teodoro}, P. and {Nu{\~n}ez Campos}, M. and {Delchambre}, L. and {Dell'Oro}, A. and {Esquej}, P. and {Fern{\'a}ndez-Hern{\'a}ndez}, J. and {Fraile}, E. and {Garabato}, D. and {Garc{\'\i}a-Lario}, P. and {Gosset}, E. and {Haigron}, R. and {Halbwachs}, J. -L. and {Hambly}, N.~C. and {Harrison}, D.~L. and {Hern{\'a}ndez}, J. and {Hestroffer}, D. and {Hodgkin}, S.~T. and {Holl}, B. and {Jan{\ss}en}, K. and {Jevardat de Fombelle}, G. and {Jordan}, S. and {Krone-Martins}, A. and {Lanzafame}, A.~C. and {L{\"o}ffler}, W. and {Marchal}, O. and {Marrese}, P.~M. and {Moitinho}, A. and {Muinonen}, K. and {Osborne}, P. and {Pancino}, E. and {Pauwels}, T. and {Reyl{\'e}}, C. and {Riello}, M. and {Rimoldini}, L. and {Roegiers}, T. and {Rybizki}, J. and {Sarro}, L.~M. and {Siopis}, C. and {Smith}, M. and {Sozzetti}, A. and {Utrilla}, E. and {van Leeuwen}, M. and {Abbas}, U. and {{\'A}brah{\'a}m}, P. and {Abreu Aramburu}, A. and {Aerts}, C. and {Aguado}, J.~J. and {Ajaj}, M. and {Aldea-Montero}, F. and {Altavilla}, G. and {{\'A}lvarez}, M.~A. and {Alves}, J. and {Anders}, F. and {Anderson}, R.~I. and {Anglada Varela}, E. and {Baines}, D. and {Baker}, S.~G. and {Balaguer-N{\'u}{\~n}ez}, L. and {Balbinot}, E. and {Balog}, Z. and {Barache}, C. and {Barbato}, D. and {Barros}, M. and {Barstow}, M.~A. and {Bartolom{\'e}}, S. and {Bassilana}, J. -L. and {Bauchet}, N. and {Becciani}, U. and {Bellazzini}, M. and {Berihuete}, A. and {Bernet}, M. and {Bertone}, S. and {Bianchi}, L. and {Binnenfeld}, A. and {Blanco-Cuaresma}, S. and {Boch}, T. and {Bombrun}, A. and {Bossini}, D. and {Bouquillon}, S. and {Bramante}, L. and {Breedt}, E. and {Bressan}, A. and {Brouillet}, N. and {Brugaletta}, E. and {Bucciarelli}, B. and {Burlacu}, A. and {Butkevich}, A.~G. and {Buzzi}, R. and {Caffau}, E. and {Cancelliere}, R. and {Carballo}, R. and {Carlucci}, T. and {Carnerero}, M.~I. and {Carrasco}, J.~M. and {Castellani}, M. and {Castro-Ginard}, A. and {Chaoul}, L. and {Charlot}, P. and {Chemin}, L. and {Chiaramida}, V. and {Chiavassa}, A. and {Chornay}, N. and {Comoretto}, G. and {Contursi}, G. and {Cooper}, W.~J. and {Cornez}, T. and {Cowell}, S. and {Crifo}, F. and {Crosta}, M. and {Crowley}, C. and {Dafonte}, C. and {Dapergolas}, A. and {David}, P. and {De Luise}, F. and {De March}, R. and {De Ridder}, J. and {de Souza}, R. and {de Torres}, A. and {del Peloso}, E.~F. and {del Pozo}, E. and {Delbo}, M. and {Delgado}, A. and {Delisle}, J. -B. and {Demouchy}, C. and {Dharmawardena}, T.~E. and {Di Matteo}, P. and {Diakite}, S. and {Diener}, C. and {Distefano}, E. and {Dolding}, C. and {Edvardsson}, B. and {Enke}, H. and {Fabre}, C. and {Fabrizio}, M. and {Faigler}, S. and {Fedorets}, G. and {Fernique}, P. and {Figueras}, F. and {Fournier}, Y. and {Fouron}, C. and {Fragkoudi}, F. and {Gai}, M. and {Garcia-Gutierrez}, A. and {Garcia-Reinaldos}, M. and {Garc{\'\i}a-Torres}, M. and {Garofalo}, A. and {Gavel}, A. and {Gavras}, P. and {Gerlach}, E. and {Geyer}, R. and {Giacobbe}, P. and {Gilmore}, G. and {Girona}, S. and {Giuffrida}, G. and {Gomel}, R. and {Gomez}, A. and {Gonz{\'a}lez-N{\'u}{\~n}ez}, J. and {Gonz{\'a}lez-Santamar{\'\i}a}, I. and {Gonz{\'a}lez-Vidal}, J.~J. and {Granvik}, M. and {Guillout}, P. and {Guiraud}, J. and {Guti{\'e}rrez-S{\'a}nchez}, R. and {Guy}, L.~P. and {Hatzidimitriou}, D. and {Hauser}, M. and {Haywood}, M. and {Helmer}, A. and {Helmi}, A. and {Sarmiento}, M.~H. and {Hidalgo}, S.~L. and {H{\l}adczuk}, N. and {Hobbs}, D. and {Holland}, G. and {Huckle}, H.~E. and {Jardine}, K. and {Jasniewicz}, G. and {Jean-Antoine Piccolo}, A. and {Jim{\'e}nez-Arranz}, {\'O}. and {Juaristi Campillo}, J. and {Julbe}, F. and {Karbevska}, L. and {Kervella}, P. and {Khanna}, S. and {Korn}, A.~J. and {K{\'o}sp{\'a}l}, {\'A}. and {Kostrzewa-Rutkowska}, Z. and {Kruszy{\'n}ska}, K. and {Kun}, M. and {Laizeau}, P. and {Lambert}, S. and {Lanza}, A.~F. and {Lasne}, Y. and {Le Campion}, J. -F. and {Lebreton}, Y. and {Lebzelter}, T. and {Leccia}, S. and {Leclerc}, N. and {Lecoeur-Taibi}, I. and {Liao}, S. and {Licata}, E.~L. and {Lindstr{\o}m}, H.~E.~P. and {Lister}, T.~A. and {Livanou}, E. and {Lobel}, A. and {Lorca}, A. and {Loup}, C. and {Madrero Pardo}, P. and {Magdaleno Romeo}, A. and {Managau}, S. and {Mann}, R.~G. and {Manteiga}, M. and {Marchant}, J.~M. and {Marconi}, M. and {Marcos}, J. and {Marcos Santos}, M.~M.~S. and {Mar{\'\i}n Pina}, D. and {Marinoni}, S. and {Marocco}, F. and {Marshall}, D.~J. and {Martin Polo}, L. and {Mart{\'\i}n-Fleitas}, J.~M. and {Marton}, G. and {Mary}, N. and {Masip}, A. and {Massari}, D. and {Mastrobuono-Battisti}, A. and {Mazeh}, T. and {Messina}, S. and {Michalik}, D. and {Millar}, N.~R. and {Mints}, A. and {Molina}, D. and {Molinaro}, R. and {Moln{\'a}r}, L. and {Monari}, G. and {Mongui{\'o}}, M. and {Montegriffo}, P. and {Montero}, A. and {Mor}, R. and {Mora}, A. and {Morbidelli}, R. and {Morel}, T. and {Morris}, D. and {Muraveva}, T. and {Murphy}, C.~P. and {Musella}, I. and {Nagy}, Z. and {Noval}, L. and {Oca{\~n}a}, F. and {Ogden}, A. and {Ordenovic}, C. and {Osinde}, J.~O. and {Pagani}, C. and {Pagano}, I. and {Palaversa}, L. and {Pallas-Quintela}, L. and {Panahi}, A. and {Payne-Wardenaar}, S. and {Pe{\~n}alosa Esteller}, X. and {Penttil{\"a}}, A. and {Pichon}, B. and {Piersimoni}, A.~M. and {Pineau}, F. -X. and {Plachy}, E. and {Plum}, G. and {Pr{\v{s}}a}, A. and {Pulone}, L. and {Racero}, E. and {Ragaini}, S. and {Rainer}, M. and {Raiteri}, C.~M. and {Ramos}, P. and {Ramos-Lerate}, M. and {Regibo}, S. and {Richards}, P.~J. and {Rios Diaz}, C. and {Ripepi}, V. and {Riva}, A. and {Rix}, H. -W. and {Rixon}, G. and {Robichon}, N. and {Robin}, A.~C. and {Robin}, C. and {Roelens}, M. and {Rogues}, H.~R.~O. and {Rohrbasser}, L. and {Romero-G{\'o}mez}, M. and {Rowell}, N. and {Royer}, F. and {Ruz Mieres}, D. and {Rybicki}, K.~A. and {Sadowski}, G. and {S{\'a}ez N{\'u}{\~n}ez}, A. and {Sagrist{\`a} Sell{\'e}s}, A. and {Sahlmann}, J. and {Salguero}, E. and {Samaras}, N. and {Sanchez Gimenez}, V. and {Sanna}, N. and {Santove{\~n}a}, R. and {Sarasso}, M. and {Sciacca}, E. and {Segol}, M. and {Segovia}, J.~C. and {S{\'e}gransan}, D. and {Semeux}, D. and {Shahaf}, S. and {Siddiqui}, H.~I. and {Siebert}, A. and {Siltala}, L. and {Silvelo}, A. and {Slezak}, E. and {Slezak}, I. and {Smart}, R.~L. and {Snaith}, O.~N. and {Solano}, E. and {Solitro}, F. and {Souami}, D. and {Souchay}, J. and {Spoto}, F. and {Steele}, I.~A. and {Steidelm{\"u}ller}, H. and {Stephenson}, C.~A. and {S{\"u}veges}, M. and {Surdej}, J. and {Szabados}, L. and {Szegedi-Elek}, E. and {Taris}, F. and {Taylor}, M.~B. and {Teixeira}, R. and {Tolomei}, L. and {Tonello}, N. and {Torra}, F. and {Torra}, J. and {Torralba Elipe}, G. and {Trabucchi}, M. and {Tsounis}, A.~T. and {Turon}, C. and {Ulla}, A. and {Unger}, N. and {Vaillant}, M.~V. and {van Dillen}, E. and {van Reeven}, W. and {Vanel}, O. and {Vecchiato}, A. and {Viala}, Y. and {Vicente}, D. and {Voutsinas}, S. and {Weiler}, M. and {Wevers}, T. and {Wyrzykowski}, {\L}. and {Yoldas}, A. and {Yvard}, P. and {Zhao}, H. and {Zorec}, J. and {Zucker}, S. and {Zwitter}, T.},
        title = "{Gaia Data Release 3. Chemical cartography of the Milky Way}",
        journal = {\aap},
        keywords = {Galaxy: abundances, stars: abundances, Galaxy: evolution, Galaxy: kinematics and dynamics, Galaxy: disk, Galaxy: halo, Astrophysics - Astrophysics of Galaxies, Astrophysics - Cosmology and Nongalactic Astrophysics, Astrophysics - Earth and Planetary Astrophysics, Astrophysics - High Energy Astrophysical Phenomena, Astrophysics - Instrumentation and Methods for Astrophysics, Astrophysics - Solar and Stellar Astrophysics},
         year = 2023,
        month = jun,
        volume = {674},
          eid = {A38},
        pages = {A38},
          doi = {10.1051/0004-6361/202243511},
        archivePrefix = {arXiv},
        eprint = {2206.05534},
        primaryClass = {astro-ph.GA},
        adsurl = {https://ui.adsabs.harvard.edu/abs/2023A&A...674A..38G},
        adsnote = {Provided by the SAO/NASA Astrophysics Data System}
        }


        @ARTICLE{2016A&A...595A...1G,
        author = {{Gaia Collaboration} and {Prusti}, T. and {de Bruijne}, J.~H.~J. and {Brown}, A.~G.~A. and {Vallenari}, A. and {Babusiaux}, C. and {Bailer-Jones}, C.~A.~L. and {Bastian}, U. and {Biermann}, M. and {Evans}, D.~W. and {Eyer}, L. and {Jansen}, F. and {Jordi}, C. and {Klioner}, S.~A. and {Lammers}, U. and {Lindegren}, L. and {Luri}, X. and {Mignard}, F. and {Milligan}, D.~J. and {Panem}, C. and {Poinsignon}, V. and {Pourbaix}, D. and {Randich}, S. and {Sarri}, G. and {Sartoretti}, P. and {Siddiqui}, H.~I. and {Soubiran}, C. and {Valette}, V. and {van Leeuwen}, F. and {Walton}, N.~A. and {Aerts}, C. and {Arenou}, F. and {Cropper}, M. and {Drimmel}, R. and {H{\o}g}, E. and {Katz}, D. and {Lattanzi}, M.~G. and {O'Mullane}, W. and {Grebel}, E.~K. and {Holland}, A.~D. and {Huc}, C. and {Passot}, X. and {Bramante}, L. and {Cacciari}, C. and {Casta{\~n}eda}, J. and {Chaoul}, L. and {Cheek}, N. and {De Angeli}, F. and {Fabricius}, C. and {Guerra}, R. and {Hern{\'a}ndez}, J. and {Jean-Antoine-Piccolo}, A. and {Masana}, E. and {Messineo}, R. and {Mowlavi}, N. and {Nienartowicz}, K. and {Ord{\'o}{\~n}ez-Blanco}, D. and {Panuzzo}, P. and {Portell}, J. and {Richards}, P.~J. and {Riello}, M. and {Seabroke}, G.~M. and {Tanga}, P. and {Th{\'e}venin}, F. and {Torra}, J. and {Els}, S.~G. and {Gracia-Abril}, G. and {Comoretto}, G. and {Garcia-Reinaldos}, M. and {Lock}, T. and {Mercier}, E. and {Altmann}, M. and {Andrae}, R. and {Astraatmadja}, T.~L. and {Bellas-Velidis}, I. and {Benson}, K. and {Berthier}, J. and {Blomme}, R. and {Busso}, G. and {Carry}, B. and {Cellino}, A. and {Clementini}, G. and {Cowell}, S. and {Creevey}, O. and {Cuypers}, J. and {Davidson}, M. and {De Ridder}, J. and {de Torres}, A. and {Delchambre}, L. and {Dell'Oro}, A. and {Ducourant}, C. and {Fr{\'e}mat}, Y. and {Garc{\'\i}a-Torres}, M. and {Gosset}, E. and {Halbwachs}, J. -L. and {Hambly}, N.~C. and {Harrison}, D.~L. and {Hauser}, M. and {Hestroffer}, D. and {Hodgkin}, S.~T. and {Huckle}, H.~E. and {Hutton}, A. and {Jasniewicz}, G. and {Jordan}, S. and {Kontizas}, M. and {Korn}, A.~J. and {Lanzafame}, A.~C. and {Manteiga}, M. and {Moitinho}, A. and {Muinonen}, K. and {Osinde}, J. and {Pancino}, E. and {Pauwels}, T. and {Petit}, J. -M. and {Recio-Blanco}, A. and {Robin}, A.~C. and {Sarro}, L.~M. and {Siopis}, C. and {Smith}, M. and {Smith}, K.~W. and {Sozzetti}, A. and {Thuillot}, W. and {van Reeven}, W. and {Viala}, Y. and {Abbas}, U. and {Abreu Aramburu}, A. and {Accart}, S. and {Aguado}, J.~J. and {Allan}, P.~M. and {Allasia}, W. and {Altavilla}, G. and {{\'A}lvarez}, M.~A. and {Alves}, J. and {Anderson}, R.~I. and {Andrei}, A.~H. and {Anglada Varela}, E. and {Antiche}, E. and {Antoja}, T. and {Ant{\'o}n}, S. and {Arcay}, B. and {Atzei}, A. and {Ayache}, L. and {Bach}, N. and {Baker}, S.~G. and {Balaguer-N{\'u}{\~n}ez}, L. and {Barache}, C. and {Barata}, C. and {Barbier}, A. and {Barblan}, F. and {Baroni}, M. and {Barrado y Navascu{\'e}s}, D. and {Barros}, M. and {Barstow}, M.~A. and {Becciani}, U. and {Bellazzini}, M. and {Bellei}, G. and {Bello Garc{\'\i}a}, A. and {Belokurov}, V. and {Bendjoya}, P. and {Berihuete}, A. and {Bianchi}, L. and {Bienaym{\'e}}, O. and {Billebaud}, F. and {Blagorodnova}, N. and {Blanco-Cuaresma}, S. and {Boch}, T. and {Bombrun}, A. and {Borrachero}, R. and {Bouquillon}, S. and {Bourda}, G. and {Bouy}, H. and {Bragaglia}, A. and {Breddels}, M.~A. and {Brouillet}, N. and {Br{\"u}semeister}, T. and {Bucciarelli}, B. and {Budnik}, F. and {Burgess}, P. and {Burgon}, R. and {Burlacu}, A. and {Busonero}, D. and {Buzzi}, R. and {Caffau}, E. and {Cambras}, J. and {Campbell}, H. and {Cancelliere}, R. and {Cantat-Gaudin}, T. and {Carlucci}, T. and {Carrasco}, J.~M. and {Castellani}, M. and {Charlot}, P. and {Charnas}, J. and {Charvet}, P. and {Chassat}, F. and {Chiavassa}, A. and {Clotet}, M. and {Cocozza}, G. and {Collins}, R.~S. and {Collins}, P. and {Costigan}, G. and {Crifo}, F. and {Cross}, N.~J.~G. and {Crosta}, M. and {Crowley}, C. and {Dafonte}, C. and {Damerdji}, Y. and {Dapergolas}, A. and {David}, P. and {David}, M. and {De Cat}, P. and {de Felice}, F. and {de Laverny}, P. and {De Luise}, F. and {De March}, R. and {de Martino}, D. and {de Souza}, R. and {Debosscher}, J. and {del Pozo}, E. and {Delbo}, M. and {Delgado}, A. and {Delgado}, H.~E. and {di Marco}, F. and {Di Matteo}, P. and {Diakite}, S. and {Distefano}, E. and {Dolding}, C. and {Dos Anjos}, S. and {Drazinos}, P. and {Dur{\'a}n}, J. and {Dzigan}, Y. and {Ecale}, E. and {Edvardsson}, B. and {Enke}, H. and {Erdmann}, M. and {Escolar}, D. and {Espina}, M. and {Evans}, N.~W. and {Eynard Bontemps}, G. and {Fabre}, C. and {Fabrizio}, M. and {Faigler}, S. and {Falc{\~a}o}, A.~J. and {Farr{\`a}s Casas}, M. and {Faye}, F. and {Federici}, L. and {Fedorets}, G. and {Fern{\'a}ndez-Hern{\'a}ndez}, J. and {Fernique}, P. and {Fienga}, A. and {Figueras}, F. and {Filippi}, F. and {Findeisen}, K. and {Fonti}, A. and {Fouesneau}, M. and {Fraile}, E. and {Fraser}, M. and {Fuchs}, J. and {Furnell}, R. and {Gai}, M. and {Galleti}, S. and {Galluccio}, L. and {Garabato}, D. and {Garc{\'\i}a-Sedano}, F. and {Gar{\'e}}, P. and {Garofalo}, A. and {Garralda}, N. and {Gavras}, P. and {Gerssen}, J. and {Geyer}, R. and {Gilmore}, G. and {Girona}, S. and {Giuffrida}, G. and {Gomes}, M. and {Gonz{\'a}lez-Marcos}, A. and {Gonz{\'a}lez-N{\'u}{\~n}ez}, J. and {Gonz{\'a}lez-Vidal}, J.~J. and {Granvik}, M. and {Guerrier}, A. and {Guillout}, P. and {Guiraud}, J. and {G{\'u}rpide}, A. and {Guti{\'e}rrez-S{\'a}nchez}, R. and {Guy}, L.~P. and {Haigron}, R. and {Hatzidimitriou}, D. and {Haywood}, M. and {Heiter}, U. and {Helmi}, A. and {Hobbs}, D. and {Hofmann}, W. and {Holl}, B. and {Holland}, G. and {Hunt}, J.~A.~S. and {Hypki}, A. and {Icardi}, V. and {Irwin}, M. and {Jevardat de Fombelle}, G. and {Jofr{\'e}}, P. and {Jonker}, P.~G. and {Jorissen}, A. and {Julbe}, F. and {Karampelas}, A. and {Kochoska}, A. and {Kohley}, R. and {Kolenberg}, K. and {Kontizas}, E. and {Koposov}, S.~E. and {Kordopatis}, G. and {Koubsky}, P. and {Kowalczyk}, A. and {Krone-Martins}, A. and {Kudryashova}, M. and {Kull}, I. and {Bachchan}, R.~K. and {Lacoste-Seris}, F. and {Lanza}, A.~F. and {Lavigne}, J. -B. and {Le Poncin-Lafitte}, C. and {Lebreton}, Y. and {Lebzelter}, T. and {Leccia}, S. and {Leclerc}, N. and {Lecoeur-Taibi}, I. and {Lemaitre}, V. and {Lenhardt}, H. and {Leroux}, F. and {Liao}, S. and {Licata}, E. and {Lindstr{\o}m}, H.~E.~P. and {Lister}, T.~A. and {Livanou}, E. and {Lobel}, A. and {L{\"o}ffler}, W. and {L{\'o}pez}, M. and {Lopez-Lozano}, A. and {Lorenz}, D. and {Loureiro}, T. and {MacDonald}, I. and {Magalh{\~a}es Fernandes}, T. and {Managau}, S. and {Mann}, R.~G. and {Mantelet}, G. and {Marchal}, O. and {Marchant}, J.~M. and {Marconi}, M. and {Marie}, J. and {Marinoni}, S. and {Marrese}, P.~M. and {Marschalk{\'o}}, G. and {Marshall}, D.~J. and {Mart{\'\i}n-Fleitas}, J.~M. and {Martino}, M. and {Mary}, N. and {Matijevi{\v{c}}}, G. and {Mazeh}, T. and {McMillan}, P.~J. and {Messina}, S. and {Mestre}, A. and {Michalik}, D. and {Millar}, N.~R. and {Miranda}, B.~M.~H. and {Molina}, D. and {Molinaro}, R. and {Molinaro}, M. and {Moln{\'a}r}, L. and {Moniez}, M. and {Montegriffo}, P. and {Monteiro}, D. and {Mor}, R. and {Mora}, A. and {Morbidelli}, R. and {Morel}, T. and {Morgenthaler}, S. and {Morley}, T. and {Morris}, D. and {Mulone}, A.~F. and {Muraveva}, T. and {Musella}, I. and {Narbonne}, J. and {Nelemans}, G. and {Nicastro}, L. and {Noval}, L. and {Ord{\'e}novic}, C. and {Ordieres-Mer{\'e}}, J. and {Osborne}, P. and {Pagani}, C. and {Pagano}, I. and {Pailler}, F. and {Palacin}, H. and {Palaversa}, L. and {Parsons}, P. and {Paulsen}, T. and {Pecoraro}, M. and {Pedrosa}, R. and {Pentik{\"a}inen}, H. and {Pereira}, J. and {Pichon}, B. and {Piersimoni}, A.~M. and {Pineau}, F. -X. and {Plachy}, E. and {Plum}, G. and {Poujoulet}, E. and {Pr{\v{s}}a}, A. and {Pulone}, L. and {Ragaini}, S. and {Rago}, S. and {Rambaux}, N. and {Ramos-Lerate}, M. and {Ranalli}, P. and {Rauw}, G. and {Read}, A. and {Regibo}, S. and {Renk}, F. and {Reyl{\'e}}, C. and {Ribeiro}, R.~A. and {Rimoldini}, L. and {Ripepi}, V. and {Riva}, A. and {Rixon}, G. and {Roelens}, M. and {Romero-G{\'o}mez}, M. and {Rowell}, N. and {Royer}, F. and {Rudolph}, A. and {Ruiz-Dern}, L. and {Sadowski}, G. and {Sagrist{\`a} Sell{\'e}s}, T. and {Sahlmann}, J. and {Salgado}, J. and {Salguero}, E. and {Sarasso}, M. and {Savietto}, H. and {Schnorhk}, A. and {Schultheis}, M. and {Sciacca}, E. and {Segol}, M. and {Segovia}, J.~C. and {Segransan}, D. and {Serpell}, E. and {Shih}, I. -C. and {Smareglia}, R. and {Smart}, R.~L. and {Smith}, C. and {Solano}, E. and {Solitro}, F. and {Sordo}, R. and {Soria Nieto}, S. and {Souchay}, J. and {Spagna}, A. and {Spoto}, F. and {Stampa}, U. and {Steele}, I.~A. and {Steidelm{\"u}ller}, H. and {Stephenson}, C.~A. and {Stoev}, H. and {Suess}, F.~F. and {S{\"u}veges}, M. and {Surdej}, J. and {Szabados}, L. and {Szegedi-Elek}, E. and {Tapiador}, D. and {Taris}, F. and {Tauran}, G. and {Taylor}, M.~B. and {Teixeira}, R. and {Terrett}, D. and {Tingley}, B. and {Trager}, S.~C. and {Turon}, C. and {Ulla}, A. and {Utrilla}, E. and {Valentini}, G. and {van Elteren}, A. and {Van Hemelryck}, E. and {van Leeuwen}, M. and {Varadi}, M. and {Vecchiato}, A. and {Veljanoski}, J. and {Via}, T. and {Vicente}, D. and {Vogt}, S. and {Voss}, H. and {Votruba}, V. and {Voutsinas}, S. and {Walmsley}, G. and {Weiler}, M. and {Weingrill}, K. and {Werner}, D. and {Wevers}, T. and {Whitehead}, G. and {Wyrzykowski}, {\L}. and {Yoldas}, A. and {{\v{Z}}erjal}, M. and {Zucker}, S. and {Zurbach}, C. and {Zwitter}, T. and {Alecu}, A. and {Allen}, M. and {Allende Prieto}, C. and {Amorim}, A. and {Anglada-Escud{\'e}}, G. and {Arsenijevic}, V. and {Azaz}, S. and {Balm}, P. and {Beck}, M. and {Bernstein}, H. -H. and {Bigot}, L. and {Bijaoui}, A. and {Blasco}, C. and {Bonfigli}, M. and {Bono}, G. and {Boudreault}, S. and {Bressan}, A. and {Brown}, S. and {Brunet}, P. -M. and {Bunclark}, P. and {Buonanno}, R. and {Butkevich}, A.~G. and {Carret}, C. and {Carrion}, C. and {Chemin}, L. and {Ch{\'e}reau}, F. and {Corcione}, L. and {Darmigny}, E. and {de Boer}, K.~S. and {de Teodoro}, P. and {de Zeeuw}, P.~T. and {Delle Luche}, C. and {Domingues}, C.~D. and {Dubath}, P. and {Fodor}, F. and {Fr{\'e}zouls}, B. and {Fries}, A. and {Fustes}, D. and {Fyfe}, D. and {Gallardo}, E. and {Gallegos}, J. and {Gardiol}, D. and {Gebran}, M. and {Gomboc}, A. and {G{\'o}mez}, A. and {Grux}, E. and {Gueguen}, A. and {Heyrovsky}, A. and {Hoar}, J. and {Iannicola}, G. and {Isasi Parache}, Y. and {Janotto}, A. -M. and {Joliet}, E. and {Jonckheere}, A. and {Keil}, R. and {Kim}, D. -W. and {Klagyivik}, P. and {Klar}, J. and {Knude}, J. and {Kochukhov}, O. and {Kolka}, I. and {Kos}, J. and {Kutka}, A. and {Lainey}, V. and {LeBouquin}, D. and {Liu}, C. and {Loreggia}, D. and {Makarov}, V.~V. and {Marseille}, M.~G. and {Martayan}, C. and {Martinez-Rubi}, O. and {Massart}, B. and {Meynadier}, F. and {Mignot}, S. and {Munari}, U. and {Nguyen}, A. -T. and {Nordlander}, T. and {Ocvirk}, P. and {O'Flaherty}, K.~S. and {Olias Sanz}, A. and {Ortiz}, P. and {Osorio}, J. and {Oszkiewicz}, D. and {Ouzounis}, A. and {Palmer}, M. and {Park}, P. and {Pasquato}, E. and {Peltzer}, C. and {Peralta}, J. and {P{\'e}turaud}, F. and {Pieniluoma}, T. and {Pigozzi}, E. and {Poels}, J. and {Prat}, G. and {Prod'homme}, T. and {Raison}, F. and {Rebordao}, J.~M. and {Risquez}, D. and {Rocca-Volmerange}, B. and {Rosen}, S. and {Ruiz-Fuertes}, M.~I. and {Russo}, F. and {Sembay}, S. and {Serraller Vizcaino}, I. and {Short}, A. and {Siebert}, A. and {Silva}, H. and {Sinachopoulos}, D. and {Slezak}, E. and {Soffel}, M. and {Sosnowska}, D. and {Strai{\v{z}}ys}, V. and {ter Linden}, M. and {Terrell}, D. and {Theil}, S. and {Tiede}, C. and {Troisi}, L. and {Tsalmantza}, P. and {Tur}, D. and {Vaccari}, M. and {Vachier}, F. and {Valles}, P. and {Van Hamme}, W. and {Veltz}, L. and {Virtanen}, J. and {Wallut}, J. -M. and {Wichmann}, R. and {Wilkinson}, M.~I. and {Ziaeepour}, H. and {Zschocke}, S.},
        title = "{The Gaia mission}",
        journal = {\aap},
        keywords = {space vehicles: instruments, Galaxy: structure, astrometry, parallaxes, proper motions, telescopes, Astrophysics - Instrumentation and Methods for Astrophysics},
         year = 2016,
        month = nov,
        volume = {595},
          eid = {A1},
        pages = {A1},
          doi = {10.1051/0004-6361/201629272},
        archivePrefix = {arXiv},
        eprint = {1609.04153},
        primaryClass = {astro-ph.IM},
        adsurl = {https://ui.adsabs.harvard.edu/abs/2016A&A...595A...1G},
        adsnote = {Provided by the SAO/NASA Astrophysics Data System}
        }


        """,
    "gz10": 
        """

        @ARTICLE{2008MNRAS.389.1179L,
               author = {{Lintott}, Chris J. and {Schawinski}, Kevin and {Slosar}, An{\v{z}}e and {Land}, Kate and {Bamford}, Steven and {Thomas}, Daniel and {Raddick}, M. Jordan and {Nichol}, Robert C. and {Szalay}, Alex and {Andreescu}, Dan and {Murray}, Phil and {Vandenberg}, Jan},
                title = "{Galaxy Zoo: morphologies derived from visual inspection of galaxies from the Sloan Digital Sky Survey}",
              journal = {\mnras},
             keywords = {methods: data analysis, galaxies: elliptical and lenticular, cD, galaxies: general, galaxies: spiral, Astrophysics},
                 year = 2008,
                month = sep,
               volume = {389},
               number = {3},
                pages = {1179-1189},
                  doi = {10.1111/j.1365-2966.2008.13689.x},
        archivePrefix = {arXiv},
               eprint = {0804.4483},
         primaryClass = {astro-ph},
               adsurl = {https://ui.adsabs.harvard.edu/abs/2008MNRAS.389.1179L},
              adsnote = {Provided by the SAO/NASA Astrophysics Data System}
        }

        @ARTICLE{2011MNRAS.410..166L,
               author = {{Lintott}, Chris and {Schawinski}, Kevin and {Bamford}, Steven and {Slosar}, An{\r{a}}{\textthreequarters}e and {Land}, Kate and {Thomas}, Daniel and {Edmondson}, Edd and {Masters}, Karen and {Nichol}, Robert C. and {Raddick}, M. Jordan and {Szalay}, Alex and {Andreescu}, Dan and {Murray}, Phil and {Vandenberg}, Jan},
                title = "{Galaxy Zoo 1: data release of morphological classifications for nearly 900 000 galaxies}",
              journal = {\mnras},
             keywords = {methods: data analysis, galaxies: elliptical and lenticular, cD, galaxies: general, galaxies: spiral, Astrophysics - Galaxy Astrophysics, Astrophysics - Cosmology and Extragalactic Astrophysics},
                 year = 2011,
                month = jan,
               volume = {410},
               number = {1},
                pages = {166-178},
                  doi = {10.1111/j.1365-2966.2010.17432.x},
        archivePrefix = {arXiv},
               eprint = {1007.3265},
         primaryClass = {astro-ph.GA},
               adsurl = {https://ui.adsabs.harvard.edu/abs/2011MNRAS.410..166L},
              adsnote = {Provided by the SAO/NASA Astrophysics Data System}
        }

        @ARTICLE{2022MNRAS.509.3966W,
               author = {{Walmsley}, Mike and {Lintott}, Chris and {G{\'e}ron}, Tobias and {Kruk}, Sandor and {Krawczyk}, Coleman and {Willett}, Kyle W. and {Bamford}, Steven and {Kelvin}, Lee S. and {Fortson}, Lucy and {Gal}, Yarin and {Keel}, William and {Masters}, Karen L. and {Mehta}, Vihang and {Simmons}, Brooke D. and {Smethurst}, Rebecca and {Smith}, Lewis and {Baeten}, Elisabeth M. and {Macmillan}, Christine},
                title = "{Galaxy Zoo DECaLS: Detailed visual morphology measurements from volunteers and deep learning for 314 000 galaxies}",
              journal = {\mnras},
             keywords = {methods: data analysis, galaxies: bar, galaxies: general, galaxies: interactions, Astrophysics - Astrophysics of Galaxies, Computer Science - Computer Vision and Pattern Recognition},
                 year = 2022,
                month = jan,
               volume = {509},
               number = {3},
                pages = {3966-3988},
                  doi = {10.1093/mnras/stab2093},
        archivePrefix = {arXiv},
               eprint = {2102.08414},
         primaryClass = {astro-ph.GA},
               adsurl = {https://ui.adsabs.harvard.edu/abs/2022MNRAS.509.3966W},
              adsnote = {Provided by the SAO/NASA Astrophysics Data System}
        }

        @ARTICLE{2019AJ....157..168D,
               author = {{Dey}, Arjun and {Schlegel}, David J. and {Lang}, Dustin and {Blum}, Robert and {Burleigh}, Kaylan and {Fan}, Xiaohui and {Findlay}, Joseph R. and {Finkbeiner}, Doug and {Herrera}, David and {Juneau}, St{\'e}phanie and {Landriau}, Martin and {Levi}, Michael and {McGreer}, Ian and {Meisner}, Aaron and {Myers}, Adam D. and {Moustakas}, John and {Nugent}, Peter and {Patej}, Anna and {Schlafly}, Edward F. and {Walker}, Alistair R. and {Valdes}, Francisco and {Weaver}, Benjamin A. and {Y{\`e}che}, Christophe and {Zou}, Hu and {Zhou}, Xu and {Abareshi}, Behzad and {Abbott}, T.~M.~C. and {Abolfathi}, Bela and {Aguilera}, C. and {Alam}, Shadab and {Allen}, Lori and {Alvarez}, A. and {Annis}, James and {Ansarinejad}, Behzad and {Aubert}, Marie and {Beechert}, Jacqueline and {Bell}, Eric F. and {BenZvi}, Segev Y. and {Beutler}, Florian and {Bielby}, Richard M. and {Bolton}, Adam S. and {Brice{\~n}o}, C{\'e}sar and {Buckley-Geer}, Elizabeth J. and {Butler}, Karen and {Calamida}, Annalisa and {Carlberg}, Raymond G. and {Carter}, Paul and {Casas}, Ricard and {Castander}, Francisco J. and {Choi}, Yumi and {Comparat}, Johan and {Cukanovaite}, Elena and {Delubac}, Timoth{\'e}e and {DeVries}, Kaitlin and {Dey}, Sharmila and {Dhungana}, Govinda and {Dickinson}, Mark and {Ding}, Zhejie and {Donaldson}, John B. and {Duan}, Yutong and {Duckworth}, Christopher J. and {Eftekharzadeh}, Sarah and {Eisenstein}, Daniel J. and {Etourneau}, Thomas and {Fagrelius}, Parker A. and {Farihi}, Jay and {Fitzpatrick}, Mike and {Font-Ribera}, Andreu and {Fulmer}, Leah and {G{\"a}nsicke}, Boris T. and {Gaztanaga}, Enrique and {George}, Koshy and {Gerdes}, David W. and {Gontcho}, Satya Gontcho A. and {Gorgoni}, Claudio and {Green}, Gregory and {Guy}, Julien and {Harmer}, Diane and {Hernandez}, M. and {Honscheid}, Klaus and {Huang}, Lijuan Wendy and {James}, David J. and {Jannuzi}, Buell T. and {Jiang}, Linhua and {Joyce}, Richard and {Karcher}, Armin and {Karkar}, Sonia and {Kehoe}, Robert and {Kneib}, Jean-Paul and {Kueter-Young}, Andrea and {Lan}, Ting-Wen and {Lauer}, Tod R. and {Le Guillou}, Laurent and {Le Van Suu}, Auguste and {Lee}, Jae Hyeon and {Lesser}, Michael and {Perreault Levasseur}, Laurence and {Li}, Ting S. and {Mann}, Justin L. and {Marshall}, Robert and {Mart{\'\i}nez-V{\'a}zquez}, C.~E. and {Martini}, Paul and {du Mas des Bourboux}, H{\'e}lion and {McManus}, Sean and {Meier}, Tobias Gabriel and {M{\'e}nard}, Brice and {Metcalfe}, Nigel and {Mu{\~n}oz-Guti{\'e}rrez}, Andrea and {Najita}, Joan and {Napier}, Kevin and {Narayan}, Gautham and {Newman}, Jeffrey A. and {Nie}, Jundan and {Nord}, Brian and {Norman}, Dara J. and {Olsen}, Knut A.~G. and {Paat}, Anthony and {Palanque-Delabrouille}, Nathalie and {Peng}, Xiyan and {Poppett}, Claire L. and {Poremba}, Megan R. and {Prakash}, Abhishek and {Rabinowitz}, David and {Raichoor}, Anand and {Rezaie}, Mehdi and {Robertson}, A.~N. and {Roe}, Natalie A. and {Ross}, Ashley J. and {Ross}, Nicholas P. and {Rudnick}, Gregory and {Safonova}, Sasha and {Saha}, Abhijit and {S{\'a}nchez}, F. Javier and {Savary}, Elodie and {Schweiker}, Heidi and {Scott}, Adam and {Seo}, Hee-Jong and {Shan}, Huanyuan and {Silva}, David R. and {Slepian}, Zachary and {Soto}, Christian and {Sprayberry}, David and {Staten}, Ryan and {Stillman}, Coley M. and {Stupak}, Robert J. and {Summers}, David L. and {Sien Tie}, Suk and {Tirado}, H. and {Vargas-Maga{\~n}a}, Mariana and {Vivas}, A. Katherina and {Wechsler}, Risa H. and {Williams}, Doug and {Yang}, Jinyi and {Yang}, Qian and {Yapici}, Tolga and {Zaritsky}, Dennis and {Zenteno}, A. and {Zhang}, Kai and {Zhang}, Tianmeng and {Zhou}, Rongpu and {Zhou}, Zhimin},
                title = "{Overview of the DESI Legacy Imaging Surveys}",
              journal = {\aj},
             keywords = {catalogs, surveys, Astrophysics - Instrumentation and Methods for Astrophysics},
                 year = 2019,
                month = may,
               volume = {157},
               number = {5},
                  eid = {168},
                pages = {168},
                  doi = {10.3847/1538-3881/ab089d},
        archivePrefix = {arXiv},
               eprint = {1804.08657},
         primaryClass = {astro-ph.IM},
               adsurl = {https://ui.adsabs.harvard.edu/abs/2019AJ....157..168D},
              adsnote = {Provided by the SAO/NASA Astrophysics Data System}
        }

        """,
    "hsc": 
        """
        """,
    "jwst": 
        """
        """,
    "legacysurvey": 
        """
        """,
    "plasticc": 
        """
        """,
    "ps1_sne_ia": 
        """
        """,
    "sdss": 
        """
        """,
    "snls": 
        """
        """,
    "ssl_legacysurvey": 
        """
        """,
    "swift_sne_ia": 
        """
        """,
    "tess": 
        """
        """,
    "vipers": 
        """
        """,
    "yse": 
        """
        """,

}
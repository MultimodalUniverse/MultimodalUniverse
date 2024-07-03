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
        """,
    "desi": 
        """
        """,
    "desi_provabgs": 
        """
        """,
    "foundation": 
        """
        """,
    "gaia": 
        """
        """,
    "gz10": 
        """
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
        """,
    "desi": 
        """
        """,
    "desi_provabgs": 
        """
        """,
    "foundation": 
        """
        """,
    "gaia": 
        """
        """,
    "gz10": 
        """
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
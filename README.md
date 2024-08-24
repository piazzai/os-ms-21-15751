# os-ms-21-15751

This repository includes data and commented R code for the analysis reported in [this paper](https://doi.org/10.1287/orsc.2021.15751). The analysis is based on the following publicly available data:

1. **Discogs.** Data dumps are available from [discogs.com](https://data.discogs.com/). This analysis used the dump from April 1, 2020. Download it and extract it to a folder called `discogs/` within your working directory.
2. **MusicBrainz.** Data dumps are available from [metabrainz.org](https://metabrainz.org/datasets). This analysis used the MusicBrainz PostgreSQL dump from April 11, 2020. Download it and extract it to a folder called `musicbrainz/`. The database schema is explained [here](https://musicbrainz.org/doc/MusicBrainz_Database/Schema).
3. **ListenBrainz.** Data dumps are available from [musicbrainz.org](https://metabrainz.org/datasets). This analysis used the ListenBrainz PostgreSQL dump from December 1, 2020. Download it and extract it to a folder called `listenbrainz/`.
4. **Additional data files.** These are distributed as part of this repository, in the `extra.tar.xz` tarball. The file is hosted on [Git LFS](https://git-lfs.com/) and can be downloaded from there, but in case that does not work it is also available from Dropbox through [this link](https://www.dropbox.com/scl/fi/i2moufh29da2s0nghytow/extra.tar.xz?rlkey=62gv9ml6mm5h55n5lja501znm&st=t7gctndl&dl=1). Download it either way and extract it to a folder called `extra/`. The file `abstamps.csv` in this tarball contains timestamps for AcousticBrainz submissions kindly provided by AcousticBrainz developers. The file `checktracks.csv` includes details and links for tracks we manually added to AcousticBrainz as part of our analysis. These tracks are now permanently part of the AcousticBrainz database.

After all data has been downloaded, update lines 36–39 of `scripts/preparation.R` with paths to the new folders. Running the code provided in this script will replicate our data preparation. This includes scraping operations (using RSelenium) that require a Discogs account and take many days to complete. To reproduce the scraping, please provide your own Discogs username and password in line 44 of the script.

The data obtained through the preparation script is saved to an RData object called `checkpoint.RData`. This object is loaded at the beginning of `scripts/analysis.R`. The code provided here replicates all results reported in the paper, including descriptive statistics, regression estimates, and simulations. For convenience, `checkpoint.RData` is distributed as part of this repository.

The data used for regressions is separately distributed in CSV format and can be found in the `datasets/` folder. There are two files: `styles.csv` contains the data used for our main analysis, which is based on Discogs styles; `genres.csv` contains the data used for our replication of the main analysis at level of Discogs genres (see the paper's [online appendix](https://pubsonline.informs.org/doi/suppl/10.1287/orsc.2021.15751)).

## Citation

Piazzai, Michele, Min Liu, and Martina Montauti (2024). Cognitive economy and product categorization. _Organization Science_, in press.  
<https://doi.org/10.1287/orsc.2021.15751>

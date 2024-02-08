# os-ms-21-15751

This repository includes data and R code for the analysis reported in [this paper](). The analysis was based on the following public databases:

1. **Discogs.** Data dumps are available from [discogs.com](https://data.discogs.com/). This analysis used the dump from April 1, 2020. Download it and extract it to a folder called `discogs/`.
2. **MusicBrainz.** Data dumps are available from [metabrainz.org](https://metabrainz.org/datasets). This analysis used the MusicBrainz PostgreSQL dump from April 11, 2020. Download it and extract it to a folder called `musicbrainz/`. The database schema is explained [here](https://musicbrainz.org/doc/MusicBrainz_Database/Schema).
3. **ListenBrainz.** Data dumps are available from [musicbrainz.org](https://metabrainz.org/datasets). This analysis used the ListenBrainz PostgreSQL dump from December 1, 2020. Download it and extract it to a folder called `listenbrainz/`.
4. **Additional data files.** These are distributed as part of this repository, in the `extra.tar.xz` tarball. The file is hosted on [Git LFS](https://git-lfs.com/). Download it and extract it to a folder called `extra/`. The file `abstamps.csv` in this tarball contains timestamps for AcousticBrainz submissions kindly provided by AcousticBrainz developers. The file `checktracks.csv` includes details and links for tracks we manually added to AcousticBrainz as part of our analysis. These tracks are now permanently part of the AcousticBrainz database.

After all data has been downloaded, update the paths in lines 36–39 of `scripts/preparation.R`. Running this code will replicate our data preparation. This includes scraping operations (using RSelenium) that require a Discogs account and take many days to complete. To rerun these operations, provide your own Discogs username and password in line 44 of the script.

The data obtained through the preparation script is saved to an RData object called `checkpoint.RData`. This object is loaded at the beginning of `scripts/analysis.R`, which is the code that replicates our results, including descriptive statistics and regression estimates. For convenience, `checkpoint.RData` is distributed as part of this repository.

The data used for regressions is separately provided in CSV format. There are two CSV files in the `datasets/` folder: `styles.csv` is the data used for our main analysis, based on Discogs styles; `genres.csv` is the data used for our replication of the main analysis at level of Discogs genres (see [Appendix E]() of the paper).

## Citation

Piazzai, Michele, Min Liu, and Martina Montauti (2024). Cognitive economy and product categorization. _Organization Science_, in press.  
<https://>

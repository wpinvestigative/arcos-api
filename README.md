# ARCOS API

These are the files that help run the [ARCOS API](https://arcos-api.ext.nile.works/__swagger__/).

The Washington Post [published](https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources) a significant portion of a database that tracks the path of every opioid pain pill, from manufacturer to pharmacy, in the United States between 2006 and 2014. We have reported a [number of stories](https://www.washingtonpost.com/national/2019/07/20/opioid-files/) using this data set, but we believe there are more stories to be told.

We are making this data accessible [as an API](https://arcos-api.ext.nile.works/__swagger__/) for everyone to download and [use in their reporting](https://www.washingtonpost.com/national/2019/08/12/post-released-deas-data-pain-pills-heres-what-local-journalists-are-using-it/) or research to promote a deeper understanding of the regional and local effects of the opioid crisis.

If you see an error in our summary or supplemental files, please submit a pull request.

**Do you use R?** Try out the [arcos api wrapper](https://github.com/wpinvestigative/arcos).

**Do you use [Docker](https://docs.docker.com/desktop/)?** Run the application yourself on your local machine with the command: `docker-compose up`. 

**Note: if you make alterations to the plumber.R file, you'll need to regenerate `./docs/openapi.json`. In order to host the API at `/arcos/`, this repo alters some plumber defaults, the location of its `openapi.json` among them.** 

|  **Data Files** | **What** |
| --- | --- |
|  data_dictionary.csv | Descriptions of every field in the ARCOS raw data |
|  buyer_annual14.csv |  |
|  buyer_list14.csv |  |
|  buyer_monthly2006.csv |  |
|  buyer_monthly2007.csv |  |
|  buyer_monthly2008.csv |  |
|  buyer_monthly2009.csv |  |
|  buyer_monthly2010.csv |  |
|  buyer_monthly2011.csv |  |
|  buyer_monthly2012.csv |  |
|  buyer_monthly2013.csv |  |
|  buyer_monthly2014.csv |  |
|  county_annual14.csv |  |
|  county_fips.csv |  |
|  county_fips.RDS |  |
|  county_monthly14.csv |  |
|  data_dictionary.csv |  |
|  detail_list_buyers14.csv |  |
|  detail_list_reporters14.csv |  |
|  not_pharms.csv |  |
|  pharmacies_cbsa14.csv |  |
|  pharmacies_counties14.csv |  |
|  pharmacies_dea_nos14.csv |  |
|  pharmacies_latlon14.csv |  |
|  pharmacies_tracts14.csv |  |
|  pop_counties_20062014.csv |  |
|  pop_states_20062014.csv |  |
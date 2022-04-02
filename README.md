
# <img src="Images/logo.png" width="80"/> Data Lake for the Platform for Analytical Models in Epidemiology (PAMEpi)

[![DOI](https://zenodo.org/badge/396775199.svg)](https://zenodo.org/badge/latestdoi/396775199)

## Table of contents
* [General info](#general-info)
* [Directory structure](#directory-structure)
* [Installation](#installation)
* [Dependencies](#dependencies)
* [Licence](#licence)
* [Citing the directory](#citing-directory)
* [Support](#support)
* [References and links](#references)

## General info

The results of the process developed here are fundamental resources that enable individualised (unidentified) or aggregated data analysis, statistical, mathematical and computational modelling, data visualisation and scientific communication about the infectious diseases of interest. Currently we are focusing on studies related to the COVID-19 pandemic in Brazil.

## Directory structure

The directory has four main folders: Data collection, Data curation, Data description and Data ETL, representing the process applied to the datasets used. Finally, we specify the disease we are working on inside each folder. 

* **Data collection:** contains the scripts for downloading data from open sources and updating when new versions are available in their original system (source).

	* All data collection plans are described in [[1]](#1). 

	* The data collection plan and the list of data (along with their metadata and descriptions) for the Covid-19 pandemic can be found in [[2]](#2). Scripts to download the data sets are https://github.com/PAMepi/PAMepi_scripts_datalake/tree/main/Data collection, like a scriptMobility_Wcota.py, scriptSrag.py, scriptSf.py and scriptVaccination.py.

* **Data curation:** contains the scripts for data harmonisation and cleansing for each data set. The scripts may change over time due to changes detected after the record update. 
	* Data curation scripts for the Covid-19 datasets:(Se quiser pode colocar lista de scripts para as bases da covid) 

## Installation

Currently the library is on production, so the easiest way to use is clone our repository or copy the functions available in this directory. 

## Dependencies

Models were implemented using Python > 3.5 and depend on libraries such as [Pandas](https://github.com/pandas-dev/pandas), [SciPy](https://github.com/scipy/scipy), [Numpy](https://github.com/numpy/numpy), [Matplotlib](https://github.com/matplotlib/matplotlib), etc. For the full list of dependencies as well libraries versions check requirements.txt.
 
## License

[MIT License](LICENSE.txt)

## Citing the directory

Platform For Analytical Modelis in Epidemiology. (2022). GitHub directory: https://github.com/PAMepi/PAMepi_scripts_datalake.git. PAMepi/PAMepi_scripts_datalake: v1.0.0 (v1.0.0). Zenodo. https://doi.org/10.5281/zenodo.6384641

## Support

This study was financed by Bill and Melinda Gates Foundation and Minderoo Foundation HDR UK, through the Grand Challenges ICODA COVID-19 Data Science, with reference number 2021.0097 and the Fiocruz Innovation Promotion Program - Innovative ideas and products - COVID-19, orders and strategies INOVA-FIOCRUZ, with reference Number VPPIS-005-FIO-20-2-40.

## References 

<a id="1">[1]</a>  Platform for Analytical Models in Epidemiology - [PAMEpi](https://pamepi.rondonia.fiocruz.br/en/index_en.html) (2020).

<a id="2">[2]</a> Platform for Analytical Models in Epidemiology - [PAMEpi-Covid-19: Data](https://pamepi.rondonia.fiocruz.br/en/data_en.html) (2020).

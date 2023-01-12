
# <img src="Images/logo.png" width="80"/> Data Lake for the Platform for Analytical Models in Epidemiology (PAMEpi)

[![DOI](https://zenodo.org/badge/396775199.svg)](https://zenodo.org/badge/latestdoi/396775199)

## Table of contents
* [General info](#general-info)
* [Installation](#installation)
* [Dependencies](#dependencies)
* [Licence](#licence)
* [Citing the directory](#citing-directory)
* [Support](#support)
* [References and links](#references)

## General info

Each folder in this directory contains all the descriptions for building a data lake that can enable studies on a specific infectious disease. Each folder has four subfolders: Data Collection, Data Curation, Data Description, and Data ETL.


* **Data Collection:** contains the scripts for downloading data from open sources and updating when new versions are available in their original system (source).

* **Data Curation:** contains the scripts for data harmonisation and cleansing for each data set. The scripts may change over time due to changes detected after the record update. 

* **Data Description:** we provide codes to perform basic data analysis and data validation. 

* **Data ETL:** we provide codes to format data for modelling and  visualization. 

## Installation

Currently the library is on production, so the easiest way to use is clone our repository or copy the functions available in this directory. 

## Dependencies

Models were implemented using Python > 3.5 and depend on libraries such as [Pandas](https://github.com/pandas-dev/pandas), [SciPy](https://github.com/scipy/scipy), [Numpy](https://github.com/numpy/numpy), [Matplotlib](https://github.com/matplotlib/matplotlib), etc. For the full list of dependencies as well libraries versions check requirements.txt inside each folder.
 
## License

[MIT License](LICENSE.txt)

## Citing the directory

Platform For Analytical Modelis in Epidemiology. (2022). GitHub directory: https://github.com/PAMepi/PAMepi_scripts_datalake.git. PAMepi/PAMepi_scripts_datalake: v1.0.0 (v1.0.0). Zenodo. https://doi.org/10.5281/zenodo.6384641

## Support

This study was financed by Bill and Melinda Gates Foundation and Minderoo Foundation HDR UK, through the Grand Challenges ICODA COVID-19 Data Science, with reference number 2021.0097 and the Fiocruz Innovation Promotion Program - Innovative ideas and products - COVID-19, orders and strategies INOVA-FIOCRUZ, with reference Number VPPIS-005-FIO-20-2-40.

## References 

<a id="1">[1]</a>  Platform for Analytical Models in Epidemiology - [PAMEpi](https://pamepi.rondonia.fiocruz.br/en/index_en.html) (2020).

<a id="2">[2]</a> Platform for Analytical Models in Epidemiology - [PAMEpi-Covid-19: Data](https://pamepi.rondonia.fiocruz.br/en/data_en.html) (2020).

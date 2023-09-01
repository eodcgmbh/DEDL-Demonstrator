# DestinE Data Lake (DEDL) Use Case Demonstration

The Pakistan Flood and the Drought event in Italy in 2022 have been selected as use cases to demonstrated the user experience and capabilities offered by DestinE Data Lake Stack service. Three Jupyter notebooks have been jointly developed by EODC GmbH and TU Wien with the objective to show case the envisioned use of Stack services available in DEDL from August 2023 onwards.

## Objective of the Demonstration
The objective of the demonstration is to showcase the capabilities of the Stack Service Dask to run processing close to the data. This is important, since data within the DEDL will be distributed across multiple locations and transferring data is not an option due to the large data volume.

### Implementation
The given use cases are implemented in an early stage to the DEDL deployment (increment 0). In this phase, a Stack Service availability is not foreseen. However, for demonstration purposes, EODC deployed a PoC on the EODC cloud environment, representing the final DEDL Stack service Dask. Accordingly, the service was deployed within two separated and independent OpenStack cloud environments to simulate the final DEDL IT infrastructure setup. The requirement deployment configuration consists of the following components:
- Kubernetes
- MinIO
- Cert-Manager
- Traefik
- Dask Gateway

Data is hosted within each environment via an object storage service, [MinIO](https://min.io/), to make use of the S3 API data access protocol. Further deployment details can be found in each notebook or use case separately.

## Pakistan Flood 2022

> From 14 June to October 2022,[citation needed] floods in Pakistan killed 1,739 people, and caused ₨ 3.2 trillion ($14.9 billion) of damage and ₨ 3.3 trillion ($15.2 billion) of economic losses. The immediate causes of the floods were heavier than usual monsoon rains and melting glaciers that followed a severe heat wave, both of which are linked to climate change. On 25 August, Pakistan declared a state of emergency because of the flooding. The flooding was the world's deadliest flood since the 2020 South Asian floods and described as the worst in the country's history.[8] It was also recorded as one of the costliest natural disasters in world history.
[Wikipedia Article reference](https://en.wikipedia.org/wiki/2022_Pakistan_floods)

**Notebook: 01-pakistan-flood-2022.ipynb**

### Data
In this use case, data from the EU-funded Copernicus Emergency Management Service ([CEMS](https://extwiki.eodc.eu/en/GFM)), the European Centre for Medium-Range Weather Forecasts ([ECMWF](https://www.ecmwf.int/en/about)) and the Global Human Settlement ([GHS](https://ghsl.jrc.ec.europa.eu/index.php)) layer is used.


- The CEMS catalogue is used to obtain **Global Flood Monitoring (GFM)** data. The GFM data product is a worldwide flood monitoring system which consists of microwave imaging data and is acquired by EU’s Copernicus Sentinel-1 satellites. Microwave imaging is a technique that is relatively unaffected by cloud cover and is especially sensitive to variations in near surface water content. This type of data has been successively employed to monitor flooding. The binary data consists of daily measurement with a 20m spatial resolution which indicate whether a pixel has been flooded or not.
- The ECMWF catalogue is used to obtain the multi-decadal **ERA5-Land** dataset. This product is a reanalysis dataset that gives insight in the seasonal variation of land variables by averaging data over multiple decades with a 9 km spatial resolution. The ERA5-Land data is retrieved to estimate the risk of further flooding for next several days by aggregating precipitation on a 20m spatial grid. This dataset is for demonstration purposes only, and will be replaced with a high resolution rainfall prediction dataset in the future.
- The **Global Human Settlement (GHS) layer** is used to make a risk assessment for flooding of regional settlements. THE GHS-BUILT-S data provides a yearly measure of built-up surfaces on a 10m spatial grid. The product is a composite dataset consisting of Landsat (MSS, TM, ETM sensor) and Sentinel-2 imaging and represents the square meters of built-up surface in a cell.

The data used in the use cases was pre-processing with the scripts included in [usecase data_preparation folder](usecase/data_preparation/src/uc-flood). This was necessary in order to mimic the envisioned data access methods offered by the DEDL.

## Italy Drought 2022

> More than 100 towns in the Po valley have been asked to ration water amid the worst drought to affect Italy’s longest river in 70 years. Northern Italy has been deprived of significant rainfall for months, with the effects of drought along the 400-mile (650km) Po River, which stretches from the Alps in the north-west and flows through the Po delta before spilling out into the Adriatic, becoming visible early in the year.
[The Guardian](https://www.theguardian.com/world/2022/jun/15/italy-drought-po-valley-ration-water)

**Notebook: 02-italy-drought-2022-prediction-ascat.ipynb**
**Notebook: 03-italy-drought-2022-prediction.ipynb**

### Data
In this use case,  data is retrieved from the European Organisation for the Exploitation of Meteorological Satellites (EUMETSAT) Data Store, the Copernicus Land Monitoring service (CLMS), and the European Centre for Medium-Range Weather Forecasts (ECMWF).

- The EUMETSAT Data Store enables access to **soil moisture data** as part of the Operational **Hydrology and water management (HSAF)** unit. The data consists of microwave images obtained by the the Advanced Scatterometer (ASCAT) satellite. Microwave imaging is a technique that is relatively unaffected by cloud cover and is especially sensitive to variations in near surface water content (0 - 2 cm). This dataset is used to evaluate drought severeness at the beginning of March 2022. *Note: exclusively used in 02-italy-drought-2022-prediction-ascat.ipynb*
- The **4DMED** Project aims to develop high resolution daily Earth Observations data sets targeting multiple aspects of the Mediterranean terrestrial water cycle. For this case study we will use the EU’s Copernicus Sentinel-1 satellite microwave imaging data (ESA Digital Twin Earth Hydrology precursor activity). Microwave imaging is a technique that is relatively unaffected by cloud cover and is especially sensitive to variations in near surface water content. This dataset is used to evaluate drought severeness at the end of March 2022. *Note: exclusively used in 03-italy-drought-2022-prediction.ipynb*
- The CLMS is queried for the **CORINE ('Coordination of information on the environment') Land Cover inventory**. The original purpose of this data set serves environmental policy development and consists of 44 classes of land cover in five main units (artificial, agriculture, forests and semi-natural, wetlands, and water). It combines a number of sources, among which Landsat-5, Landsat-7, and ESA Sentinel-2 satellite imaging data in the most recent years. This dataset will highlight which sectors will be most likely drought impacted under the predicted scenario. 
- The ECMWF catalogue is used to obtain the multi-decadal **ERA5-Land dataset**. This product is a reanalysis dataset that gives insight in the seasonal variation of land variables by averaging data over multiple decades with a 9 km spatial resolution. The ERA5-Land data is retrieved to estimate the development of drought over the months following the 30th of March 2022. This dataset is for demonstration purposes only, and will be replaced with a high resolution soil moisture prediction dataset in the future.

The data used in the use cases was pre-processing with the scripts included in [usecase data_preparation folder](usecase/data_preparation/src/uc-drought). This was necessary in order to mimic the envisioned data access methods offered by the DEDL.

## Development
This project is setup to make use of devcontainers in VSCode. The container image is defined in [Docker/Dockerfile.DevContainer]()

### Artefacts
The Container image [Docker/Dockerfile.Dask]() is used to by Dask Gateway and acts as the main image to be used for creating a Dask Cluster (scheduler and worker nodes).
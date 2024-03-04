This project aims to analyse currency exchange data sourced from National Bank of Poland API. 

Data pipeline is defined in the de-project-pipeline.py file and results are presented in the de-project-report.ipynb notebook. Data discovery platform, Datahub, has been set up and screenshots of it can be found in the datahub-screenshots folder. The pipeline requires setting up a MinIO server, which is done using de-project-compose.yaml. This compose file automatically creates a bucket in which the data will be stored.

This project has only been tested on Linux, as I've encountered problems installing the prefect package on Windows.
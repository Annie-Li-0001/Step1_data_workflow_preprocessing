
# Python Private Quantitative Investment Data Synchronization Task

![Python](https://img.shields.io/badge/python-3.8-blue)  
![Airflow](https://img.shields.io/badge/airflow-2.0.2-orange)  
![Docker](https://img.shields.io/badge/docker-20.10.8-green)  
![Tushare](https://img.shields.io/badge/tushare-1.2.64-red)  

This project is an open-source project for Python quantitative investment data synchronization tasks. By utilizing Airflow and Docker technologies, it can conveniently synchronize daily stock, fund, index, financial data, and indicator data. The data source used is Tushare.

## Features

- **Supports synchronization of various data types**, including stock, fund, index, financial data, and indicator data.
- **Uses Airflow as the task scheduling tool**, allowing flexible setting of task execution time and order.
- **Employs Docker containerization technology** for easy deployment and operation, reducing the hassle of dependencies and environment configuration.
- **The data source uses Tushare's open data interface**, ensuring the reliability and accuracy of the data.
- **Highly customizable**, allowing users to add new data synchronization tasks or modify existing tasks according to their needs.

## Environment Requirements

- Python 3.8
- Airflow 2.0.2
- Docker 20.10.8
- Tushare 1.2.64
```

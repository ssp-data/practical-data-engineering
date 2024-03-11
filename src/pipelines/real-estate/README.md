<p align="left">
<a href="https://www.ssp.sh/" target="_blank"><img src="https://ssp.sh/images/sspaeti_quadrat.png" height="100"/></a>
</p>

# Practical Data Engineering: A Hands-On Real-Estate Project Guide

[![Open Source Logos](https://www.ssp.sh/blog/data-engineering-project-in-twenty-minutes/images/open-source-logos.png)](https://www.ssp.sh/blog/data-engineering-project-in-twenty-minutes/)

This repository containts a practical implementation of a data engineering project that spans across web-scraping real-estates, processing with Spark and Delta Lake, adding data science with Jupyter Notebooks, ingesting data into Apache Druid, visualizing with Apache Superset, and managing workflows with Dagster—all orchestrated on Kubernetes. 

**Built your own DE project or forked mine? Let me know in the comments; I'd be curious to know more about.**

## 🌟 About This Project

This Practical Data Engineering project addresses common data engineering challenges while exploring innovative technologies. It should serve as a learning project but incorporate comprehensive real-world use cases. It's a guide to building a data application that collects real-estate data, enriches it with various metrics, and offers insights through machine learning and data visualization. This application helps you find your dream properties in your area and showcases how to handle a full-fledged data engineering pipeline using modern tools and frameworks.

### Why this project?
- **Real-World Application**: Tackling a genuine problem with real estate data to find the best properties.
- **Comprehensive Tech Stack**: Utilizes a wide range of technologies from web scraping, S3 storage, data processing, machine learning, to data visualization and orchestration.
- **Hands-On Learning**: Offers a hands-on approach to understanding how different technologies integrate and complement each other in a real-world scenario.

### Key Features & Learnings:
- Scraping real estate listings with [Beautiful Soup](https://beautiful-soup-4.readthedocs.io/en/latest/index.html)
- Change Data Capture (CDC) mechanisms for efficient data updates
- Utilizing [MinIO](https://github.com/minio/minio) as an S3-Gateway for cloud-agnostic storage
- Implementing UPSERTs and ACID transactions with [Delta Lake](https://delta.io/) 
- Integrating [Jupyter Notebooks](https://github.com/jupyter/notebook) for data science tasks
- Visualizing data with [Apache Superset](https://github.com/apache/superset)
- Orchestrating workflows with [Dagster](https://github.com/dagster-io/dagster/)
- Deploying on [Kubernetes](https://github.com/kubernetes/kubernetes) for scalability and cloud-agnostic architecture

### Technologies, Tools, and Frameworks:
This project leverages a vast array of open-source technologies including MinIO, Spark, Delta Lake, Jupyter Notebooks, Apache Druid, Apache Superset, and Dagster—all running on Kubernetes to ensure scalability and cloud-agnostic deployment.

<p align="center">
<img src="https://www.ssp.sh/blog/data-engineering-project-in-twenty-minutes/images/lakehouse-open-sourced.png" height="500">
</p>

### 🔄 Project Evolution and Updates

This project started in November 2020 as a project for me to learn and teach about data engineering. I published the entire project in March 2021 (see the initial version on [branch `v1`](https://github.com/sspaeti-com/practical-data-engineering/tree/v1)). Three years later, it's interesting that the tools used in this project are still used today. We always say how fast the Modern Data Stack changes, but if you choose wisely, you see that good tools will stay the time. Today, in `March 2024`, I updated the project to the latest Dagster and representative tools versions. I kept most technologies, except Apache Spark. It was a nightmare to setup locally and to work with Delta Lake SQL APi. I replaced it with [delta-rs](https://github.com/delta-io/delta-rs) direct, which is implemented in Rust and can edit and write Delta Tables directly in Python. 

Next, I might add Rill Developer to the mix to have some fun analyzing the data powered by DuckDB. For a more production-ready dashboard, Superset would still be my choice tough. 


## 🛠 Installation & Usage
Please refer to individual component directories for detailed setup and usage instructions. The project is designed to run both locally and on cloud environments, offering flexibility in deployment and testing.

### Prerequisites:
- Python and pip for installing dependencies
- MinIO running for cloud-agnostic S3 storage
- Docker Desktop & Kubernetes for running Jupyter Notebooks
- Basic understanding of Python and SQL for effective navigation and customization of the project

### Quick Start:
1. Clone this repository.
2. Install dependencies
3. Install and start MinIO
4. Explore the data with the provided Jupyter Notebooks and Superset dashboards.

```sh
#change to the pipeline directory
cd src/pipelines/real-estate

# installation
pip install -e ".[dev]"

# run minio
minio server /tmp/minio/

# startup dagster
dagster dev
```


## 📈 Visualizing the Pipeline

![Dagster UI – Practical Data Engineering Pipeline](https://www.ssp.sh/blog/data-engineering-project-in-twenty-minutes/images/Dagster-Practical-Data-Engineering-Pipeline.png)

## 📚 Resources & Further Reading
- [Building a Data Engineering Project in 20 Minutes](https://www.ssp.sh/blog/data-engineering-project-in-twenty-minutes/): Access the full blog post detailing the project's development, challenges, and solutions.
- [DevOps Repositories](https://github.com/sspaeti-com/data-engineering-devops): Explore the setup for Druid, MinIO and other components.
- [Business Intelligence Meets Data Engineering with Emerging Technologies](https://www.ssp.sh/blog/business-intelligence-meets-data-engineering/): An earlier post that dives into some of the technologies used in this project.

## 📣 Feedback
Your feedback is invaluable to improve this project. If you've built your project based on this repository or have suggestions, please let me know through creating an Issues or a Pull Request directly.

---

*This project is part of my journey in exploring data engineering challenges and solutions. It's an open invitation for everyone interested in data engineering to learn, contribute, and share your experiences.*

*Below some impressions of the jupyter notebook used in this project.*


<p align="center">
<img src="https://sspaeti.com/blog/the-location-independent-lifestyle/europe/sspaeti_com_todays_office_033.jpg" width="600">

</p>



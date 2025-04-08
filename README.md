<!-- Top Anchor -->
<a id="readme-top"></a>

<!-- Title -->
<h3 align="center">British Columbia Car Crash Predictor</h3>

<p align="center">
  In this project, we analyze car crash incidents in British Columbia, Canada, and predict real-time hotspots for car crashes.
  <br />
  <a href="https://github.sfu.ca/gma89/van-crash-predictor"><strong>Explore the code »</strong></a>
  <br />
  <br />
  Contributors: 
  <a href="https://github.sfu.ca/gma89">Gloria Mo</a> · 
  <a href="https://github.sfu.ca/rla187">Rojin Lohrasbinejad</a> · 
  <a href="https://github.sfu.ca/sha392">Swaifa Haque</a>
</p>

---

<!-- Table of Contents -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#project-pipeline">Project Pipeline</a></li>
      </ul>         
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li><a href="#data-engineering">Data Engineering</a>
      <ul>
        <li><a href="#extracting-the-data">Extracting the Data</a></li>
        <li><a href="#transforming-the-data">Transforming the Data</a></li>
        <li><a href="#loading-the-data">Loading the Data</a></li>
      </ul>
    </li>
    <li><a href="#machine-learning">Machine Learning</a>
      <ul>
        <li><a href="#machine-learning-model">Machine Learning Model</a></li>
        <li><a href="#training-data">Training Data</a></li>
        <li><a href="#model-prediction">Model Prediction</a></li>
      </ul>
    </li>    
    <li><a href="#visualization">Visualization</a>
      <ul>
        <li><a href="#tableau-dashboard">Tableau Dashboard</a></li>
        <li><a href="#others">Others</a></li>
      </ul>
    </li>    
    <li><a href="#usage">Usage</a></li>
    <li><a href="#results">Results</a></li>
  </ol>
</details>

---

## About The Project

This project analyzes historical car crash data in British Columbia to predict high-risk locations for future incidents. The system is built using Python, Apache Spark, Apache Kafka, and PySpark.ML for data processing and machine learning. The results are visualized using Seaborn, Folium, and a dashboard in Tableau for intuitive insight presentation.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Project Pipeline: 
![BC Crash Prediction Pipeline](https://github.sfu.ca/gma89/van-crash-predictor/raw/staging/bc_crash_prediction_pipeline.png)
<sub>Pipeline created using the "diagrams" library in Python!</sub>

### Built With

* ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
* ![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)
* ![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
* ![PySpark](https://img.shields.io/badge/PySpark-3F4F8F?style=for-the-badge)
* ![Seaborn](https://img.shields.io/badge/Seaborn-0C4C8A?style=for-the-badge)
* ![Folium](https://img.shields.io/badge/Folium-43B02A?style=for-the-badge)
* ![Tableau](https://img.shields.io/badge/Tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Data Engineering

In the Data Engineering process, we focus on extracting, transforming, and loading (ETL) data from various sources to prepare it for machine learning and analysis. The data engineering pipeline is broken down into multiple stages and includes files for geocoding, cleaning, merging, and producing data for the machine learning models.

### Extracting the Data

The key datasets used in this project are as follows:
- **TAS Data** (Traffic Accident System Data): Contains details about where and how accidents happened in British Columbia (BC).
- **ICBC Data** (Insurance Corporation of British Columbia): Contains historical accident data that includes attributes such as location, weather, time, and crash severity.
- **Azure Maps Current Traffic Flow API**: Contains live traffic flow data of British Columbia roads and streets.
- **Open Weather API**: Contains live weather reports of British Columbia, including temperature, visibility, and description.

### Transforming the Data

--TO DO--

### Loading the Data

-- TO DO-- 

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Machine Learning

--TO DO--

### Machine Learning Model
### Training Data
### Model Prediction

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Visualization

### Tableau Dashboard
The dashboard features an interactive map displaying all accident locations from 2019 to 2023. In addition, various charts provide insights into the causes of accidents and other contributing factors, helping to analyze patterns and trends in road safety.

### Others

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Usage
The crash prediction model aims to enhance road safety in British Columbia by predicting potential crash hotspots. This can help alert drivers and authorities to high-risk areas, contributing to safer roads for both people and road users.
<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Results

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

### What to look for: 
TO DO?
- **Crash Hotspot Prediction Accuracy**: Check how well the model predicts known high-risk intersections in the city using historical data.
- **Model Metrics**: Review precision and recall scores in `notebooks/model_evaluation.ipynb`.
- **Streaming Pipeline**: Validate real-time data ingestion through Apache Kafka by monitoring console logs or using Kafka UI tools (e.g., Confluent Control Center if set up).
- **Visualizations**: Explore the interactive Tableau dashboard, which displays an accident heatmap and crash trends using time and weather filters.

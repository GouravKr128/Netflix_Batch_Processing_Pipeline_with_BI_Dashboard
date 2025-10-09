## **Netflix Content Analytics - E2E Data Engineering Project**
<br>

### **Overview**
- This project is an end-to-end data engineering solution built using the Netflix Movies & TV Shows dataset from Kaggle. It follows the Medallion Architecture (Bronze → Silver → Gold) to build a modern data pipeline for ingestion, transformation, modeling, and visualization.
- Managed data governance and access using Unity Catalog.

### **Tech Stack**
Azure Data Factory, Azure Data Lake Storage Gen2, Databricks, PySpark, Delta Lake, Power BI

### **Description**

1. **Data Ingestion (Bronze Layer)**
   - Ingested raw data from GitHub repository to Azure Data Lake Storage Gen2 using Azure Data Factory.

2. **Data Transformation (Silver Layer)**
   - Processed the ingested raw data in Databricks using PySpark.
   - Applied data cleaning and transformation steps including: Null value handling, Duplicate removal, Type conversions, Standardization, Splitting of multi-value columns etc.
   - Wrote the transformed data to Silver layer in Delta file format.

3. **Data Modelling (Gold Layer)**
   - Orchestrated Databricks notebooks to build an ETL workflow that reads initial and incremental cleaned data from silver layer and models it into a star schema
   - Implemented an alert activity in the workflow to monitor for new content types beyond “Movie” and “TV Show”.

![Workflow](https://github.com/GouravKr128/E2E-Netflix_data_Stream_Processing_and_Analytics--InProg./blob/9fcf21bceff88f209aac902c13c23f8229e133b6/Screenshots/Screenshot_2.png)

![Star Schema](https://github.com/GouravKr128/E2E-Netflix_data_Stream_Processing_and_Analytics--InProg./blob/9fcf21bceff88f209aac902c13c23f8229e133b6/Screenshots/Screenshot_4.png)
<br>

4. **Data Visualization (Power BI)**
   - Built an interactive dashboard to provide actionable insights on Netflix content trends.

![Dashboard](https://github.com/GouravKr128/E2E-Netflix_data_Stream_Processing_and_Analytics--InProg./blob/9fcf21bceff88f209aac902c13c23f8229e133b6/Screenshots/Screenshot_5.png)
<br>
![Page2](https://github.com/GouravKr128/E2E-Netflix_data_Stream_Processing_and_Analytics--InProg./blob/9fcf21bceff88f209aac902c13c23f8229e133b6/Screenshots/Screenshot_6.png)
<br>

### **Outcome**
   - Successfully built an automated, scalable data pipeline using modern data engineering tools.
   - Designed a star schema model in the Gold layer with fact and dimension Delta tables.
   - Implemented alerting mechanism to detect new content types beyond TV Shows and Movies.
   - Delivered an interactive Power BI dashboard providing insights into content distribution, top countries, ratings, and yearly growth.

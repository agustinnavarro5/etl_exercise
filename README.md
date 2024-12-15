# **E-commerce Transaction Data Pipeline**

## **Overview**

This project involves building a data pipeline to process raw e-commerce transaction data. The pipeline performs data extraction, cleaning, transformation, and analysis to generate insights from the transaction data. The goal is to process the data, clean it, transform it into a useful format, and store the results in a PostgreSQL database for further reporting and analysis.

---

## **Project Requirements**

You are provided with a dataset containing raw e-commerce transaction data. The tasks you need to complete include data cleaning, transformation, analysis, and saving the results to a PostgreSQL database.

### **Tasks:**

1. **Extract**: Extract raw transaction data from a source.
2. **Cleansing**: Clean the raw data by handling missing values, duplicates, and converting data types.
3. **Transformation**: Create new columns (e.g., `total_sales`) and group the data to calculate aggregated values.
4. **Data Analysis**:
    - **Top Categories**: Identify the top 3 item categories based on total sales.
    - **Customer Segmentation**: Segment users into groups based on their total spending: low, medium, and high spenders.
    - **Retention Analysis**: Perform a cohort analysis to measure user retention over time.
5. **Save to Database**: Save the cleaned and transformed data into a PostgreSQL database.
6. **Schedule**: Schedule the process to run monthly and capture new data.
## **Docker Setup**

This project uses **Docker Compose** to manage the containers for both **Airflow** and **PostgreSQL**. The following services are defined:

- **Airflow**: Manages task orchestration and scheduling.
- **PostgreSQL**: A database to store the transaction data and aggregated results.

### **1. Docker Compose Configuration**

The `docker-compose.yml` file is configured to set up the necessary services. It includes the setup for Airflow, PostgreSQL, and their dependencies.

Here is an example `docker-compose.yml`:

```yaml
version: '3.7'
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: ecommerce
    ports:
      - "5432:5432"
  
  airflow:
    image: apache/airflow:2.7.1
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/ecommerce
      AIRFLOW__CORE__LOAD_EXAMPLES: 'False'
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./scripts:/opt/airflow/scripts
    depends_on:
      - postgres
```

2. **Build and Start the Docker Containers**:

To set up the environment, use Docker Compose to build and run the services:

Navigate to the project directory where docker-compose.yml is located.
Run the following command to start the containers:

```bash
docker-compose up -d
```

This will:

- Start the PostgreSQL container and set up the database.
- Start the Airflow container, which will automatically install the required dependencies and run the scheduler and webserver.


### 3. **Accessing Airflow UI**

After the containers are up and running, you can access the Airflow Web UI at http://localhost:8080.

```
Username: airflow
Password: airflow
```

Here, you can trigger your DAG and monitor the task execution.

4. Create PostgreSQL connection in Airflow UI

To create a connection in airflow UI you must to follow the next steps:
- Go to Admin -> Connections option
- Add new record:
    - Put the values of the postgresql connection like:
    - connection ID: postgres_conn
    - type: PostgreSQL
    - host: localhost
    - database: airflow
    - login: airflow
    - password: airflow
    - port 5432

5. Trigger a DAGRun execution for the entire pipeline and next check into the docker postgresql database the transactions table created and inserted with the next command:

```bash
docker exec -it etl_exercise-postgres-1 psql -U airflow
```

And run:
```SQL
SELECT * FROM transactions;
```

6. You can check the reports PNG generated in the folder 'dags/reports'

## **Conclusion**

This project is designed to help process e-commerce transaction data using a data pipeline. It involves extracting raw data, cleaning it, transforming it, performing data analysis, and saving the results to a PostgreSQL database. The entire pipeline is orchestrated with Apache Airflow, and the process is scheduled to run monthly to process new data.
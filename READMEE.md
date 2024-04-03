## Exercise: Real-time User Clickstream Data Processing and ETL with Apache Airflow

In this exercise, you will create a comprehensive workflow combining real-time user clickstream data processing with Apache Kafka and ETL (Extract, Transform, Load) tasks using Apache Airflow. You will define a Directed Acyclic Graph (DAG) to simulate extracting data, transforming it, and loading it into a destination.

### Instructions:

1. **Setup Kafka Environment**:
   - Ensure you have Kafka installed and running locally or on a server.
   - Create two Kafka topics:
     - `user_clickstream` for simulated user click events.
     - `transformed_clickstream` for transformed click event data.

2. **Define ETL DAG**:
   - Create a new Python file named `clickstream_etl_workflow.py` within your Airflow DAGs directory.
   - Define a DAG object with the following specifications:
     - DAG ID: `clickstream_etl_workflow`
     - Schedule Interval: `@daily`
     - Start Date: Today's date
     - Email Configuration: Disable email notifications
     - Set `catchup` to `False` to prevent backfilling historical runs.
   - Define four tasks within the DAG:
     - **Task 1 - Extract Data from Kafka**: 
       - Task ID: `extract_from_kafka`
       - Python Operator: Use a PythonOperator to print "Extracting data from Kafka..." when executed.
     - **Task 2 - Transform Data**: 
       - Task ID: `transform_data`
       - Python Operator: Use a PythonOperator to print "Transforming data..." when executed.
     - **Task 3 - Load Transformed Data to Kafka**: 
       - Task ID: `load_to_kafka`
       - Python Operator: Use a PythonOperator to print "Loading transformed data to Kafka..." when executed.
     - **Task 4 - Load Transformed Data to Destination**: 
       - Task ID: `load_to_destination`
       - Python Operator: Use a PythonOperator to print "Loading transformed data to destination..." when executed.
   - Define task dependencies to ensure the tasks execute in the correct order: Extract from Kafka -> Transform Data -> Load to Kafka -> Load to Destination.

3. **Check Container Status**:
   - Ensure Docker containers are running by executing the following command:
     ```
     docker ps
     ```

4. **Trigger DAG Execution**:
   - Access the Airflow Web UI at `http://localhost:8080`.
   - Navigate to the `DAGs` section and locate the `clickstream_etl_workflow` DAG.
   - Trigger the execution of the DAG manually and observe the task execution in the UI.

### Answers:

Below is a sample Python script `clickstream_etl_workflow.py` defining a comprehensive ETL workflow with Apache Airflow:

```python
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'catchup': False
}

def extract_from_kafka():
    print("Extracting data from Kafka...")

def transform_data():
    print("Transforming data...")

def load_to_kafka():
    print("Loading transformed data to Kafka...")

def load_to_destination():
    print("Loading transformed data to destination...")

with DAG('clickstream_etl_workflow',
         default_args=default_args,
         description='A comprehensive ETL workflow with Apache Airflow',
         schedule_interval='@daily') as dag:

    start_task = DummyOperator(task_id='start_task')

    extract_from_kafka_task = PythonOperator(task_id='extract_from_kafka', python_callable=extract_from_kafka)

    transform_data_task = PythonOperator(task_id='transform_data', python_callable=transform_data)

    load_to_kafka_task = PythonOperator(task_id='load_to_kafka', python_callable=load_to_kafka)

    load_to_destination_task = PythonOperator(task_id='load_to_destination', python_callable=load_to_destination)

    start_task >> extract_from_kafka_task >> transform_data_task
    transform_data_task >> [load_to_kafka_task, load_to_destination_task]
```
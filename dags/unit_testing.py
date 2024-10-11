from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import unittest
import sys
import os

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define a function to run the unit test and print the result
def run_unit_test():
    """
    Function to run the PySpark unit test using unittest.
    It will call the 'unit_test.py' script and print the results.
    """
    # Dynamically import the unit_test module
    test_dir = "/app/unit_testing"
    sys.path.insert(0, test_dir)  # Add test directory to Python path

    # Import unit_test (assuming it's named 'unit_test.py')
    try:
        import testing_spark  # This should be your test module name
        loader = unittest.TestLoader()
        suite = loader.loadTestsFromModule(testing_spark)
        
        # Run the tests and print results
        runner = unittest.TextTestRunner(stream=sys.stdout, verbosity=2)
        result = runner.run(suite)
        
        # Check if the tests passed or failed
        if not result.wasSuccessful():
            raise Exception("Unit tests failed!")
        
    except Exception as e:
        print(f"Unit test execution failed: {e}")
        raise

# Define the DAG
with DAG(
    'pyspark_unit_test_dag',
    default_args=default_args,
    description='A DAG to run PySpark unit test for the KafkaStreamingApp',
    schedule_interval=None,  # Run manually or set to 'daily' for scheduled runs
    catchup=False,
) as dag:

    # Define the PythonOperator to run the unit test
    run_tests = PythonOperator(
        task_id='run_pyspark_unit_test',
        python_callable=run_unit_test,
        dag=dag
    )

    # Set task dependencies (you can add more tasks if necessary)
    run_tests

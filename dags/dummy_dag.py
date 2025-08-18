"""
Dummy DAG Example for Apache Airflow 3.0.x
============================================

This DAG demonstrates basic Airflow 3.0.x concepts including:
- DAG definition with modern syntax
- Different types of operators
- Task dependencies
- Python functions as tasks
- Data passing between tasks (XCom)
- Error handling and retries
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'framework-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,  # Don't run for past dates
}

# Define the DAG
dag = DAG(
    'dummy_example_dag',
    default_args=default_args,
    description='A simple dummy DAG for testing Airflow 3.0.x',
    schedule_interval=timedelta(hours=1),  # Run every hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['example', 'dummy', 'test'],
    max_active_runs=1,
)

# Python function for PythonOperator
def print_hello(**context):
    """Simple Python function that logs information"""
    logging.info("Hello from Airflow 3.0.x!")
    logging.info(f"Execution date: {context['ds']}")
    logging.info(f"DAG run ID: {context['dag_run'].run_id}")
    return f"Hello executed at {datetime.now()}"

def process_data(**context):
    """Function that simulates data processing"""
    import random
    import time
    
    # Simulate some processing time
    time.sleep(2)
    
    # Generate some dummy data
    processed_count = random.randint(100, 1000)
    success_rate = round(random.uniform(0.85, 0.99), 2)
    
    result = {
        'processed_records': processed_count,
        'success_rate': success_rate,
        'timestamp': str(datetime.now())
    }
    
    logging.info(f"Processed {processed_count} records with {success_rate*100}% success rate")
    
    # Return data to be available for downstream tasks via XCom
    return result

def analyze_results(**context):
    """Function that analyzes results from previous task"""
    # Pull data from previous task using XCom
    task_instance = context['task_instance']
    results = task_instance.xcom_pull(task_ids='process_data_task')
    
    if results:
        logging.info("Analysis Results:")
        logging.info(f"Records processed: {results['processed_records']}")
        logging.info(f"Success rate: {results['success_rate']*100}%")
        
        if results['success_rate'] > 0.9:
            logging.info("✅ Processing quality is excellent!")
            return "EXCELLENT"
        elif results['success_rate'] > 0.8:
            logging.info("⚠️ Processing quality is acceptable")
            return "ACCEPTABLE"
        else:
            logging.warning("❌ Processing quality needs improvement")
            return "NEEDS_IMPROVEMENT"
    else:
        logging.error("No data received from previous task")
        return "ERROR"

# Task 1: Start dummy task
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Task 2: Simple bash command
check_system = BashOperator(
    task_id='check_system',
    bash_command="""
    echo "🚀 Starting system check..."
    echo "Current date: $(date)"
    echo "Current user: $(whoami)"
    echo "Available disk space:"
    df -h | head -5
    echo "✅ System check completed"
    """,
    dag=dag,
)

# Task 3: Python function task
hello_task = PythonOperator(
    task_id='say_hello',
    python_callable=print_hello,
    dag=dag,
)

# Task 4: Data processing simulation
process_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    dag=dag,
)

# Task 5: Analysis task that uses XCom data
analyze_task = PythonOperator(
    task_id='analyze_results',
    python_callable=analyze_results,
    dag=dag,
)

# Task 6: Conditional bash task
report_task = BashOperator(
    task_id='generate_report',
    bash_command="""
    echo "📊 Generating final report..."
    echo "DAG: {{ dag.dag_id }}"
    echo "Run ID: {{ dag_run.run_id }}"
    echo "Execution Date: {{ ds }}"
    echo "Report generated at: $(date)"
    echo "✅ Report generation completed"
    """,
    dag=dag,
)

# Task 7: Cleanup task
cleanup_task = BashOperator(
    task_id='cleanup',
    bash_command="""
    echo "🧹 Starting cleanup process..."
    echo "Cleaning temporary files..."
    # Simulate cleanup
    sleep 1
    echo "✅ Cleanup completed successfully"
    """,
    dag=dag,
)

# Task 8: End dummy task
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies using the >> operator (modern Airflow syntax)
start_task >> [check_system, hello_task] >> process_task
process_task >> analyze_task >> report_task >> cleanup_task >> end_task

# Alternative dependency syntax (both work the same):
# start_task.set_downstream([check_system, hello_task])
# [check_system, hello_task] >> process_task
# process_task >> analyze_task >> report_task >> cleanup_task >> end_task

# You can also use task groups for better organization (Airflow 3.0.x feature)
# from airflow.utils.task_group import TaskGroup
# 
# with TaskGroup("data_processing_group", dag=dag) as processing_group:
#     process_task = PythonOperator(...)
#     analyze_task = PythonOperator(...)
#     process_task >> analyze_task
import Functions as fnc
from datetime import timedelta, datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'crm-data',
    'email': 'kobipan@gmail.com',
    'email_on_failure': True,
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'wait_for_downstream': False,
    'max_active_runs': 1
}



dependencies = []
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # lst_id = fnc.set_last_id()+1
    # fnc.append_data(from_id=lst_id,limit=10)
    ith
    DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval='00 3 * * *',
        max_active_runs=1,
        template_searchpath=str(lib_path),
        catchup=False,
        tags=[tag_name, 'spark']
    ) as dag:
    dag.doc_md = (Path(__file__) / "../../README.md").resolve().read_text()
    start_process_op = DummyOperator(task_id="start_process", dag=dag)
    end_process_op = DummyOperator(task_id="end_process", dag=dag, trigger_rule="none_failed")
    data_quality_group = TaskGroup(group_id='data_quality', dag=dag)

    check_dependencies_op = DdsSensor(dependencies=dependencies,
                                      task_id="dds_check_dependencies",
                                      poke_interval=60 * 10,  # poke every 10 minutes
                                      timeout=60 * 60 * 12,  # keep trying to check for 12 hours
                                      mode='reschedule')

    check_set_list_of_month_op = PythonOperator(task_id='set_list_of_month',
                                             python_callable=fnc.set_list_of_month,
                                             op_kwargs={
                                                 'SPREADSHEET_ID': SPREADSHEET_ID,
                                                 'paramName': 'event_full_run'
                                             }
                                             )
    download_files_to_s3_op = PythonOperator(task_id='download_files_to_s3',
                                                python_callable= fnc.download_files_parallel,
                                                op_kwargs={
                                                     'list_of_month': 'list_of_month'
                                                }
                                                )

    get_num_of_pasangers_op = PythonOperator(task_id='get_num_of_pasangers',
                                             python_callable=fnc.get_num_of_pasangers,
                                             )

    start_process_op >> check_dependencies_op >> check_set_list_of_month_op >> download_files_to_s3_op >> get_num_of_pasangers_op


    # list_of_month = ['2021-07','2021-08'] # ['2021-01','2021-02','2021-03','2021-04']
    # fnc.download_files_parallel(month_to_upload = list_of_month)
    #
    # #fnc.read_data_from_s3(parquet_file_path)
    # fnc.get_num_of_pasangers()
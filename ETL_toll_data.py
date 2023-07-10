#Link to download the datafile "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
#Defining DAG arguments

default_args = {
    'owner': 'Ramya',
    'start_date': days_ago(0),
    'email': ['ramya@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1), #Daily once

)

#task to unzip data
unzip = BashOperator(
    task_id='unzip',
    bash_command='tar -zxvf tolldata.tgz',
    dag=dag,
)

#task to extract data from csv
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f 1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)

#task to extract data from tsv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f 5-7 tollplaza-data.tsv > tsv_data.csv',
    dag=dag,
)

#task to extract data from fixed width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 59-61, 63-68 payment-data.txt > fixed_width_data.csv',
    dag=dag,
)

#task to consolidate data extracted from previous tasks
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

#Transform and load the data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk "$4 = toupper($4)" extracted_data.csv > transformed_data',
    dag=dag,
)

#defining the task pipeline
unzip >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data


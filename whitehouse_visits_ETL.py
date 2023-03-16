from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import airflow.utils.dates
import redshift_connector

dag = DAG(
    dag_id = "Whitehouse_visits_ETL",
    description = "Perform ETL on whitehouse visits data and load it in cloud data warehouse",
    start_date = airflow.utils.dates.days_ago(14),
    schedule_interval= None
)

# extracting the data from the data lake
download_data = BashOperator(
    task_id = "download_whitehouse_visits_data",
    bash_command = "aws s3 cp s3://whitehouse-visits-data/whitehouse_visits.txt /home/talentum/data/",
    dag = dag
)

# creating a table in the cloud data warehouse
def _create_table():
    conn = redshift_connector.connect(
         host='examplecluster1.certselwfwzr.ap-south-1.redshift.amazonaws.com',
         database='dev',
         port=5439,
         user='awsuser',
         password='My_password123'
      )

    # Create a Cursor object
    cursor = conn.cursor()

    # setting the autocommit on
    conn.autocommit = True
    conn.run("VACUUM")

    # Query a table using the Cursor
    cursor.execute("create table visits (visitor_lname varchar,visitor_fname varchar,app_no varchar,acc_type varchar\
    ,time_of_arrival varchar,app_made_date varchar,app_start_date varchar,app_end_date varchar,\
    no_of_people varchar,visitee_lname varchar,visitee_fname varchar,location varchar,comments varchar)")

create_table = PythonOperator(
    task_id = "Create_table_in_data_warehouse",
    python_callable = _create_table,
    dag = dag
)

data = []
def _transform():
    with open('/home/talentum/data/whitehouse_visits.txt',"rt",encoding='latin-1') as file:
        for line in file.readlines():
            cells = line.split(',')
            data.append((cells[0],cells[1],cells[3],cells[5],cells[6],cells[10],\
                                cells[11],cells[12],cells[14],cells[21],cells[23],cells[24],cells[25]))

# transforming the data
transform_data = PythonOperator(
    task_id = "Select_all_the_required_columns",
    python_callable = _transform,
    dag = dag
)

def _load():
    global data
    with redshift_connector.connect(
         host='examplecluster1.certselwfwzr.ap-south-1.redshift.amazonaws.com',
         database='dev',
         port=5439,
         user='awsuser',
         password='My_password123'
      ) as conn:
        
        # setting autocommit as true
        conn.autocommit = True
        conn.run("VACUUM")

        # Create a Cursor object
        with conn.cursor() as cursor:
            # function to insert every record in the data warehouse 
            def insert(record):
                redshift_connector.paramstyle = 'qmark'
                query = "insert into visits values (?,?,?,?,?,?,?,?,?,?,?,?,?)"
                cursor.execute(query,record)
            
            # insert each record in the table
            for row in data:
                insert(row)

load_data = PythonOperator(
    task_id = "load_the_transformed_data_in_the_data_warehouse",
    python_callable = _load,
    dag = dag
)

download_data >> create_table >> transform_data >> load_data
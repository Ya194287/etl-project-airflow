from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'mongo_to_postgres_replication',
    default_args=default_args,
    description='ETL: MongoDB → PostgreSQL',
    schedule_interval=None,
    catchup=False,
    tags=['etl'],
)

def replicate_collection(collection_name, table_name, id_field):
    # Extract from MongoDB
    mongo_hook = MongoHook(conn_id='mongo_default')
    mongo_client = mongo_hook.get_conn()
    db = mongo_client['shop_db']
    collection = db[collection_name]
    
    docs = list(collection.find())
    print(f"Found {len(docs)} documents in {collection_name}")
    
    if not docs:
        return
    
    # Transform
    transformed = []
    for doc in docs:
        row = {}
        for key, value in doc.items():
            if key == '_id':
                row[id_field] = str(value)
            elif isinstance(value, list):
                # Для массивов нужно специальное форматирование для PostgreSQL
                if value and all(isinstance(x, str) for x in value):
                    # Массив строк -> формат для PostgreSQL
                    row[key] = '{' + ','.join(f'"{x}"' for x in value) + '}'
                else:
                    # Другие типы массивов -> в JSON
                    row[key] = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, dict):
                row[key] = json.dumps(value, ensure_ascii=False)
            else:
                row[key] = value
        transformed.append(row)
    
    # Load to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_etl')
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    
    # Clear table and insert new data
    cur.execute(f"TRUNCATE {table_name};")
    
    if transformed:
        columns = list(transformed[0].keys())
        values = [tuple(d.get(col) for col in columns) for d in transformed]
        
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
        from psycopg2.extras import execute_values
        execute_values(cur, insert_sql, values)
        
        conn.commit()
        print(f"Loaded {len(transformed)} rows into {table_name}")
    
    cur.close()
    conn.close()
    mongo_client.close()

# Tasks for each collection
collections = [
    {'collection': 'UserSessions', 'table': 'user_sessions', 'id_field': 'session_id'},
    {'collection': 'EventLogs', 'table': 'event_logs', 'id_field': 'event_id'},
    {'collection': 'SupportTickets', 'table': 'support_tickets', 'id_field': 'ticket_id'},
    {'collection': 'UserRecommendations', 'table': 'user_recommendations', 'id_field': 'user_id'},
    {'collection': 'ModerationQueue', 'table': 'moderation_queue', 'id_field': 'review_id'},
]

for col in collections:
    task = PythonOperator(
        task_id=f'replicate_{col["collection"]}',
        python_callable=replicate_collection,
        op_kwargs={
            'collection_name': col['collection'],
            'table_name': col['table'],
            'id_field': col['id_field']
        },
        dag=dag,
    )
    # Задачу нужно присвоить переменной, иначе она не зарегистрируется
    globals()[f'replicate_{col["collection"]}'] = task

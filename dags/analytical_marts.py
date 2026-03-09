from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'analytical_marts',
    default_args=default_args,
    description='Create analytical marts',
    schedule_interval=None,
    catchup=False,
    tags=['analytics'],
)

def create_user_activity_mart():
    pg_hook = PostgresHook(postgres_conn_id='postgres_etl')
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    
    # Создаём витрину активности пользователей
    cur.execute("""
        DROP TABLE IF EXISTS user_activity_mart;
        
        CREATE TABLE user_activity_mart AS
        SELECT 
            user_id,
            COUNT(DISTINCT session_id) as session_count,
            COALESCE(SUM(duration_minutes), 0) as total_minutes,
            COALESCE(AVG(duration_minutes), 0) as avg_session_duration,
            COUNT(pages_visited) as total_pages_visited
        FROM user_sessions
        GROUP BY user_id
        ORDER BY total_minutes DESC;
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("User activity mart created successfully")

def create_support_efficiency_mart():
    pg_hook = PostgresHook(postgres_conn_id='postgres_etl')
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    
    # Создаём витрину эффективности поддержки
    cur.execute("""
        DROP TABLE IF EXISTS support_efficiency_mart;
        
        CREATE TABLE support_efficiency_mart AS
        SELECT 
            status,
            issue_type,
            COUNT(*) as ticket_count,
            COALESCE(
                AVG(EXTRACT(EPOCH FROM (updated_at - created_at))/3600),
                0
            ) as avg_resolution_hours,
            COUNT(CASE WHEN status = 'open' THEN 1 END) as open_tickets
        FROM support_tickets
        GROUP BY status, issue_type
        ORDER BY ticket_count DESC;
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Support efficiency mart created successfully")

def check_marts():
    pg_hook = PostgresHook(postgres_conn_id='postgres_etl')
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    
    # Проверяем, что витрины созданы и содержат данные
    cur.execute("SELECT COUNT(*) FROM user_activity_mart")
    user_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM support_efficiency_mart")
    support_count = cur.fetchone()[0]
    
    print(f"User activity mart: {user_count} rows")
    print(f"Support efficiency mart: {support_count} rows")
    
    # Показываем пример данных
    cur.execute("SELECT * FROM user_activity_mart LIMIT 5")
    print("\nSample from user_activity_mart:")
    for row in cur.fetchall():
        print(row)
    
    cur.execute("SELECT * FROM support_efficiency_mart LIMIT 5")
    print("\nSample from support_efficiency_mart:")
    for row in cur.fetchall():
        print(row)
    
    cur.close()
    conn.close()

task1 = PythonOperator(
    task_id='create_user_activity_mart',
    python_callable=create_user_activity_mart,
    dag=dag,
)

task2 = PythonOperator(
    task_id='create_support_efficiency_mart',
    python_callable=create_support_efficiency_mart,
    dag=dag,
)

task3 = PythonOperator(
    task_id='check_marts',
    python_callable=check_marts,
    dag=dag,
)

# Зависимости: сначала создаём витрины, потом проверяем
[task1, task2] >> task3

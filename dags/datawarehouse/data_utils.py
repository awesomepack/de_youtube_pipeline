from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

# Create connection and cursor from airflow hook
def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id = 'postgres_db_ty_elt' , database = 'elt_db')
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory = RealDictCursor)
    return conn , cur

# Close the cursor and connection
def close_conn_cursor(conn , cur):
    cur.close()
    conn.close()


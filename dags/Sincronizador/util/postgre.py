from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


class PostgresQL:
    def __init__(self, conn_id="airflow_db"):
        self.pg_hook = PostgresHook(postgres_conn_id=conn_id)

    def get_conn(self):
        return self.pg_hook.get_conn()

    def fetch_existing_ids(self, table, column, ids):
        """
        Retorna o conjunto de IDs existentes no banco.
        """
        sql = f'SELECT "{column}" FROM {table} WHERE "{column}" = ANY(%s);'
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (ids,))
                return {row[0] for row in cursor.fetchall()}

    def bulk_insert(self, table, columns, values):
        """
        Insere valores em lote no banco. Usa execute_values internamente.
        """
        sql = f"""
            INSERT INTO {table} (
                {', '.join(f'"{col}"' for col in columns)}
            ) VALUES %s
        """
        
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, sql, values)
            conn.commit()

    def fetch_as_dicts(self, table, columns, where_column=None, ids=None):
        """
        Retorna uma lista de dicionários com os dados das colunas especificadas.
        Se where_column e ids forem fornecidos, aplica filtro:
            WHERE "<where_column>" = ANY(%s)
        """
        column_list = ', '.join(f'"{col}"' for col in columns)
        sql = f'SELECT {column_list} FROM {table}'
        params = None

        if where_column and ids is not None:
            sql += f' WHERE "{where_column}" = ANY(%s)'
            params = (ids,)

        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                rows = cursor.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    def bulk_update(self, table, id_column, update_columns, data):
        """
        Atualiza colunas especificadas em lote com base na coluna identificadora (id_column).
        """
        if not data:
            return  # Nada a fazer

        set_clauses = []
        params = {f'id_{i}': row[id_column] for i, row in enumerate(data)}
        
        for col in update_columns:
            case_statements = []
            for i, row in enumerate(data):
                key_id = f"id_{i}"
                key_val = f"{col}_{i}"
                val = row.get(col)
                params[key_val] = val
                case_statements.append(f"WHEN %({key_id})s THEN %({key_val})s")
            case_expr = f'"{col}" = CASE "{id_column}" ' + ' '.join(case_statements) + ' END'
            set_clauses.append(case_expr)

        sql = f'''
            UPDATE {table}
            SET {', '.join(set_clauses)}
            WHERE "{id_column}" IN ({', '.join(f'%({f"id_{i}"})s' for i in range(len(data)))})
        '''

        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
            conn.commit()


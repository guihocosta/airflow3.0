from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

class SqlServer:
    """
    Helper SQL Server. Força paramstyle quando necessário:
      - force_paramstyle="format"  -> usa %s (pymssql)
      - force_paramstyle="qmark"   -> usa ?   (pyodbc)
      - None (default)             -> tenta autodetectar (mas prefira forçar)
    """
    def __init__(self, conn_id="sql_server_conn", force_paramstyle="format"):
        self.mssql_hook = MsSqlHook(mssql_conn_id=conn_id)
        self._paramstyle = force_paramstyle  # "format" (%s) ou "qmark" (?)
    
    def get_conn(self):
        return self.mssql_hook.get_conn()

    def _ensure_paramstyle(self, conn):
        if self._paramstyle:
            return
        # fallback de autodetecção (não confie 100% — por isso o force)
        mod = getattr(conn, "__module__", "")
        if "pymssql" in mod:
            self._paramstyle = "format"
        elif "pyodbc" in mod:
            self._paramstyle = "qmark"
        else:
            cur = conn.cursor()
            mod_c = getattr(cur, "__module__", "")
            cur.close()
            self._paramstyle = "format" if "pymssql" in mod_c else "qmark"

    def _ph(self, n: int) -> str:
        # placeholders por driver
        if self._paramstyle == "format":
            return ", ".join(["%s"] * n)   # pymssql
        return ", ".join(["?"] * n)        # pyodbc

    def fetch_existing_ids(self, table, column, ids):
        if not ids:
            return set()
        with self.get_conn() as conn:
            self._ensure_paramstyle(conn)
            sql = f"SELECT [{column}] FROM {table} WHERE [{column}] IN ({self._ph(len(ids))});"
            with conn.cursor() as cursor:
                cursor.execute(sql, list(ids))
                return {row[0] for row in cursor.fetchall()}

    def bulk_insert(self, table, columns, values):
        if not values:
            return
        # Garante lista de tuplas para executemany
        if isinstance(values, dict):
            raise ValueError("values deve ser lista de tuplas; dict não é suportado em executemany.")
        values = [tuple(v) for v in values]

        with self.get_conn() as conn:
            self._ensure_paramstyle(conn)
            cols_sql = ", ".join(f"[{c}]" for c in columns)
            row_ph = self._ph(len(columns))
            sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({row_ph});"
            with conn.cursor() as cursor:
                # fast_executemany só existe em pyodbc
                if self._paramstyle == "qmark":
                    try:
                        cursor.fast_executemany = True
                    except Exception:
                        pass
                cursor.executemany(sql, values)
            conn.commit()

    def fetch_as_dicts(self, table, columns, where_column=None, ids=None):
        with self.get_conn() as conn:
            self._ensure_paramstyle(conn)
            cols_sql = ", ".join(f"[{c}]" for c in columns)
            sql = f"SELECT {cols_sql} FROM {table}"
            params = []
            if where_column is not None and ids is not None:
                if not ids:
                    return []
                sql += f" WHERE [{where_column}] IN ({self._ph(len(ids))})"
                params.extend(ids)
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def bulk_update(self, table, id_column, update_columns, data):
        if not data or not update_columns:
            return
        with self.get_conn() as conn:
            self._ensure_paramstyle(conn)
            set_clauses, params = [], []

            # CASE por coluna: [col] = CASE [id] WHEN <id> THEN <valor> ... END
            when_token = "WHEN %s THEN %s" if self._paramstyle == "format" else "WHEN ? THEN ?"
            for col in update_columns:
                parts = []
                for row in data:
                    parts.append(when_token)
                    params.append(row[id_column])
                    params.append(row.get(col))
                set_clauses.append(f"[{col}] = CASE [{id_column}] " + " ".join(parts) + " END")

            ids = [row[id_column] for row in data]
            sql = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE [{id_column}] IN ({self._ph(len(ids))});"
            params.extend(ids)
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
            conn.commit()
    
    def fetch_custom_query_as_dicts(self, sql: str, params=None):
        """
        Executa uma consulta SQL completa e retorna os resultados como uma lista de dicionários.
        """
        if params is None:
            params = []
        with self.get_conn() as conn:
            self._ensure_paramstyle(conn)
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

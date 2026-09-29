import os

class SQLLoader:
    """Загрузчик SQL-запросов из директории ./sql/"""
    def __init__(self, sql_dir):
        self.sql_dir = sql_dir

    def get_query(self, filename: str) -> str:
        filepath = os.path.join(self.sql_dir, filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"SQL-файл не найден: {filepath}")
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()
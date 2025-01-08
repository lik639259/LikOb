from typing import Dict, Any, Optional
from .table import Table
from ..sql.parser import SQLParser
from ..sql.executor import QueryExecutor
import json

class SimpleDB:
    def __init__(self):
        self.tables: Dict[str, Table] = {}
        self.parser = SQLParser()
        self.executor = QueryExecutor(self)
        self.current_transaction: Transaction = None

    def execute(self, sql: str) -> Optional[list]:
        """执行SQL语句"""
        parsed = self.parser.parse(sql)
        return self.executor.execute(parsed)

    def create_table(self, name: str, columns: list) -> None:
        """创建表"""
        if name in self.tables:
            raise Exception(f"表 {name} 已存在")
        self.tables[name] = Table(name, columns)

    def get_table(self, name: str) -> Table:
        """获取表"""
        if name not in self.tables:
            raise Exception(f"表 {name} 不存在")
        return self.tables[name]

    def save(self, filename: str):
        """保存数据库到文件"""
        with open(filename, 'w') as f:
            json.dump({table_name: table.data for table_name, table in self.tables.items()}, f)

    def load(self, filename: str):
        """从文件加载数据库"""
        with open(filename, 'r') as f:
            data = json.load(f)
            for table_name, rows in data.items():
                if table_name not in self.tables:
                    raise Exception(f"表 {table_name} 不存在")
                table = self.tables[table_name]
                for row in rows:
                    table.insert(row)
from typing import Dict, Any, Optional
from .table import Table
from ..sql.parser import SQLParser
from ..sql.executor import QueryExecutor

class SimpleDB:
    def __init__(self):
        self.tables: Dict[str, Table] = {}
        self.parser = SQLParser()
        self.executor = QueryExecutor(self)

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
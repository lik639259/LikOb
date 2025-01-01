from typing import List, Dict, Any
from .table import Table
from ..sql.parser import SQLParser
from ..sql.executor import QueryExecutor

class SimpleDB:
    def __init__(self):
        self.tables = {}
        self.parser = SQLParser()
        self.executor = QueryExecutor(self)
    
    def create_table(self, table_name: str, columns: List[tuple]) -> str:
        """创建新表"""
        if table_name in self.tables:
            raise Exception(f"表 {table_name} 已存在")
        
        # 创建新表
        self.tables[table_name] = Table(table_name, columns)
        return f"成功创建表 {table_name}"
    
    def get_table(self, table_name: str) -> Table:
        """获取表对象"""
        if table_name not in self.tables:
            raise Exception(f"表 {table_name} 不存在")
        return self.tables[table_name]
    
    def execute(self, sql: str) :
        """执行SQL语句"""
        # 解析SQL
        parsed_result = self.parser.parse(sql)
        # 执行命令
        return self.executor.execute(parsed_result)
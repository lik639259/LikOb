from typing import Dict, Any, Optional, List, Tuple
from .table import Table
from ..sql.parser import SQLParser
from ..sql.executor import QueryExecutor
import json
from .transaction import Transaction

class SimpleDB:
    def __init__(self):
        self.tables: Dict[str, Table] = {}
        self.parser = SQLParser()
        self.executor = QueryExecutor(self)
        self.current_transaction: Transaction = None

    def begin_transaction(self):
        """开始一个新事务"""
        if self.current_transaction is not None:
            raise Exception("已有活动事务，请先提交或回滚。")
        self.current_transaction = Transaction()

    def execute(self, sql: str) -> Optional[List[Dict[str, Any]]]:
        """执行SQL语句"""
        parsed = self.parser.parse(sql)
        command = parsed['command']
        
        if command == 'BEGIN':
            self.begin_transaction()
            return [{'message': '事务已开始。'}]
        elif command == 'SAVE':
            self.save(parsed['filename'])
            return [{'message': f"数据库已保存到 {parsed['filename']}。"}]
        elif command == 'END':
            return self.end_transaction()
        elif command == 'LOAD':
            self.load(parsed['filename'])
            return [{'message': f"数据库已从 {parsed['filename']} 加载。"}]
        else:
            return self.executor.execute(parsed)
        # 其他命令处理...

    def create_table(self, table_name: str, columns: List[Tuple[str, str]], indexes: Optional[List[str]] = None):
        """创建新表"""
        if table_name in self.tables:
            raise Exception(f"表 {table_name} 已存在")
        self.tables[table_name] = Table(table_name, columns, indexes)

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

    def end_transaction(self):
        """结束当前事务"""
        if self.current_transaction is None:
            raise Exception("没有活动事务。")
        self.current_transaction.commit()  # 提交事务
        self.current_transaction = None
        return [{'message': '事务已结束。'}]
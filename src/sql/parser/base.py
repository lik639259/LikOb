from typing import Tuple, Any
from ...core.exceptions import SQLParseError
from .select import SelectParser
from .dml import DMLParser
from .ddl import DDLParser

class SQLParser:
    def __init__(self):
        self.select_parser = SelectParser()
        self.dml_parser = DMLParser()
        self.ddl_parser = DDLParser()

    def parse(self, sql: str) -> Tuple[str, ...]:
        """解析SQL语句的主入口"""
        sql = sql.strip()
        if sql.endswith(';'):
            sql = sql[:-1].strip()
        
        sql = self._preserve_string_case(sql.lower())
        
        if sql.startswith('select'):
            return self.select_parser.parse(sql)
        elif sql.startswith('insert'):
            return self.dml_parser.parse_insert(sql)
        elif sql.startswith('update'):
            return self.dml_parser.parse_update(sql)
        elif sql.startswith('delete'):
            return self.dml_parser.parse_delete(sql)
        elif sql.startswith('create'):
            return self.ddl_parser.parse_create(sql)
        elif sql.startswith('alter'):
            return self.ddl_parser.parse_alter(sql)
        elif sql.startswith('drop'):
            return self.ddl_parser.parse_drop(sql)
        else:
            raise SQLParseError("不支持的SQL语句")

    def _preserve_string_case(self, sql: str) -> str:
        """保留字符串字面量的大小写"""
        import re
        def replace(match):
            return match.group(0)
        return re.sub(r"'[^']*'", replace, sql)
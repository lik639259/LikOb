import re
from typing import Tuple, List, Any, Dict
from ...core.exceptions import SQLParseError
from .condition import ConditionParser
from .utils import parse_value

class DMLParser:
    def __init__(self):
        self.condition_parser = ConditionParser()

    def parse_insert(self, sql: str) -> Tuple[str, str, List[Any]]:
        """解析INSERT语句"""
        pattern = r"insert into (\w+)\s+(?:values\s+)?\((.*)\)"
        match = re.match(pattern, sql)
        if not match:
            raise SQLParseError("INSERT 语法错误")
        
        table_name = match.group(1)
        values_str = match.group(2)
        values = self._parse_values(values_str)
        
        return ("INSERT", table_name, values)

    def parse_update(self, sql: str) -> Tuple[str, str, List[Tuple[str, Any]], Dict]:
        """解析UPDATE语句"""
        pattern = r"update (\w+) set (.*?)(?: where (.*))?$"
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise SQLParseError("UPDATE 语法错误")
        
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)
        
        # 解析SET子句
        updates = []
        for assignment in set_clause.split(','):
            col, expr = assignment.split('=')
            col = col.strip()
            expr = expr.strip()
            
            # 处理算术表达式
            if '*' in expr or '+' in expr or '-' in expr or '/' in expr:
                updates.append((col, ('EXPR', expr)))
            else:
                updates.append((col, parse_value(expr)))
        
        # 解析WHERE子句
        where_condition = None
        if where_clause:
            where_condition = self.condition_parser.parse_condition(where_clause)
        
        return ("UPDATE", table_name, updates, where_condition)

    def parse_delete(self, sql: str) -> Tuple[str, str, Dict]:
        """解析DELETE语句"""
        pattern = r"delete from (\w+)(?: where (.*))?$"
        match = re.match(pattern, sql)
        if not match:
            raise SQLParseError("DELETE 语法错误")
        
        table_name = match.group(1)
        where_clause = match.group(2)
        
        where_condition = None
        if where_clause:
            where_condition = self.condition_parser.parse_condition(where_clause)
        
        return ("DELETE", table_name, where_condition)

    def _parse_values(self, values_str: str) -> List[Any]:
        """解析VALUES列表"""
        values = []
        current = ''
        in_string = False
        
        for char in values_str:
            if char == "'" and not in_string:
                in_string = True
                current += char
            elif char == "'" and in_string:
                in_string = False
                current += char
            elif char == ',' and not in_string:
                values.append(parse_value(current.strip()))
                current = ''
            else:
                current += char
        
        if current:
            values.append(parse_value(current.strip()))
        
        return values
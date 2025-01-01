import re
from typing import Tuple, List
from ...core.exceptions import SQLParseError

class DDLParser:
    def parse_create(self, sql: str) -> Tuple[str, ...]:
        """解析CREATE语句"""
        if 'table' in sql:
            return self._parse_create_table(sql)
        elif 'index' in sql:
            return self._parse_create_index(sql)
        raise SQLParseError("不支持的CREATE语句")

    def _parse_create_table(self, sql: str) -> Tuple[str, str, List[tuple]]:
        """解析CREATE TABLE语句"""
        pattern = r"create\s+table\s+(\w+)\s*\((.*)\)"
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise SQLParseError("CREATE TABLE 语法错误")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        columns = []
        
        # 解析列定义
        for col_def in columns_str.split(','):
            col_def = col_def.strip()
            if not col_def:
                continue
            
            parts = col_def.split()
            if len(parts) < 2:
                raise SQLParseError(f"列定义错误: {col_def}")
            
            name = parts[0]
            type_ = parts[1].upper()
            
            # 处理可选的列约束
            constraints = []
            for constraint in parts[2:]:
                constraint = constraint.upper()
                if constraint in ['NOT', 'NULL'] and len(constraints) > 0:
                    constraints[-1] += f" {constraint}"
                else:
                    constraints.append(constraint)
            
            columns.append((name, type_))  # 暂时只返回名称和类型
        
        return ("CREATE", table_name, columns)

    def _parse_create_index(self, sql: str) -> Tuple[str, str, str, List[str]]:
        """解析CREATE INDEX语句"""
        pattern = r"create(?: unique)? index (\w+) on (\w+) \((.*)\)"
        match = re.match(pattern, sql)
        if not match:
            raise SQLParseError("CREATE INDEX 语法错误")
        
        index_name = match.group(1)
        table_name = match.group(2)
        columns = [col.strip() for col in match.group(3).split(',')]
        
        return ("CREATE_INDEX", index_name, table_name, columns)

    def parse_alter(self, sql: str) -> Tuple[str, ...]:
        """解析ALTER TABLE语句"""
        pattern = r"alter table (\w+) (add|drop|modify) (.*)"
        match = re.match(pattern, sql)
        if not match:
            raise SQLParseError("ALTER TABLE 语法错误")
        
        table_name = match.group(1)
        operation = match.group(2).upper()
        details = match.group(3)
        
        if operation == 'ADD':
            column_def = self._parse_column_definition(details)
            return ("ALTER", table_name, "ADD", column_def)
        elif operation == 'DROP':
            return ("ALTER", table_name, "DROP", details.strip())
        elif operation == 'MODIFY':
            column_def = self._parse_column_definition(details)
            return ("ALTER", table_name, "MODIFY", column_def)

    def _parse_column_definition(self, col_def: str) -> Tuple[str, str, List[str]]:
        """解析列定义，包括约束"""
        parts = col_def.split()
        if len(parts) < 2:
            raise SQLParseError(f"列定义错误: {col_def}")
        
        name = parts[0]
        type_ = parts[1].upper()
        constraints = []
        
        i = 2
        while i < len(parts):
            if parts[i].upper() == 'PRIMARY' and i + 1 < len(parts) and parts[i + 1].upper() == 'KEY':
                constraints.append('PRIMARY KEY')
                i += 2
            elif parts[i].upper() == 'REFERENCES':
                # 收集完整的REFERENCES约束
                ref_constraint = ['REFERENCES']
                i += 1
                while i < len(parts) and not parts[i].upper() in ['PRIMARY', 'NOT', 'NULL', 'REFERENCES']:
                    ref_constraint.append(parts[i])
                    i += 1
                constraints.append(' '.join(ref_constraint))
            elif parts[i].upper() == 'NOT' and i + 1 < len(parts) and parts[i + 1].upper() == 'NULL':
                constraints.append('NOT NULL')
                i += 2
            else:
                constraints.append(parts[i].upper())
                i += 1
        
        return (name, type_, constraints)
from typing import Dict, Tuple, List
from ...core.exceptions import SQLParseError
from .condition import ConditionParser

class SelectParser:
    def __init__(self):
        self.condition_parser = ConditionParser()

    def parse(self, sql: str) -> Tuple[str, Dict]:
        """解析SELECT语句"""
        parts = {
            'type': 'SELECT',
            'distinct': False,
            'columns': [],
            'tables': [],
            'joins': [],
            'where': None,
            'group_by': [],
            'having': None,
            'order_by': [],
            'limit': None,
            'offset': None
        }
        
        # 解析DISTINCT
        if 'distinct' in sql:
            parts['distinct'] = True
            sql = sql.replace('distinct', '', 1)
        
        # 解析列
        columns_end = sql.find(' from ')
        if columns_end == -1:
            raise SQLParseError("缺少 FROM 子句")
        
        columns_str = sql[7:columns_end].strip()
        # 处理带别名的列
        parts['columns'] = self._parse_columns(columns_str)
        
        # 解析剩余部分
        self._parse_from_clause(sql[columns_end:], parts)
        
        return ('SELECT', parts)

    def _parse_columns(self, columns_str: str) -> List[str]:
        """解析列定义，包括聚合函数和别名"""
        columns = []
        current = ''
        in_parentheses = 0
        in_quotes = False
        
        for char in columns_str:
            if char == '(' and not in_quotes:
                in_parentheses += 1
                current += char
            elif char == ')' and not in_quotes:
                in_parentheses -= 1
                current += char
            elif char == "'" and not in_quotes:
                in_quotes = True
                current += char
            elif char == "'" and in_quotes:
                in_quotes = False
                current += char
            elif char == ',' and in_parentheses == 0 and not in_quotes:
                columns.append(self._process_column(current.strip()))
                current = ''
            else:
                current += char
        
        if current:
            columns.append(self._process_column(current.strip()))
        
        return columns

    def _process_column(self, col: str) -> str:
        """处理单个列定义，包括函数和别名"""
        # 处理 COUNT(*)
        if col.lower().startswith('count(*)'):
            parts = col.lower().split(' as ')
            if len(parts) > 1:
                return f"COUNT(*) AS {parts[1].strip()}"
            return "COUNT(*)"
        
        # 处理其他聚合函数
        if '(' in col:
            func_start = col.find('(')
            func_end = col.rfind(')')
            if func_end > func_start:
                func_name = col[:func_start].strip().upper()
                func_arg = col[func_start+1:func_end].strip()
                remainder = col[func_end+1:].strip()
                
                # 处理别名
                if ' as ' in remainder.lower():
                    alias = remainder.lower().split(' as ')[1].strip()
                    return f"{func_name}({func_arg}) AS {alias}"
                return f"{func_name}({func_arg})"
        
        # 处理普通列的别名
        parts = col.split(' as ')
        if len(parts) > 1:
            return f"{parts[0].strip()} AS {parts[1].strip()}"
        return col.strip()

    def _parse_from_clause(self, sql: str, parts: Dict) -> None:
        """解析FROM子句及后续内容"""
        sql = sql[5:].strip()  # 移除 'from '
        
        # 分解各个子句
        clauses = {
            'where': '',
            'group by': '',
            'having': '',
            'order by': '',
            'limit': ''
        }
        
        # 从后向前解析子句，避免关键字冲突
        for clause in ['limit', 'order by', 'having', 'group by', 'where']:
            pos = sql.lower().find(f' {clause} ')
            if pos != -1:
                clauses[clause] = sql[pos + len(clause) + 2:].strip()
                sql = sql[:pos]
        
        # 解析表和JOIN
        if ' join ' in sql.lower():
            self._parse_joins(sql, parts)
        else:
            parts['tables'] = [t.strip() for t in sql.split(',')]
        
        # 解析WHERE子句
        if clauses['where']:
            parts['where'] = self.condition_parser.parse_condition(clauses['where'])
        
        # 解析GROUP BY
        if clauses['group by']:
            parts['group_by'] = [col.strip() for col in clauses['group by'].split(',')]
        
        # 解析HAVING
        if clauses['having']:
            parts['having'] = self.condition_parser.parse_condition(clauses['having'])
        
        # 解析ORDER BY
        if clauses['order by']:
            self._parse_order_by(clauses['order by'], parts)
        
        # 解析LIMIT
        if clauses['limit']:
            self._parse_limit(clauses['limit'], parts)

    def _parse_joins(self, sql: str, parts: Dict) -> None:
        """解析JOIN子句"""
        join_parts = sql.lower().split(' join ')
        parts['tables'] = [join_parts[0].strip()]
        
        for join_part in join_parts[1:]:
            if ' on ' not in join_part:
                raise SQLParseError("JOIN 必须包含 ON 条件")
            
            table_part, condition = join_part.split(' on ', 1)
            parts['joins'].append({
                'table': table_part.strip(),
                'condition': self.condition_parser.parse_condition(condition.strip())
            })

    def _parse_order_by(self, order_by: str, parts: Dict) -> None:
        """解析ORDER BY子句"""
        order_parts = order_by.split(',')
        for part in order_parts:
            part = part.strip()
            if ' desc' in part.lower():
                col = part[:-5].strip()
                parts['order_by'].append((col, 'DESC'))
            elif ' asc' in part.lower():
                col = part[:-4].strip()
                parts['order_by'].append((col, 'ASC'))
            else:
                parts['order_by'].append((part, 'ASC'))

    def _parse_limit(self, limit: str, parts: Dict) -> None:
        """解析LIMIT子句"""
        if 'offset' in limit.lower():
            limit_parts = limit.split('offset')
            parts['limit'] = int(limit_parts[0].strip())
            parts['offset'] = int(limit_parts[1].strip())
        else:
            parts['limit'] = int(limit.strip())
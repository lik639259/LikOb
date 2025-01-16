import re
from typing import Tuple, List, Dict, Any, Optional

class SQLParser:
    def parse(self, sql: str) -> Dict[str, Any]:
        """解析SQL语句"""
        # 移除末尾的分号和空白字符
        sql = sql.strip().rstrip(';').strip()
        
        if sql.upper().startswith('BEGIN'):
            return {'command': 'BEGIN'}
        elif sql.upper().startswith('SAVE'):
            match = re.match(r"SAVE\s+'(.+)'", sql, re.IGNORECASE)
            if match:
                return {'command': 'SAVE', 'filename': match.group(1)}
            raise Exception("无效的SAVE语句")
        elif sql.upper().startswith('END'):
            return {'command': 'END'}
        elif sql.upper().startswith('LOAD'):
            match = re.match(r"LOAD\s+'(.+)'", sql, re.IGNORECASE)
            if match:
                return {'command': 'LOAD', 'filename': match.group(1)}
            raise Exception("无效的LOAD语句")
        elif sql.upper().startswith('CREATE TABLE'):
            return self._parse_create(sql)
        elif sql.upper().startswith('INSERT INTO'):
            return self._parse_insert(sql)
        elif sql.upper().startswith('SELECT'):
            match = re.match(r"SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+JOIN\s+(\w+)\s+ON\s+(\w+))?", sql, re.IGNORECASE)
            if match:
                columns = match.group(1).split(',')
                table = match.group(2)
                join_table = match.group(3)
                on_column = match.group(4)
                parsed = {
                    'command': 'SELECT',
                    'columns': [col.strip() for col in columns],
                    'table': table
                }
                if join_table:
                    parsed['join'] = {
                        'table': join_table,
                        'on': on_column
                    }
                return parsed
        elif sql.upper().startswith('UPDATE'):
            return self._parse_update(sql)
        elif sql.upper().startswith('DELETE'):
            return self._parse_delete(sql)
        
        raise Exception("不支持的SQL语句")

    def _parse_create(self, sql: str) -> Dict[str, Any]:
        """解析CREATE TABLE语句"""
        match = re.match(r'CREATE\s+TABLE\s+(\w+)\s*\((.*)\)', sql, re.IGNORECASE)
        if not match:
            raise Exception("无效的CREATE TABLE语句")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        
        columns = []
        for col in columns_str.split(','):
            col = col.strip()
            parts = col.split()
            if len(parts) < 2:
                raise Exception(f"无效的列定义: {col}")
            
            col_name = parts[0]
            col_type = parts[1]
            columns.append((col_name, col_type))
        
        return {
            'command': 'CREATE',
            'table': table_name,
            'columns': columns
        }

    def _parse_insert(self, sql: str) -> Dict[str, Any]:
        """解析INSERT INTO语句"""
        match = re.match(r'INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.*)\)', sql, re.IGNORECASE)
        if not match:
            raise Exception("无效的INSERT INTO语句")
        
        table_name = match.group(1)
        values_str = match.group(2)
        
        values = []
        for val in values_str.split(','):
            val = val.strip()
            if val.startswith("'") and val.endswith("'"):
                values.append(val[1:-1])  # 字符串
            else:
                try:
                    values.append(float(val) if '.' in val else int(val))  # 数字
                except ValueError:
                    values.append(val)  # 其他类型
        
        return {
            'command': 'INSERT',
            'table': table_name,
            'values': values
        }

    def _parse_conditions(self, clause: str) -> Dict[str, Any]:
        """解析条件子句"""
        conditions = {
            'operator': 'AND',
            'conditions': []
        }
        
        # 处理AND条件
        for condition in clause.split('AND'):
            condition = condition.strip()
            # 检查聚合函数
            agg_match = re.match(r'(COUNT|SUM|AVG|MAX|MIN)\((.*?)\)\s*(=|!=|>|>=|<|<=)\s*(.*)', condition)
            if agg_match:
                func, arg, op, val = agg_match.groups()
                conditions['conditions'].append({
                    'type': 'aggregate',
                    'function': func.upper(),
                    'argument': arg.strip(),
                    'operator': op,
                    'value': self._parse_value(val.strip())
                })
            else:
                for op in ['>=', '<=', '!=', '=', '>', '<']:
                    if op in condition:
                        col, val = condition.split(op)
                        conditions['conditions'].append({
                            'type': 'simple',
                            'column': col.strip(),
                            'operator': op,
                            'value': self._parse_value(val.strip())
                        })
                        break
        
        return conditions

    def _parse_value(self, val: str) -> Any:
        """解析值"""
        if val.startswith("'") and val.endswith("'"):
            return val[1:-1]
        try:
            return float(val) if '.' in val else int(val)
        except ValueError:
            return val

    def _parse_select(self, sql: str) -> Dict[str, Any]:
        """解析SELECT语句"""
        result = {
            'command': 'SELECT',
            'columns': None,
            'table': None,
            'where': None,
            'group_by': None,
            'having': None,
            'order_by': None,
            'aggregates': []
        }
        
        # 解析列名和聚合函数
        columns_end = sql.upper().find('FROM')
        columns_str = sql[6:columns_end].strip()
        
        if columns_str == '*':
            result['columns'] = None
        else:
            result['columns'] = []
            for col in columns_str.split(','):
                col = col.strip()
                # 检查是否是聚合函数
                agg_match = re.match(r'(COUNT|SUM|AVG|MAX|MIN)\((.*?)\)(?:\s+[aA][sS]\s+(\w+))?', col)
                if agg_match:
                    func, arg, alias = agg_match.groups()
                    result['aggregates'].append({
                        'function': func.upper(),
                        'argument': arg.strip(),
                        'alias': alias if alias else f"{func.lower()}_{arg.strip()}"
                    })
                else:
                    result['columns'].append(col)
        
        # 解析表名
        sql = sql[columns_end + 4:].strip()
        table_end = sql.find(' ')
        if table_end == -1:
            result['table'] = sql
            return result
        result['table'] = sql[:table_end]
        sql = sql[table_end:].strip()
        
        # 解析WHERE
        where_idx = sql.upper().find('WHERE')
        if where_idx != -1:
            group_idx = sql.upper().find('GROUP BY')
            having_idx = sql.upper().find('HAVING')
            order_idx = sql.upper().find('ORDER BY')
            
            where_end = min(x for x in [group_idx, having_idx, order_idx, len(sql)] if x != -1)
            where_clause = sql[where_idx + 5:where_end].strip()
            result['where'] = self._parse_conditions(where_clause)
            sql = sql[where_end:].strip()
        
        # 解析GROUP BY
        group_idx = sql.upper().find('GROUP BY')
        if group_idx != -1:
            having_idx = sql.upper().find('HAVING')
            order_idx = sql.upper().find('ORDER BY')
            
            group_end = min(x for x in [having_idx, order_idx, len(sql)] if x != -1)
            group_clause = sql[group_idx + 8:group_end].strip()
            result['group_by'] = [col.strip() for col in group_clause.split(',')]
            sql = sql[group_end:].strip()
        
        # 解析HAVING
        having_idx = sql.upper().find('HAVING')
        if having_idx != -1:
            order_idx = sql.upper().find('ORDER BY')
            having_end = order_idx if order_idx != -1 else len(sql)
            having_clause = sql[having_idx + 6:having_end].strip()
            result['having'] = self._parse_conditions(having_clause)
            sql = sql[having_end:].strip()
        
        # 解析ORDER BY
        order_idx = sql.upper().find('ORDER BY')
        if order_idx != -1:
            order_clause = sql[order_idx + 8:].strip()
            orders = []
            for item in order_clause.split(','):
                item = item.strip()
                if item.upper().endswith('DESC'):
                    orders.append((item[:-4].strip(), 'DESC'))
                else:
                    orders.append((item.rstrip('ASC').strip(), 'ASC'))
            result['order_by'] = orders
        
        return result

    def _parse_update(self, sql: str) -> Dict[str, Any]:
        """解析UPDATE语句"""
        # UPDATE table SET col1 = val1, col2 = val2 WHERE conditions
        match = re.match(r'UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$', sql, re.IGNORECASE)
        if not match:
            raise Exception("无效的UPDATE语句")
        
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)
        
        # 解析SET子句
        updates = {}
        for item in set_clause.split(','):
            col, val = item.split('=')
            col = col.strip()
            val = val.strip()
            if val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
            else:
                try:
                    val = float(val) if '.' in val else int(val)
                except ValueError:
                    pass
            updates[col] = val
        
        result = {
            'command': 'UPDATE',
            'table': table_name,
            'updates': updates,
        }
        
        if where_clause:
            result['where'] = self._parse_conditions(where_clause)
        
        return result

    def _parse_delete(self, sql: str) -> Dict[str, Any]:
        """解析DELETE语句"""
        # DELETE FROM table WHERE conditions
        match = re.match(r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?$', sql, re.IGNORECASE)
        if not match:
            raise Exception("无效的DELETE语句")
        
        table_name = match.group(1)
        where_clause = match.group(2)
        
        result = {
            'command': 'DELETE',
            'table': table_name,
        }
        
        if where_clause:
            result['where'] = self._parse_conditions(where_clause)
        
        return result 
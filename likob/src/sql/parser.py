import re
from typing import Tuple, List, Dict, Any, Optional

class SQLParser:
    def parse(self, sql: str) -> Dict[str, Any]:
        """解析SQL语句"""
        # 移除末尾的分号和空白字符
        sql = sql.strip().rstrip(';').strip()
        
        if sql.upper().startswith('CREATE TABLE'):
            return self._parse_create(sql)
        elif sql.upper().startswith('INSERT INTO'):
            return self._parse_insert(sql)
        elif sql.upper().startswith('SELECT'):
            return self._parse_select(sql)
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

    def _parse_conditions(self, where_clause: str) -> Dict[str, Any]:
        """解析WHERE条件"""
        conditions = {
            'operator': 'AND',  # 默认操作符
            'conditions': []
        }
        
        # 处理基本比较
        comparisons = where_clause.split('AND')
        for comp in comparisons:
            comp = comp.strip()
            for op in ['>=', '<=', '!=', '=', '>', '<']:
                if op in comp:
                    col, val = comp.split(op)
                    val = val.strip()
                    if val.startswith("'") and val.endswith("'"):
                        val = val[1:-1]
                    else:
                        try:
                            val = float(val) if '.' in val else int(val)
                        except ValueError:
                            pass
                    conditions['conditions'].append({
                        'column': col.strip(),
                        'operator': op,
                        'value': val
                    })
                    break
        
        return conditions

    def _parse_select(self, sql: str) -> Dict[str, Any]:
        """解析SELECT语句"""
        # 基本结构
        result = {
            'command': 'SELECT',
            'columns': None,
            'table': None,
            'where': None,
            'group_by': None,
            'order_by': None
        }
        
        # 解析列名
        columns_end = sql.upper().find('FROM')
        columns_str = sql[6:columns_end].strip()
        if columns_str == '*':
            result['columns'] = None
        else:
            result['columns'] = [col.strip() for col in columns_str.split(',')]
        
        # 解析表名
        sql = sql[columns_end + 4:].strip()
        table_end = sql.find(' ')
        if table_end == -1:
            result['table'] = sql
        else:
            result['table'] = sql[:table_end]
            sql = sql[table_end:].strip()
        
        # 解析WHERE条件
        where_idx = sql.upper().find('WHERE')
        if where_idx != -1:
            where_end = sql.upper().find('GROUP BY')
            if where_end == -1:
                where_end = sql.upper().find('ORDER BY')
            if where_end == -1:
                where_end = len(sql)
            where_clause = sql[where_idx + 5:where_end].strip()
            result['where'] = self._parse_conditions(where_clause)
        
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
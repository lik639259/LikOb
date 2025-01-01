from typing import List, Dict, Any, Tuple, Optional
from ..core.exceptions import SQLParseError

class QueryExecutor:
    def __init__(self, database):
        self.db = database
    
    def execute(self, parsed_command: tuple) -> Any:
        """执行解析后的SQL命令"""
        command_type = parsed_command[0]
        
        if command_type == "SELECT":
            return self._execute_select(parsed_command[1])
        elif command_type == "INSERT":
            return self._execute_insert(parsed_command[1], parsed_command[2])
        elif command_type == "UPDATE":
            return self._execute_update(parsed_command[1], parsed_command[2], parsed_command[3])
        elif command_type == "DELETE":
            return self._execute_delete(parsed_command[1], parsed_command[2])
        elif command_type == "CREATE":
            return self._execute_create(parsed_command[1], parsed_command[2])
        else:
            raise SQLParseError(f"不支持的命令类型: {command_type}")

    def _execute_create(self, table_name: str, columns: List[tuple]) -> str:
        """执行CREATE TABLE命令"""
        try:
            return self.db.create_table(table_name, columns)
        except Exception as e:
            raise Exception(f"创建表失败: {str(e)}")

    def _execute_insert(self, table_name: str, values: List[Any]) -> str:
        """执行INSERT命令"""
        table = self.db.get_table(table_name)
        if len(values) != len(table.columns):
            raise SQLParseError(f"列数不匹配: 期望 {len(table.columns)} 列，实际 {len(values)} 列")
        
        # 创建列值字典
        value_dict = {}
        for (col_name, _), value in zip(table.columns.items(), values):
            value_dict[col_name] = value
        
        # 插入数据
        success = table.insert(value_dict)
        return "插入成功" if success else "插入失败"

    def _execute_select(self, select_parts: dict) -> List[Dict]:
        """执行SELECT查询"""
        table = self.db.get_table(select_parts['tables'][0])
        
        # 尝试使用索引
        if select_parts['where']:
            result = self._try_use_index(table, select_parts['where'])
            if result is not None:
                # 使用索引找到的结果
                data = result
            else:
                # 没有合适的索引，使用全表扫描
                data = table.data.copy()
                data = self._apply_where(data, select_parts['where'])
        else:
            data = table.data.copy()
        
        result = data
        
        # 应用GROUP BY
        if select_parts['group_by']:
            result = self._apply_group_by(result, select_parts)
            
            # 应用HAVING（只在GROUP BY之后）
            if select_parts['having']:
                # 替换having条件中的别名
                having_condition = self._resolve_aliases(select_parts['having'], result[0] if result else {})
                result = [row for row in result if self._evaluate_condition(row, having_condition)]
        
        # 应用ORDER BY
        if select_parts['order_by']:
            result = self._apply_order_by(result, select_parts['order_by'])
        
        # 应用LIMIT/OFFSET
        if select_parts['limit'] is not None:
            offset = select_parts['offset'] or 0
            result = result[offset:offset + select_parts['limit']]
        
        return result

    def _execute_update(self, table_name: str, updates: List[Tuple[str, Any]], where_condition: Dict) -> str:
        """执行UPDATE命令"""
        table = self.db.get_table(table_name)
        count = 0
        
        for row in table.data:
            if where_condition is None or self._evaluate_condition(row, where_condition):
                for col_name, new_value in updates:
                    row[col_name] = new_value
                count += 1
        
        return f"更新了 {count} 行数据"

    def _execute_delete(self, table_name: str, where_condition: Dict) -> str:
        """执行DELETE命令"""
        table = self.db.get_table(table_name)
        original_length = len(table.data)
        
        if where_condition is None:
            # 删除所有数据
            table.data.clear()
            return f"删除了 {original_length} 行数据"
        
        # 使用列表推导式保留不满足条件的行
        table.data = [row for row in table.data 
                     if not self._evaluate_condition(row, where_condition)]
        
        deleted_count = original_length - len(table.data)
        return f"删除了 {deleted_count} 行数据"

    def _apply_where(self, data: List[Dict], condition: Dict) -> List[Dict]:
        """应用WHERE条件"""
        return [row for row in data if self._evaluate_condition(row, condition)]

    def _evaluate_condition(self, row: Dict, condition: Dict) -> bool:
        """评估条件，包括别名处理"""
        op = condition['operator']
        
        if op in ('AND', 'OR'):
            results = [self._evaluate_condition(row, cond) 
                      for cond in condition['conditions']]
            return all(results) if op == 'AND' else any(results)
        
        # 获取列值，支持别名
        if 'column' in condition:
            column = condition['column']
            if column not in row:
                raise SQLParseError(f"未知的列名或别名: {column}")
            value = row[column]
            
            if op in ('IN', 'NOT IN'):
                is_in = value in condition['values']
                return not is_in if op == 'NOT IN' else is_in
            elif op == 'BETWEEN':
                return condition['low'] <= value <= condition['high']
            else:
                target = condition['value']
                if op == '=': return value == target
                if op == '!=': return value != target
                if op == '>': return value > target
                if op == '<': return value < target
                if op == '>=': return value >= target
                if op == '<=': return value <= target
        
        return False

    def _apply_group_by(self, data: List[Dict], select_parts: dict) -> List[Dict]:
        """应用GROUP BY"""
        from collections import defaultdict
        
        groups = defaultdict(list)
        group_cols = select_parts['group_by']
        
        # 分组
        for row in data:
            key = tuple(row[col] for col in group_cols)
            groups[key].append(row)
        
        # 处理聚合
        result = []
        for group_key, group_rows in groups.items():
            new_row = {col: group_key[i] for i, col in enumerate(group_cols)}
            
            # 处理聚合函数
            for col in select_parts['columns']:
                col_lower = col.lower()
                if 'count(*)' in col_lower:
                    # 处理 COUNT(*)
                    alias = col_lower.split(' as ')[1] if ' as ' in col_lower else 'count(*)'
                    new_row[alias] = len(group_rows)
                elif '(' in col:
                    # 处理其他聚合函数
                    func_name = col[:col.find('(')].upper()
                    field = col[col.find('(')+1:col.find(')')]
                    alias = col.split(' AS ')[1] if ' AS ' in col else col
                    
                    values = [row[field] for row in group_rows]
                    if func_name == 'COUNT':
                        new_row[alias] = len(values)
                    elif func_name == 'SUM':
                        new_row[alias] = sum(values)
                    elif func_name == 'AVG':
                        new_row[alias] = sum(values) / len(values)
                    elif func_name == 'MAX':
                        new_row[alias] = max(values)
                    elif func_name == 'MIN':
                        new_row[alias] = min(values)
                else:
                    # 非聚合列
                    new_row[col] = group_rows[0][col]
            
            result.append(new_row)
        
        return result

    def _apply_order_by(self, data: List[Dict], order_specs: List[Tuple[str, str]]) -> List[Dict]:
        """应用ORDER BY"""
        def get_sort_key(item):
            result = []
            for col, direction in order_specs:
                val = item[col]
                # 处理数值类型
                if isinstance(val, (int, float)):
                    val = -val if direction == 'DESC' else val
                # 处理字符串类型
                elif isinstance(val, str):
                    val = val if direction == 'ASC' else chr(255 - ord(val[0])) + val[1:]
                result.append(val)
            return tuple(result)
        
        return sorted(data, key=get_sort_key)

    def _select_columns(self, data: List[Dict], columns: List[str]) -> List[Dict]:
        """选择指定列"""
        return [{col: row[col] for col in columns} for row in data]

    def _format_result(self, result: List[Dict]) -> str:
        """格式化查询结果为表格形式"""
        if not result:
            return "空结果集"
        
        # 获取所有列
        columns = list(result[0].keys())
        
        # 计算每列的最大宽度
        widths = {col: len(str(col)) for col in columns}
        for row in result:
            for col in columns:
                widths[col] = max(widths[col], len(str(row[col])))
        
        # 构建表头
        header = "| " + " | ".join(f"{col.upper():{widths[col]}}" for col in columns) + " |"
        separator = "+" + "+".join("-" * (widths[col] + 2) for col in columns) + "+"
        
        # 构建数据行
        rows = []
        for row in result:
            formatted_row = "| " + " | ".join(f"{str(row[col]):{widths[col]}}" for col in columns) + " |"
            rows.append(formatted_row)
        
        # 组合所有部分
        return "\n".join([
            separator,
            header,
            separator,
            *rows,
            separator
        ])

    def _apply_having(self, data: List[Dict], having_condition: Dict) -> List[Dict]:
        """应用HAVING过滤"""
        return [row for row in data if self._evaluate_condition(row, having_condition)]

    def _resolve_aliases(self, condition: Dict, sample_row: Dict) -> Dict:
        """解析条件中的别名"""
        if 'column' in condition:
            # 如果列名是别名，尝试在结果中找到对应的列
            if condition['column'] in sample_row:
                return condition
            raise SQLParseError(f"未知的列名或别名: {condition['column']}")
        
        # 递归处理复合条件
        if 'conditions' in condition:
            return {
                'operator': condition['operator'],
                'conditions': [self._resolve_aliases(c, sample_row) for c in condition['conditions']]
            }
        return condition

    def _try_use_index(self, table: 'Table', condition: Dict) -> Optional[List[Dict]]:
        """尝试使用索引查询"""
        if condition['operator'] in ('=', '>', '<', '>=', '<='):
            column = condition['column']
            if column in table.indexes:
                value = condition['value']
                if condition['operator'] == '=':
                    row_ids = table.indexes[column].find(value)
                else:
                    # 范围查询
                    start = value if condition['operator'] in ('>', '>=') else None
                    end = value if condition['operator'] in ('<', '<=') else None
                    row_ids = table.indexes[column].find_range(start, end)
                
                return [table.data[row_id] for row_id in row_ids]
        
        return None
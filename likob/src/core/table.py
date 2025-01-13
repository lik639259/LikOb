from typing import List, Dict, Any, Tuple, Optional, Union
import operator
from collections import defaultdict
import threading

class Table:
    def __init__(self, name: str, columns: List[Tuple[str, str]], indexes: Optional[List[str]] = None):
        self.name = name
        self.columns = {}
        self.data = []
        self.indexes: Dict[str, List[int]] = {}  # 存储索引
        self.primary_key = None
        
        # 处理列定义
        for col_name, col_type in columns:
            self.columns[col_name] = col_type.upper()

        # 创建索引
        if indexes:
            for index in indexes:
                self.create_index(index)

    def create_index(self, column: str):
        """创建索引"""
        if column not in self.columns:
            raise Exception(f"列 {column} 不存在")
        
        if column in self.indexes:
            raise Exception(f"列 {column} 已经有索引")
        
        # 创建索引
        self.indexes[column] = []
        for i, row in enumerate(self.data):
            self.indexes[column].append((row[column], i))

        # 按照索引列排序
        self.indexes[column].sort(key=lambda x: x[0])

    def insert(self, values: List[Any]) -> None:
        """插入数据"""
        with self.lock:  # 使用锁
            column_names = list(self.columns.keys())
            if len(values) != len(column_names):
                raise Exception(f"值的数量 ({len(values)}) 与列的数量 ({len(column_names)}) 不匹配")
            
            # 类型转换和验证
            row_data = {}
            for col_name, value in zip(column_names, values):
                col_type = self.columns[col_name]
                try:
                    if col_type == 'INT':
                        value = int(value)
                    elif col_type == 'FLOAT':
                        value = float(value)
                    elif col_type == 'TEXT':
                        value = str(value)
                except ValueError:
                    raise Exception(f"列 {col_name} 的值 {value} 不能转换为 {col_type} 类型")
                row_data[col_name] = value
            
            self.data.append(row_data)
            # 更新索引
            for col, index in self.indexes.items():
                index.insert(row_data[col], row_index)

    def select(self, columns: Optional[List[str]] = None, conditions: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """查询数据"""
        result = []
        
        if conditions and 'column' in conditions and 'value' in conditions:
            column = conditions['column']
            value = conditions['value']
            if column in self.indexes:
                # 使用索引查找
                index_list = self.indexes[column]
                # 二分查找
                low, high = 0, len(index_list) - 1
                while low <= high:
                    mid = (low + high) // 2
                    if index_list[mid][0] < value:
                        low = mid + 1
                    elif index_list[mid][0] > value:
                        high = mid - 1
                    else:
                        # 找到匹配项
                        result.append(self.data[index_list[mid][1]])
                        break
            else:
                # 全表扫描
                result = [row for row in self.data if row[column] == value]
        else:
            # 全表扫描
            result = self.data

        return result

    def _process_aggregates(self, data: List[Dict[str, Any]], columns: Optional[List[str]],
                          group_by: Optional[List[str]], having: Optional[Dict],
                          aggregates: List[Dict]) -> List[Dict[str, Any]]:
        """处理聚合函数和分组"""
        if not group_by:
            # 不分组，直接计算聚合
            result = self._calculate_aggregates(data, aggregates)
            if columns:
                result.update({col: data[0][col] for col in columns if col in data[0]})
            return [result]

        # 按组分类数据
        groups = defaultdict(list)
        for row in data:
            key = tuple(row[col] for col in group_by)
            groups[key].append(row)

        # 计算每个组的结果
        result = []
        for key, group_data in groups.items():
            group_result = dict(zip(group_by, key))
            agg_results = self._calculate_aggregates(group_data, aggregates)
            group_result.update(agg_results)
            
            # 应用 HAVING 条件
            if having:
                if not self._match_having_conditions(group_result, having):
                    continue
            
            result.append(group_result)

        # 返回结果
        return result

    def _calculate_aggregates(self, data: List[Dict[str, Any]], 
                            aggregates: List[Dict]) -> Dict[str, Any]:
        """计算聚合函数"""
        result = {}
        for agg in aggregates:
            func = agg['function']
            arg = agg['argument']
            alias = agg['alias']
            
            if arg == '*':
                values = [1] * len(data)  # 用于 COUNT(*)
            else:
                values = [row[arg] for row in data]
            
            if func == 'COUNT':
                result[alias] = len(values)
            elif func == 'SUM':
                result[alias] = sum(values)
            elif func == 'AVG':
                result[alias] = sum(values) / len(values) if values else 0
            elif func == 'MAX':
                result[alias] = max(values) if values else None
            elif func == 'MIN':
                result[alias] = min(values) if values else None
            
        return result

    def _match_having_conditions(self, group_result: Dict[str, Any], 
                               having: Dict[str, Any]) -> bool:
        """检查是否满足 HAVING 条件"""
        ops = {
            '=': operator.eq,
            '!=': operator.ne,
            '>': operator.gt,
            '>=': operator.ge,
            '<': operator.lt,
            '<=': operator.le
        }
        
        for condition in having['conditions']:
            if condition['type'] == 'aggregate':
                col_name = f"{condition['function'].lower()}_{condition['argument']}"
                if col_name not in group_result:
                    return False
                if not ops[condition['operator']](group_result[col_name], condition['value']):
                    return False
            else:
                col = condition['column']
                if col not in group_result:
                    return False
                if not ops[condition['operator']](group_result[col], condition['value']):
                    return False
        
        return True

    def update(self, updates: Dict[str, Any], conditions: Optional[Dict] = None) -> int:
        """更新数据"""
        # 验证列名
        for col in updates:
            if col not in self.columns:
                raise Exception(f"未知的列名: {col}")
        
        # 找到匹配的行
        if conditions:
            rows = self._filter_data(self.data, conditions)
        else:
            rows = self.data
        
        # 更新数据
        count = 0
        for row in rows:
            for col, value in updates.items():
                row[col] = self._convert_value(value, self.columns[col])
            count += 1
        
        return count

    def delete(self, conditions: Optional[Dict] = None) -> int:
        """删除数据"""
        if conditions is None:
            count = len(self.data)
            self.data = []
            return count
        
        original_length = len(self.data)
        self.data = [row for row in self.data if not self._match_conditions(row, conditions)]
        return original_length - len(self.data)

    def _convert_value(self, value: Any, col_type: str) -> Any:
        """转换值的类型"""
        try:
            if col_type == 'INT':
                return int(value)
            elif col_type == 'FLOAT':
                return float(value)
            elif col_type == 'TEXT':
                return str(value)
            return value
        except ValueError:
            raise Exception(f"值 {value} 不能转换为 {col_type} 类型")

    def _match_conditions(self, row: Dict[str, Any], conditions: Dict) -> bool:
        """检查行是否匹配条件"""
        ops = {
            '=': operator.eq,
            '!=': operator.ne,
            '>': operator.gt,
            '>=': operator.ge,
            '<': operator.lt,
            '<=': operator.le
        }
        
        for condition in conditions['conditions']:
            col = condition['column']
            op = condition['operator']
            val = condition['value']
            
            if col not in row:
                raise Exception(f"未知的列名: {col}")
            
            if not ops[op](row[col], val):
                return False
        
        return True

    def _filter_data(self, data: List[Dict[str, Any]], conditions: Dict) -> List[Dict[str, Any]]:
        """根据条件筛选数据"""
        return [row for row in data if self._match_conditions(row, conditions)]
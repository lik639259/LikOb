from typing import List, Dict, Any, Tuple, Optional, Union
import operator

class Table:
    def __init__(self, name: str, columns: List[Tuple[str, str]]):
        self.name = name
        self.columns = {}
        self.data = []
        
        # 处理列定义
        for col_name, col_type in columns:
            self.columns[col_name] = col_type.upper()

    def insert(self, values: List[Any]) -> None:
        """插入数据"""
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

    def select(self, columns: Optional[List[str]] = None, conditions: Optional[Dict] = None,
              order_by: Optional[List[Tuple[str, str]]] = None) -> List[Dict[str, Any]]:
        """查询数据"""
        # 处理列选择
        if columns is None:
            columns = list(self.columns.keys())
        
        # 验证列名
        for col in columns:
            if col not in self.columns:
                raise Exception(f"未知的列名: {col}")
        
        # 筛选数据
        result = self.data
        if conditions:
            result = self._filter_data(result, conditions)
        
        # 排序
        if order_by:
            for col, direction in reversed(order_by):
                result = sorted(
                    result,
                    key=lambda x: x[col],
                    reverse=(direction == 'DESC')
                )
        
        # 投影列
        return [{col: row[col] for col in columns} for row in result]

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
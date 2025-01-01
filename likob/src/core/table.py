from typing import List, Dict, Any, Tuple, Set, Optional
from .index import Index

class Table:
    def __init__(self, name: str, columns: List[Tuple[str, str]]):
        self.name = name
        self.columns = {}
        self.data = []
        self.indexes = {}
        self.primary_key = None
        # 处理列定义
        self._process_columns(columns)

    def _process_columns(self, columns: List[Tuple[str, str]]) -> None:
        """处理列定义和约束"""
        for col_name, col_type in columns:
            # 检查是否是主键Y
            if 'PRIMARY KEY' in col_type.upper():
                if self.primary_key:
                    raise Exception("表只能有一个主键")
                self.primary_key = col_name
                # 为主键创建唯一索引
                self.indexes[col_name] = Index(self.name, col_name, is_unique=True)
                # 移除 PRIMARY KEY 标记
                col_type = col_type.upper().replace('PRIMARY KEY', '').strip()
            
            self.columns[col_name] = col_type

    def insert(self, values: List[Any]) -> None:
        """插入数据"""
        # 获取列名列表
        column_names = list(self.columns.keys())
        
        # 检查值的数量是否匹配列的数量
        if len(values) != len(column_names):
            raise Exception(f"值的数量 ({len(values)}) 与列的数量 ({len(column_names)}) 不匹配")
        
        # 创建数据字典
        row_data = dict(zip(column_names, values))
        
        # 添加数据
        self.data.append(row_data)

    def select(self, columns: List[str] = None) -> List[Dict[str, Any]]:
        """查询数据"""
        if columns is None:
            columns = list(self.columns.keys())
        
        result = []
        for row in self.data:
            result.append({col: row.get(col) for col in columns})
        return result
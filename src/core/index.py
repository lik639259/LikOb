from typing import Dict, List, Any, Set
from collections import defaultdict

class Index:
    def __init__(self, table_name: str, column_name: str, is_unique: bool = False):
        self.table_name = table_name
        self.column_name = column_name
        self.is_unique = is_unique
        # 使用字典存储索引，键是列值，值是行的集合
        self.index_map: Dict[Any, Set[int]] = defaultdict(set)

    def add(self, value: Any, row_id: int) -> None:
        """添加索引项"""
        if self.is_unique and value in self.index_map:
            raise Exception(f"唯一索引冲突: {self.column_name} = {value}")
        self.index_map[value].add(row_id)

    def remove(self, value: Any, row_id: int) -> None:
        """删除索引项"""
        if value in self.index_map:
            self.index_map[value].discard(row_id)
            if not self.index_map[value]:
                del self.index_map[value]

    def find(self, value: Any) -> Set[int]:
        """查找指定值的所有行ID"""
        return self.index_map.get(value, set())

    def find_range(self, start: Any = None, end: Any = None) -> Set[int]:
        """范围查询"""
        result = set()
        for value, row_ids in self.index_map.items():
            if ((start is None or value >= start) and 
                (end is None or value <= end)):
                result.update(row_ids)
        return result

    def clear(self) -> None:
        """清空索引"""
        self.index_map.clear()
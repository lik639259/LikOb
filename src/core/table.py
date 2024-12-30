from typing import List, Dict, Any, Tuple, Set, Optional
from .index import Index

class Table:
    def __init__(self, name: str, columns: List[Tuple[str, str, List[str]]]):
        self.name = name
        # 存储列定义：{列名: (类型, 约束)}
        self.columns: Dict[str, Tuple[str, List[str]]] = {}
        self.data = []
        self.indexes: Dict[str, Index] = {}
        self.primary_key: Optional[str] = None
        self.foreign_keys: Dict[str, Tuple[str, str]] = {}  # {列名: (引用表, 引用列)}
        
        # 处理列定义和约束
        self._process_columns(columns)

    def _process_columns(self, columns: List[Tuple[str, str, List[str]]]) -> None:
        """处理列定义和约束"""
        for col_name, col_type, constraints in columns:
            self.columns[col_name] = (col_type, constraints)
            
            # 处理主键约束
            if 'PRIMARY KEY' in constraints:
                if self.primary_key:
                    raise Exception("表只能有一个主键")
                self.primary_key = col_name
                # 为主键创建唯一索引
                self.indexes[col_name] = Index(self.name, col_name, is_unique=True)
            
            # 处理外键约束
            for constraint in constraints:
                if constraint.startswith('REFERENCES'):
                    # 格式: REFERENCES table_name(column_name)
                    parts = constraint[11:-1].split('(')  # 去掉 REFERENCES 和括号
                    ref_table = parts[0].strip()
                    ref_column = parts[1].strip()
                    self.foreign_keys[col_name] = (ref_table, ref_column)

    def insert(self, row_data: Dict[str, Any]) -> bool:
        """插入数据"""
        # 验证数据完整性
        self._validate_constraints(row_data)
        
        # 验证数据类型
        for col_name, value in row_data.items():
            if col_name not in self.columns:
                raise Exception(f"未知的列: {col_name}")
            if not self._validate_type(value, self.columns[col_name][0]):
                raise Exception(f"列 {col_name} 的类型不匹配")
        
        # 检查索引约束
        row_id = len(self.data)
        for col_name, index in self.indexes.items():
            if col_name in row_data:
                try:
                    index.add(row_data[col_name], row_id)
                except Exception as e:
                    # 回滚已添加的索引
                    self._rollback_indexes(row_data, row_id, col_name)
                    raise e
        
        # 插入数据
        self.data.append(row_data)
        return True

    def _validate_constraints(self, row_data: Dict[str, Any]) -> None:
        """验证所有约束"""
        # 验证主键
        if self.primary_key:
            if self.primary_key not in row_data:
                raise Exception(f"缺少主键值: {self.primary_key}")
            if row_data[self.primary_key] is None:
                raise Exception("主键不能为NULL")
        
        # 验证外键
        for col_name, (ref_table, ref_col) in self.foreign_keys.items():
            if col_name in row_data and row_data[col_name] is not None:
                ref_table_obj = self.db.get_table(ref_table)
                found = False
                for ref_row in ref_table_obj.data:
                    if ref_row[ref_col] == row_data[col_name]:
                        found = True
                        break
                if not found:
                    raise Exception(f"外键约束失败: {col_name} = {row_data[col_name]}")
        
        # 验证NOT NULL约束
        for col_name, (_, constraints) in self.columns.items():
            if 'NOT NULL' in constraints:
                if col_name not in row_data or row_data[col_name] is None:
                    raise Exception(f"列 {col_name} 不能为NULL")

    def _rollback_indexes(self, row_data: Dict[str, Any], row_id: int, failed_col: str) -> None:
        """回滚索引操作"""
        for col_name, index in self.indexes.items():
            if col_name in row_data and col_name != failed_col:
                index.remove(row_data[col_name], row_id)

    def update(self, row_ids: Set[int], new_values: Dict[str, Any]) -> None:
        """更新数据"""
        # 验证新值是否违反约束
        for row_id in row_ids:
            if 0 <= row_id < len(self.data):
                test_row = self.data[row_id].copy()
                test_row.update(new_values)
                self._validate_constraints(test_row)
        
        # 执行更新
        super().update(row_ids, new_values)

    def delete(self, row_ids: Set[int]) -> None:
        """删除数据"""
        # 检查是否有其他表的外键引用这些行
        for row_id in row_ids:
            if 0 <= row_id < len(self.data):
                row = self.data[row_id]
                self._check_foreign_key_references(row)
        
        # 执行删除
        super().delete(row_ids)

    def _check_foreign_key_references(self, row: Dict[str, Any]) -> None:
        """检查是否有外键引用"""
        if self.primary_key and self.primary_key in row:
            pk_value = row[self.primary_key]
            # 检查所有表的外键引用
            for table in self.db.tables.values():
                for col_name, (ref_table, ref_col) in table.foreign_keys.items():
                    if ref_table == self.name and ref_col == self.primary_key:
                        # 检查是否有引用
                        for ref_row in table.data:
                            if ref_row[col_name] == pk_value:
                                raise Exception(f"无法删除：存在外键引用 {table.name}.{col_name}")

    def create_index(self, column_name: str, is_unique: bool = False) -> None:
        """创建索引"""
        if column_name not in self.columns:
            raise Exception(f"列 {column_name} 不存在")
        
        if column_name in self.indexes:
            raise Exception(f"列 {column_name} 已经有索引")
        
        # 创建新索引
        index = Index(self.name, column_name, is_unique)
        
        # 为现有数据建立索引
        for row_id, row in enumerate(self.data):
            try:
                index.add(row[column_name], row_id)
            except Exception as e:
                # 如果建立索引失败（如唯一性冲突），回滚索引
                index.clear()
                raise Exception(f"创建索引失败: {str(e)}")
        
        self.indexes[column_name] = index
        return f"在表 {self.name} 的列 {column_name} 上创建索引成功"

    def drop_index(self, column_name: str) -> None:
        """删除索引"""
        if column_name not in self.indexes:
            raise Exception(f"列 {column_name} 没有索引")
        del self.indexes[column_name]

    def find_by_index(self, column_name: str, value: Any) -> List[Dict[str, Any]]:
        """使用索引查找数据"""
        if column_name not in self.indexes:
            raise Exception(f"列 {column_name} 没有索引")
        
        row_ids = self.indexes[column_name].find(value)
        return [self.data[row_id] for row_id in row_ids]
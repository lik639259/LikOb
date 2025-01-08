from typing import List, Dict, Any
from enum import Enum

class IsolationLevel(Enum):
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"

class Transaction:
    def __init__(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED):
        self.operations: List[Dict[str, Any]] = []
        self.is_active = True
        self.isolation_level = isolation_level

    def add_operation(self, operation: Dict[str, Any]):
        if self.is_active:
            self.operations.append(operation)

    def commit(self):
        if not self.is_active:
            raise Exception("Transaction is already committed or rolled back.")
        self.is_active = False
        return self.operations

    def rollback(self):
        if not self.is_active:
            raise Exception("Transaction is already committed or rolled back.")
        self.is_active = False
        self.operations.clear()  # 清空操作

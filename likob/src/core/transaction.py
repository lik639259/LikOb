from typing import List, Dict, Any

class Transaction:
    def __init__(self):
        self.operations: List[Dict[str, Any]] = []
        self.is_active = True

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

from typing import Dict, Any, Optional, List

class QueryExecutor:
    def __init__(self, db):
        self.db = db

    def execute(self, parsed_sql: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """执行解析后的SQL语句"""
        command = parsed_sql['command']
        
        if command == 'CREATE':
            self.db.create_table(parsed_sql['table'], parsed_sql['columns'])
            return [{'message': f"表 '{parsed_sql['table']}' 创建成功。"}]
        
        elif command == 'INSERT':
            table = self.db.get_table(parsed_sql['table'])
            table.insert(parsed_sql['values'])
            return [{'message': '1 行插入成功。'}]
        
        elif command == 'SELECT':
            if 'join' in parsed_sql:
                table1 = self.db.get_table(parsed_sql['table'])
                table2 = self.db.get_table(parsed_sql['join']['table'])
                on_column = parsed_sql['join']['on']
                return table1.join(table2, on_column)
            else:
                table = self.db.get_table(parsed_sql['table'])
                return table.select(
                    columns=parsed_sql['columns'],
                    conditions=parsed_sql.get('where')
                )
        
        elif command == 'UPDATE':
            operation = {'command': 'UPDATE', 'table': parsed_sql['table'], 'updates': parsed_sql['updates'], 'where': parsed_sql.get('where')}
            self._add_to_transaction(operation)
            table = self.db.get_table(parsed_sql['table'])
            count = table.update(
                updates=parsed_sql['updates'],
                conditions=parsed_sql.get('where')
            )
            return [{'message': f"{count} rows updated"}]
        
        elif command == 'DELETE':
            operation = {'command': 'DELETE', 'table': parsed_sql['table'], 'where': parsed_sql.get('where')}
            self._add_to_transaction(operation)
            table = self.db.get_table(parsed_sql['table'])
            count = table.delete(conditions=parsed_sql.get('where'))
            return [{'message': f"{count} rows deleted"}]
        
        elif command == 'JOIN':
            return self._execute_join(parsed_sql)
        
        raise Exception(f"不支持的命令: {command}")

    def _add_to_transaction(self, operation: Dict[str, Any]):
        """将操作添加到当前事务"""
        if self.db.current_transaction is not None:
            self.db.current_transaction.add_operation(operation)

    def _execute_join(self, parsed_sql: Dict[str, Any]) -> List[Dict[str, Any]]:
        table1 = self.db.get_table(parsed_sql['table1'])
        table2 = self.db.get_table(parsed_sql['table2'])
        # 这里可以实现 JOIN 逻辑
        # ...
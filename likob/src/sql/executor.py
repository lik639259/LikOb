from typing import Dict, Any, Optional, List

class QueryExecutor:
    def __init__(self, db):
        self.db = db

    def execute(self, parsed_sql: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """执行解析后的SQL语句"""
        command = parsed_sql['command']
        
        if command == 'CREATE':
            self.db.create_table(parsed_sql['table'], parsed_sql['columns'])
            return [{'message': f"Table '{parsed_sql['table']}' created successfully"}]
        
        elif command == 'INSERT':
            table = self.db.get_table(parsed_sql['table'])
            table.insert(parsed_sql['values'])
            return [{'message': '1 row inserted'}]
        
        elif command == 'SELECT':
            table = self.db.get_table(parsed_sql['table'])
            return table.select(
                columns=parsed_sql['columns'],
                conditions=parsed_sql.get('where'),
                order_by=parsed_sql.get('order_by')
            )
        
        elif command == 'UPDATE':
            table = self.db.get_table(parsed_sql['table'])
            count = table.update(
                updates=parsed_sql['updates'],
                conditions=parsed_sql.get('where')
            )
            return [{'message': f"{count} rows updated"}]
        
        elif command == 'DELETE':
            table = self.db.get_table(parsed_sql['table'])
            count = table.delete(conditions=parsed_sql.get('where'))
            return [{'message': f"{count} rows deleted"}]
        
        raise Exception(f"不支持的命令: {command}")
import cmd
from .src.core.database import SimpleDB
from typing import List, Dict, Any

class LikObShell(cmd.Cmd):
    intro = 'Welcome to LikOb Database Shell. Type help or ? to list commands.\n'
    prompt = 'LikOb> '

    def __init__(self):
        super().__init__()
        try:
            self.db = SimpleDB()
        except Exception as e:
            print(f"初始化数据库失败: {str(e)}")
            self.db = None

    def do_exit(self, arg):
        """退出程序"""
        print("\nGoodbye!")
        return True

    def do_quit(self, arg):
        """退出程序"""
        return self.do_exit(arg)

    def default(self, line):
        """处理SQL语句"""
        if not line.strip():
            return

        try:
            if not self.db:
                self.db = SimpleDB()
            
            result = self.db.execute(line)
            if result is not None:
                self._print_result(result)
        except Exception as e:
            print(f"\n错误: {str(e)}")
            # 不返回 True，这样就不会退出
        return False

    def _print_result(self, result: List[Dict[str, Any]]) -> None:
        """格式化输出结果"""
        if not result:
            print("\nEmpty set")
            return
        
        # 如果结果包含消息，直接打印
        if len(result) == 1 and 'message' in result[0]:
            print(f"\n{result[0]['message']}")
            return

        # 获取列名
        columns = list(result[0].keys())
        
        # 计算每列的最大宽度
        widths = {col: len(str(col)) for col in columns}
        for row in result:
            for col in columns:
                val = str(row[col])
                # 考虑中文字符的宽度
                width = sum(2 if ord(c) > 127 else 1 for c in val)
                widths[col] = max(widths[col], width)

        # 创建分隔线
        separator = '+' + '+'.join('-' * (widths[col] + 2) for col in columns) + '+'
        
        # 打印表头
        print('\n' + separator)
        header = '|'
        for col in columns:
            header += f" {col:{widths[col]}} |"
        print(header)
        print(separator)
        
        # 打印数据
        for row in result:
            line = '|'
            for col in columns:
                val = str(row[col])
                # 计算实际显示宽度
                display_width = sum(2 if ord(c) > 127 else 1 for c in val)
                padding = widths[col] - display_width
                line += f" {val}{' ' * padding} |"
            print(line)
        
        print(separator)
        print(f"\n{len(result)} {'row' if len(result) == 1 else 'rows'} in set\n")

    def emptyline(self):
        """处理空行"""
        pass

    def do_help(self, arg):
        """显示帮助信息"""
        print("\nAvailable commands:")
        print("  SELECT - 查询数据")
        print("  INSERT - 插入数据")
        print("  UPDATE - 更新数据")
        print("  DELETE - 删除数据")
        print("  CREATE - 创建表")
        print("  exit/quit - 退出程序")
        print("  help - 显示此帮助信息")
        print("\nExample:")
        print("  CREATE TABLE users (id INT, name TEXT);")
        print("  INSERT INTO users VALUES (1, 'Alice');")
        print("  SELECT * FROM users;")
        print()

    def do_begin(self, arg):
        """开始一个新事务"""
        try:
            self.db.begin_transaction()
            print("事务已开始。")
        except Exception as e:
            print(f"错误: {str(e)}")

    def do_commit(self, arg):
        """提交当前事务"""
        try:
            self.db.commit_transaction()
            print("事务已提交。")
        except Exception as e:
            print(f"错误: {str(e)}")

    def do_rollback(self, arg):
        """回滚当前事务"""
        try:
            self.db.rollback_transaction()
            print("事务已回滚。")
        except Exception as e:
            print(f"错误: {str(e)}")

def main():
    try:
        LikObShell().cmdloop()
    except KeyboardInterrupt:
        print("\nGoodbye!")
    except Exception as e:
        print(f"\nFatal error: {str(e)}")
        print("Database shell terminated.")

if __name__ == '__main__':
    main()
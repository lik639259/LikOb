import cmd
from .src.core.database import SimpleDB
from typing import List, Dict, Any

class LikObShell(cmd.Cmd):
    intro = 'Welcome to LikOb Database Shell. Type help or ? to list commands.\n'
    prompt = 'LikOb> '

    def __init__(self):
        super().__init__()
        self.db = SimpleDB()

    def do_exit(self, arg):
        """退出程序"""
        print("Goodbye!")
        return True

    def do_quit(self, arg):
        """退出程序"""
        return self.do_exit(arg)

    def default(self, line):
        """处理SQL语句"""
        if not line.strip():
            return
            
        try:
            result = self.db.execute(line)
            if result is not None:
                if isinstance(result, list):
                    self._print_result(result)
                else:
                    print(result)
        except Exception as e:
            print(f"错误: {str(e)}")
            return False  # 继续运行，不退出

    def _print_result(self, result: List[Dict[str, Any]]) -> None:
        """格式化输出结果"""
        if not result:
            print("Empty set")
            return
        
        # 如果结果包含消息，直接打印
        if len(result) == 1 and 'message' in result[0]:
            print(result[0]['message'])
            return
        
        # 获取列名
        columns = list(result[0].keys())
        
        # 计算每列的最大宽度
        widths = {col: len(str(col)) for col in columns}
        for row in result:
            for col in columns:
                widths[col] = max(widths[col], len(str(row[col])))
        
        # 打印表头
        separator = '+' + '+'.join('-' * (widths[col] + 2) for col in columns) + '+'
        print(separator)
        print('| ' + ' | '.join(f"{col:{widths[col]}}" for col in columns) + ' |')
        print(separator)
        
        # 打印数据
        for row in result:
            print('| ' + ' | '.join(f"{str(row[col]):{widths[col]}}" for col in columns) + ' |')
        print(separator)
        print(f"\n{len(result)} rows in set")

    def emptyline(self):
        """处理空行"""
        pass

def main():
    try:
        LikObShell().cmdloop()
    except KeyboardInterrupt:
        print("\nGoodbye!")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        print("Database shell terminated.")

if __name__ == '__main__':
    main()
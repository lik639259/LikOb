import cmd
from .core.database import SimpleDB

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

    def do_reset(self, arg):
        """重置数据库实例"""
        self.db = SimpleDB()
        print("数据库已重置")

    def default(self, line):
        """处理SQL语句"""
        try:
            result = self.db.execute(line)
            if result is not None:
                print(result)
        except Exception as e:
            print(f"错误: {str(e)}")

    def emptyline(self):
        """空行处理"""
        pass

def main():
    LikObShell().cmdloop()

if __name__ == '__main__':
    main()
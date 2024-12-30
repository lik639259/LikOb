from src.core.database import SimpleDB

def main():
    db = SimpleDB()
    print("简易数据库系统 v0.1")
    print("输入 'exit' 退出")
    print("输入 'help' 查看帮助")
    
    while True:
        try:
            sql = input("SQL> ").strip()
            if sql.lower() == 'exit':
                break
            elif sql.lower() == 'help':
                show_help()
            elif sql:
                result = db.execute(sql)
                print(result)
        except Exception as e:
            print(f"错误: {str(e)}")

def show_help():
    print("""
支持的SQL命令:
1. CREATE TABLE 表名 (列名1 类型1, 列名2 类型2, ...)
   例: CREATE TABLE users (id INT, name TEXT, age INT)

2. INSERT INTO 表名 VALUES (值1, 值2, ...)
   例: INSERT INTO users VALUES (1, 'Alice', 20)

3. SELECT 列名1, 列名2 FROM 表名 [WHERE 条件]
   例: SELECT * FROM users
   例: SELECT name, age FROM users WHERE age > 20

4. DELETE FROM 表名 [WHERE 条件]
   例: DELETE FROM users WHERE age < 20
    """)

if __name__ == "__main__":
    main()
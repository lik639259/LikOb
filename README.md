# LikOb

一个用Python实现的简单关系型数据库（当然，它会变得越来越不简单！），用于学习数据库原理。

## 功能特性

- 基本的 SQL 语句支持（CREATE, SELECT, INSERT, DELETE）
- 复杂的 SQL 语句支持 （聚合函数, UNION，CASE...）
- 基本查询（WHERE, LIMIT, DISTINCT, ORDER BY）
- 连接支持（JOIN等各种子查询与连接）
- 数据持久化 （支持数据库以json文件导入和导出）
- 类型检查
- 事务支持（BEGIN, COMMIT, ROLLBACK）
- 索引支持以优化查询性能

## 安装
```bash
pip install likob
```
如果无法安装，可能是版本冲突问题，可以安装具体的版本
```
pip install likob==0.1.2
```

## 源码下载
`````
git clone https://github.com/lik639259/LikOb.git
cd LikOb
`````

## 源码使用方法

### 作为包导入你的代码

```python
import likob
`````````

创建数据库实例
```
db = likob.create_database()
```

执行SQL语句
```
db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)")
db.execute("INSERT INTO users VALUES (1, 'Alice')")
result = db.execute("SELECT * FROM users")
print(result)
```

### 启动shell程序

如果你已经安装了likob
```
likob
```

或者运行根目录下的main.py文件
```
python main.py
```

启动后可以看到这样的界面
````
Welcome to LikOb Database Shell. Type help or ? to list commands.
LikOb> 
````

## 贡献

欢迎任何形式的贡献！如果你有建议或发现了问题，请提交 issue 或者 pull request。

## 许可证

该项目使用 MIT 许可证，详细信息请查看 LICENSE 文件。

### 注意事项

1. 这并不是LikOb的最终版本，事实上这个项目还有很多的优化空间，后续会随缘更新，添加新功能或优化代码性能
2. 本项目完全开源，请随意下载并使用它
3. 本项目只是一个mini数据库，其性能远逊于MySQL、sqlite等传统数据库，不适合用于大型开发或生产环境，更多的是用于数据库初学者进行交流学习，本项目使用python编写，各个文件、函数的功能简单易懂，且标有详细的注释，对初学者是相当友好的
4. 作者只是一名卑微的本科生(ㄒoㄒ)，所以这个数据库很难做到十全十美，欢迎（也恳求）大家的issue和pr。当然，只要能获得大家的star我就很满意了



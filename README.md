# LikOb

一个用Python实现的简单关系型数据库，用于学习数据库原理。

## 功能特性

- 基本的SQL语句支持（CREATE, SELECT, INSERT, DELETE）
- 条件查询（WHERE）
- 数据持久化
- 表格化输出
- 类型检查

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

### 注意事项

1. 这并不是LikOb的最终版本，事实上这个项目还有很多的优化空间，后续会随缘更新，添加新功能或优化代码性能
2. 本项目完全开源，请随意下载并使用它
3. 本项目只是一个mini数据库，其性能远逊于MySQL、sqlite等传统数据库，不适合用于大型开发或生产环境，更多的是用于数据库初学者进行交流学习，本项目使用python编写，各个文件、函数的功能简单易懂，且标有详细的注释，对初学者是相当友好的
4. 作者只是一名卑微的本科生(ㄒoㄒ)，所有这个数据库很难做到十全十美，欢迎（也恳求）大家的issue和pr。当然，只要能获得大家的star我就很满意了



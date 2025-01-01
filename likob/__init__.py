from .src.core.database import SimpleDB

def create_database():
    """创建一个新的数据库实例"""
    return SimpleDB()

# 导出主要的类和函数
__all__ = ['create_database', 'SimpleDB']
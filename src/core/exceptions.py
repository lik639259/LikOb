class SimpleDBError(Exception):
    """基础异常类"""
    pass

class TableError(SimpleDBError):
    """表格相关错误"""
    pass

class SQLParseError(SimpleDBError):
    """SQL解析错误"""
    pass

class StorageError(SimpleDBError):
    """存储相关错误"""
    pass

class TypeError(SimpleDBError):
    """数据类型错误"""
    pass
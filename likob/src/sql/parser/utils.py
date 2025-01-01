from typing import Any
from ...core.exceptions import SQLParseError

def parse_value(value: str) -> Any:
    """解析SQL值"""
    value = value.strip()
    
    # 字符串
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    
    # NULL
    if value.lower() == 'null':
        return None
    
    # 布尔值
    if value.lower() == 'true':
        return True
    if value.lower() == 'false':
        return False
    
    # 数字
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            raise SQLParseError(f"无法解析的值: {value}")
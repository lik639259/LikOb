from typing import Dict, Any, List, Union
from ...core.exceptions import SQLParseError
from .utils import parse_value

class ConditionParser:
    """SQL条件解析器，用于解析WHERE和HAVING子句"""
    
    def __init__(self):
        # 支持的操作符优先级
        self.operators_priority = {
            'OR': 1,
            'AND': 2,
            'NOT': 3,
            'IN': 4,
            'NOT IN': 4,
            'LIKE': 4,
            'BETWEEN': 4,
            '=': 5,
            '!=': 5,
            '>': 5,
            '<': 5,
            '>=': 5,
            '<=': 5,
            'IS NULL': 5,
            'IS NOT NULL': 5
        }

    def parse_condition(self, condition: str) -> Dict:
        """解析条件表达式
        返回解析后的条件字典
        """
        condition = condition.strip()
        
        # 处理括号
        if condition.startswith('(') and condition.endswith(')'):
            return self.parse_condition(condition[1:-1].strip())
        
        # 处理AND/OR
        if ' and ' in condition.lower():
            return self._parse_logical_operation(condition, 'AND')
        if ' or ' in condition.lower():
            return self._parse_logical_operation(condition, 'OR')
        
        # 处理NOT
        if condition.lower().startswith('not '):
            return {
                'operator': 'NOT',
                'condition': self.parse_condition(condition[4:])
            }
        
        # 处理其他条件类型
        return self._parse_simple_condition(condition)

    def _parse_logical_operation(self, condition: str, operator: str) -> Dict:
        """解析逻辑操作（AND/OR）"""
        parts = self._split_logical_parts(condition, operator.lower())
        return {
            'operator': operator,
            'conditions': [self.parse_condition(part.strip()) for part in parts]
        }

    def _split_logical_parts(self, condition: str, operator: str) -> List[str]:
        """智能分割逻辑表达式，考虑括号嵌套"""
        parts = []
        current = ''
        paren_count = 0
        in_string = False
        
        for char in condition:
            if char == "'" and not in_string:
                in_string = True
                current += char
            elif char == "'" and in_string:
                in_string = False
                current += char
            elif char == '(' and not in_string:
                paren_count += 1
                current += char
            elif char == ')' and not in_string:
                paren_count -= 1
                current += char
            else:
                current += char
            
            # 检查是否找到完整的逻辑操作符
            if not in_string and paren_count == 0:
                if current.lower().endswith(f' {operator} '):
                    parts.append(current[:-len(operator)-2].strip())
                    current = ''
        
        if current:
            parts.append(current.strip())
        
        return parts

    def _parse_simple_condition(self, condition: str) -> Dict:
        """解析简单条件"""
        condition = condition.strip()
        
        # 处理 IS NULL / IS NOT NULL
        if ' is ' in condition.lower():
            return self._parse_is_null(condition)
        
        # 处理 IN / NOT IN
        if ' in ' in condition.lower():
            return self._parse_in_condition(condition)
        
        # 处理 BETWEEN
        if ' between ' in condition.lower():
            return self._parse_between(condition)
        
        # 处理 LIKE
        if ' like ' in condition.lower():
            return self._parse_like(condition)
        
        # 处理基本比较操作
        return self._parse_comparison(condition)

    def _parse_is_null(self, condition: str) -> Dict:
        """解析 IS NULL / IS NOT NULL 条件"""
        parts = condition.lower().split(' is ')
        column = parts[0].strip()
        is_not = ' not ' in parts[1]
        
        return {
            'operator': 'IS NOT NULL' if is_not else 'IS NULL',
            'column': column
        }

    def _parse_in_condition(self, condition: str) -> Dict:
        """解析 IN / NOT IN 条件"""
        not_in = ' not in ' in condition.lower()
        parts = condition.split(' not in ' if not_in else ' in ')
        column = parts[0].strip()
        
        # 解析值列表
        values_str = parts[1].strip()
        if not (values_str.startswith('(') and values_str.endswith(')')):
            raise SQLParseError("IN 子句必须包含在括号中")
        
        values_str = values_str[1:-1]
        values = [parse_value(v.strip()) for v in values_str.split(',')]
        
        return {
            'operator': 'NOT IN' if not_in else 'IN',
            'column': column,
            'values': values
        }

    def _parse_between(self, condition: str) -> Dict:
        """解析 BETWEEN 条件"""
        parts = condition.lower().split(' between ')
        if len(parts) != 2:
            raise SQLParseError("BETWEEN 语法错误")
        
        column = parts[0].strip()
        range_parts = parts[1].split(' and ')
        if len(range_parts) != 2:
            raise SQLParseError("BETWEEN 必须包含 AND")
        
        return {
            'operator': 'BETWEEN',
            'column': column,
            'low': parse_value(range_parts[0].strip()),
            'high': parse_value(range_parts[1].strip())
        }

    def _parse_like(self, condition: str) -> Dict:
        """解析 LIKE 条件"""
        parts = condition.lower().split(' like ')
        if len(parts) != 2:
            raise SQLParseError("LIKE 语法错误")
        
        column = parts[0].strip()
        pattern = parts[1].strip()
        
        # 移除引号（如果存在）
        if pattern.startswith("'") and pattern.endswith("'"):
            pattern = pattern[1:-1]
        
        return {
            'operator': 'LIKE',
            'column': column,
            'pattern': pattern
        }

    def _parse_comparison(self, condition: str) -> Dict:
        """解析基本比较操作（=, !=, >, <, >=, <=）"""
        for op in ['>=', '<=', '!=', '=', '>', '<']:
            if op in condition:
                parts = condition.split(op)
                if len(parts) != 2:
                    raise SQLParseError(f"比较操作符 {op} 语法错误")
                
                return {
                    'operator': op,
                    'column': parts[0].strip(),
                    'value': parse_value(parts[1].strip())
                }
        
        raise SQLParseError(f"不支持的条件: {condition}")
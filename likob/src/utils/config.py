from dataclasses import dataclass
from typing import Dict, Any
import json
import os

@dataclass
class DBConfig:
    """数据库配置类"""
    data_directory: str = "data"
    page_size: int = 4096
    cache_size: int = 1000
    log_level: str = "INFO"
    
    @classmethod
    def load_from_file(cls, filepath: str) -> "DBConfig":
        """从配置文件加载配置"""
        if not os.path.exists(filepath):
            return cls()
            
        with open(filepath, 'r') as f:
            config_dict = json.load(f)
            return cls(**config_dict)
    
    def save_to_file(self, filepath: str) -> None:
        """保存配置到文件"""
        with open(filepath, 'w') as f:
            json.dump(self.__dict__, f, indent=4)
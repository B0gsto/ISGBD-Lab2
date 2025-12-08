from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum


class DataType(Enum):
    INTEGER = "INTEGER"
    VARCHAR = "VARCHAR"
    DECIMAL = "DECIMAL"
    TIMESTAMP = "TIMESTAMP"
    TEXT = "TEXT"
    SERIAL = "SERIAL"


@dataclass
class Column:
    name: str
    data_type: DataType
    nullable: bool = True
    is_primary_key: bool = False
    
    def __str__(self) -> str:
        return self.name


@dataclass
class ColumnStats:
    distinct_count: int = 0
    null_fraction: float = 0.0
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    most_common_values: List[Any] = field(default_factory=list)
    most_common_freqs: List[float] = field(default_factory=list)


@dataclass
class Table:
    name: str
    columns: List[Column] = field(default_factory=list)
    row_count: int = 0
    avg_row_size: int = 100
    total_pages: int = 0
    
    def get_column(self, name: str) -> Optional[Column]:
        for col in self.columns:
            if col.name == name:
                return col
        return None
    
    def __str__(self) -> str:
        return self.name


@dataclass
class Index:
    name: str
    table_name: str
    columns: List[str]
    is_unique: bool = False
    is_primary: bool = False
    cardinality: int = 0
    pages: int = 0
    
    @property
    def is_composite(self) -> bool:
        return len(self.columns) > 1
    
    def covers_columns(self, cols: List[str]) -> bool:
        if len(cols) > len(self.columns):
            return False
        return self.columns[:len(cols)] == cols
    
    def __str__(self) -> str:
        cols = ", ".join(self.columns)
        return f"{self.name} ({cols})"


@dataclass
class TableStats:
    table: Table
    column_stats: Dict[str, ColumnStats] = field(default_factory=dict)
    indexes: List[Index] = field(default_factory=list)
    
    def get_selectivity(self, column: str, operator: str, value: Any) -> float:
        if column not in self.column_stats:
            return 0.1
        
        stats = self.column_stats[column]
        
        if operator == "=":
            if stats.distinct_count > 0:
                return 1.0 / stats.distinct_count
            return 0.01
        
        elif operator in ("<", "<=", ">", ">="):
            if stats.min_value is not None and stats.max_value is not None:
                try:
                    range_size = float(stats.max_value) - float(stats.min_value)
                    if range_size > 0:
                        if operator in ("<", "<="):
                            return (float(value) - float(stats.min_value)) / range_size
                        else:
                            return (float(stats.max_value) - float(value)) / range_size
                except (TypeError, ValueError):
                    pass
            return 0.33
        
        elif operator == "LIKE":
            if isinstance(value, str) and not value.startswith("%"):
                return 0.1
            return 0.5
        
        elif operator == "IN":
            if isinstance(value, (list, tuple)) and stats.distinct_count > 0:
                return min(1.0, len(value) / stats.distinct_count)
            return 0.1
        
        elif operator == "IS NULL":
            return stats.null_fraction
        
        elif operator == "IS NOT NULL":
            return 1.0 - stats.null_fraction
        
        return 0.1
    
    def find_best_index(self, columns: List[str], operators: List[str]) -> Optional[Index]:
        best_index = None
        best_score = 0
        
        for index in self.indexes:
            score = 0
            for i, col in enumerate(columns):
                if i < len(index.columns) and index.columns[i] == col:
                    if i < len(operators) and operators[i] in ("=", "IN"):
                        score += 2
                    elif i < len(operators) and operators[i] in ("<", "<=", ">", ">="):
                        score += 1
                        break
                else:
                    break
            
            if score > best_score:
                best_score = score
                best_index = index
        
        return best_index


@dataclass
class Schema:
    tables: Dict[str, Table] = field(default_factory=dict)
    table_stats: Dict[str, TableStats] = field(default_factory=dict)
    
    def add_table(self, table: Table, stats: Optional[TableStats] = None):
        self.tables[table.name] = table
        if stats:
            self.table_stats[table.name] = stats
        else:
            self.table_stats[table.name] = TableStats(table=table)
    
    def get_table(self, name: str) -> Optional[Table]:
        return self.tables.get(name)
    
    def get_stats(self, table_name: str) -> Optional[TableStats]:
        return self.table_stats.get(table_name)

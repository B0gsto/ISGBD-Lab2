from dataclasses import dataclass, field
from typing import List, Optional, Any
from enum import Enum


class Operator(Enum):
    EQ = "="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    LIKE = "LIKE"
    IN = "IN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    BETWEEN = "BETWEEN"


class JoinType(Enum):
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL JOIN"
    CROSS = "CROSS JOIN"


class LogicalOp(Enum):
    AND = "AND"
    OR = "OR"


@dataclass
class Predicate:
    table: Optional[str]
    column: str
    operator: str
    value: Any
    logical_op: LogicalOp = LogicalOp.AND
    
    def get_full_column(self) -> str:
        if self.table:
            return f"{self.table}.{self.column}"
        return self.column
    
    def __str__(self) -> str:
        col = self.get_full_column()
        if self.operator in ("IS NULL", "IS NOT NULL"):
            return f"{col} {self.operator}"
        elif self.operator == "IN":
            vals = ", ".join(repr(v) for v in self.value)
            return f"{col} IN ({vals})"
        elif self.operator == "BETWEEN":
            return f"{col} BETWEEN {self.value[0]} AND {self.value[1]}"
        else:
            return f"{col} {self.operator} {repr(self.value)}"


@dataclass
class JoinCondition:
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: JoinType = JoinType.INNER
    
    def __str__(self) -> str:
        return (f"{self.join_type.value} {self.right_table} "
                f"ON {self.left_table}.{self.left_column} = "
                f"{self.right_table}.{self.right_column}")


@dataclass
class TableReference:
    name: str
    alias: Optional[str] = None
    
    def get_ref(self) -> str:
        return self.alias if self.alias else self.name
    
    def __str__(self) -> str:
        if self.alias:
            return f"{self.name} AS {self.alias}"
        return self.name


@dataclass
class Query:
    select_columns: List[str] = field(default_factory=list)
    tables: List[TableReference] = field(default_factory=list)
    joins: List[JoinCondition] = field(default_factory=list)
    predicates: List[Predicate] = field(default_factory=list)
    group_by_columns: List[str] = field(default_factory=list)
    order_by_columns: List[tuple] = field(default_factory=list)
    limit_value: Optional[int] = None
    offset_value: Optional[int] = None
    
    def select(self, *columns: str) -> 'Query':
        self.select_columns.extend(columns)
        return self
    
    def from_table(self, table: str, alias: Optional[str] = None) -> 'Query':
        self.tables.append(TableReference(table, alias))
        return self
    
    def join(self, table: str, alias: Optional[str], 
             left_col: str, right_col: str,
             join_type: JoinType = JoinType.INNER) -> 'Query':
        if "." in left_col:
            left_table, left_column = left_col.split(".", 1)
        else:
            left_table = self.tables[0].get_ref() if self.tables else ""
            left_column = left_col
        
        if "." in right_col:
            right_table, right_column = right_col.split(".", 1)
        else:
            right_table = alias if alias else table
            right_column = right_col
        
        self.joins.append(JoinCondition(
            left_table=left_table,
            left_column=left_column,
            right_table=alias if alias else table,
            right_column=right_column,
            join_type=join_type
        ))
        
        self.tables.append(TableReference(table, alias))
        return self
    
    def where(self, column: str, operator: str, value: Any = None,
              logical_op: LogicalOp = LogicalOp.AND) -> 'Query':
        table = None
        col = column
        
        if "." in column:
            table, col = column.split(".", 1)
        
        self.predicates.append(Predicate(
            table=table,
            column=col,
            operator=operator,
            value=value,
            logical_op=logical_op
        ))
        return self
    
    def and_where(self, column: str, operator: str, value: Any = None) -> 'Query':
        return self.where(column, operator, value, LogicalOp.AND)
    
    def or_where(self, column: str, operator: str, value: Any = None) -> 'Query':
        return self.where(column, operator, value, LogicalOp.OR)
    
    def group_by(self, *columns: str) -> 'Query':
        self.group_by_columns.extend(columns)
        return self
    
    def order_by(self, column: str, desc: bool = False) -> 'Query':
        self.order_by_columns.append((column, desc))
        return self
    
    def limit(self, n: int) -> 'Query':
        self.limit_value = n
        return self
    
    def offset(self, n: int) -> 'Query':
        self.offset_value = n
        return self
    
    def get_table_name(self, ref: str) -> str:
        for table in self.tables:
            if table.get_ref() == ref or table.name == ref:
                return table.name
        return ref
    
    def get_predicates_for_table(self, table_ref: str) -> List[Predicate]:
        result = []
        table_name = self.get_table_name(table_ref)
        
        for pred in self.predicates:
            if pred.table == table_ref or pred.table == table_name:
                result.append(pred)
            elif pred.table is None:
                result.append(pred)
        
        return result
    
    def to_sql(self) -> str:
        parts = []
        
        cols = ", ".join(self.select_columns) if self.select_columns else "*"
        parts.append(f"SELECT {cols}")
        
        if self.tables:
            parts.append(f"FROM {self.tables[0]}")
        
        for join in self.joins:
            parts.append(f"  {join}")
        
        if self.predicates:
            where_parts = []
            for i, pred in enumerate(self.predicates):
                if i == 0:
                    where_parts.append(str(pred))
                else:
                    where_parts.append(f"{pred.logical_op.value} {pred}")
            parts.append("WHERE " + " ".join(where_parts))
        
        if self.group_by_columns:
            parts.append("GROUP BY " + ", ".join(self.group_by_columns))
        
        if self.order_by_columns:
            order_parts = []
            for col, desc in self.order_by_columns:
                order_parts.append(f"{col} DESC" if desc else col)
            parts.append("ORDER BY " + ", ".join(order_parts))
        
        if self.limit_value is not None:
            parts.append(f"LIMIT {self.limit_value}")
        if self.offset_value is not None:
            parts.append(f"OFFSET {self.offset_value}")
        
        return "\n".join(parts)
    
    def __str__(self) -> str:
        return self.to_sql()

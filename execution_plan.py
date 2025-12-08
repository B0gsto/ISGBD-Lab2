from dataclasses import dataclass, field
from typing import List, Optional, Any, Dict
from enum import Enum


class PhysicalOperator(Enum):
    SEQ_SCAN = "Seq Scan"
    INDEX_SCAN = "Index Scan"
    INDEX_ONLY_SCAN = "Index Only Scan"
    BITMAP_SCAN = "Bitmap Heap Scan"
    NESTED_LOOP = "Nested Loop"
    HASH_JOIN = "Hash Join"
    MERGE_JOIN = "Merge Join"
    SORT = "Sort"
    FILTER = "Filter"
    HASH = "Hash"
    AGGREGATE = "Aggregate"
    LIMIT = "Limit"
    RESULT = "Result"
    

@dataclass
class PlanNode:
    operator: PhysicalOperator
    table: Optional[str] = None
    alias: Optional[str] = None
    index_name: Optional[str] = None
    startup_cost: float = 0.0
    total_cost: float = 0.0
    estimated_rows: int = 0
    actual_rows: Optional[int] = None
    width: int = 0
    filter_condition: Optional[str] = None
    join_condition: Optional[str] = None
    sort_keys: List[str] = field(default_factory=list)
    children: List['PlanNode'] = field(default_factory=list)
    extra_info: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def total_subtree_cost(self) -> float:
        cost = self.total_cost
        for child in self.children:
            cost = max(cost, child.total_subtree_cost)
        return cost
    
    def add_child(self, child: 'PlanNode') -> 'PlanNode':
        self.children.append(child)
        return self
    
    def format(self, indent: int = 0, show_costs: bool = True) -> str:
        prefix = "  " * indent
        arrow = "-> " if indent > 0 else ""
        
        parts = [f"{prefix}{arrow}{self.operator.value}"]
        
        if self.table:
            table_ref = self.alias if self.alias else self.table
            parts.append(f"on {table_ref}")
        
        if self.index_name:
            parts.append(f"using {self.index_name}")
        
        line = " ".join(parts)
        
        if show_costs:
            line += f"  (cost={self.startup_cost:.2f}..{self.total_cost:.2f}"
            line += f" rows={self.estimated_rows} width={self.width})"
        
        lines = [line]
        
        if self.filter_condition:
            lines.append(f"{prefix}     Filter: {self.filter_condition}")
        
        if self.join_condition:
            lines.append(f"{prefix}     Join Cond: {self.join_condition}")
        
        if self.sort_keys:
            lines.append(f"{prefix}     Sort Key: {', '.join(self.sort_keys)}")
        
        for key, value in self.extra_info.items():
            lines.append(f"{prefix}     {key}: {value}")
        
        for child in self.children:
            lines.append(child.format(indent + 1, show_costs))
        
        return "\n".join(lines)
    
    def __str__(self) -> str:
        return self.format()


@dataclass
class ExecutionPlan:
    root: PlanNode
    query_sql: str = ""
    planning_time_ms: float = 0.0
    is_optimized: bool = False
    optimization_notes: List[str] = field(default_factory=list)
    
    @property
    def total_cost(self) -> float:
        return self.root.total_cost
    
    @property
    def estimated_rows(self) -> int:
        return self.root.estimated_rows
    
    def format(self, verbose: bool = False) -> str:
        lines = []
        
        if self.query_sql:
            lines.append(f"Query: {self.query_sql[:100]}...")
            lines.append("")
        
        lines.append("QUERY PLAN")
        lines.append("-" * 60)
        lines.append(self.root.format())
        lines.append("-" * 60)
        lines.append(f"Total Cost: {self.total_cost:.2f}")
        lines.append(f"Estimated Rows: {self.estimated_rows}")
        
        if self.planning_time_ms > 0:
            lines.append(f"Planning Time: {self.planning_time_ms:.3f} ms")
        
        if verbose and self.optimization_notes:
            lines.append("")
            lines.append("Optimization Notes:")
            for note in self.optimization_notes:
                lines.append(f"  - {note}")
        
        return "\n".join(lines)
    
    def __str__(self) -> str:
        return self.format()


def compare_plans(plan1: ExecutionPlan, plan2: ExecutionPlan) -> str:
    lines = []
    
    lines.append("=" * 70)
    lines.append("PLAN COMPARISON")
    lines.append("=" * 70)
    
    lines.append("")
    lines.append("PLAN 1 (Before Optimization):")
    lines.append(plan1.root.format())
    lines.append(f"Cost: {plan1.total_cost:.2f}")
    
    lines.append("")
    lines.append("PLAN 2 (After Optimization):")
    lines.append(plan2.root.format())
    lines.append(f"Cost: {plan2.total_cost:.2f}")
    
    lines.append("")
    lines.append("-" * 70)
    
    cost_diff = plan1.total_cost - plan2.total_cost
    if cost_diff > 0:
        improvement = (cost_diff / plan1.total_cost) * 100
        lines.append(f"Cost Improvement: {cost_diff:.2f} ({improvement:.1f}% reduction)")
    elif cost_diff < 0:
        lines.append(f"Cost Increase: {abs(cost_diff):.2f} (optimization not beneficial)")
    else:
        lines.append("No cost difference")
    
    if plan2.optimization_notes:
        lines.append("")
        lines.append("Optimizations Applied:")
        for note in plan2.optimization_notes:
            lines.append(f"  ✓ {note}")
    
    return "\n".join(lines)

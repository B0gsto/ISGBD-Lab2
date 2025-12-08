from dataclasses import dataclass
from typing import Optional
from models import Table, Index, TableStats


PAGE_SIZE = 8192

SEQ_PAGE_COST = 1.0
RANDOM_PAGE_COST = 4.0
CPU_TUPLE_COST = 0.01
CPU_INDEX_COST = 0.005
CPU_OPERATOR_COST = 0.0025


@dataclass
class OperationCost:
    startup_cost: float = 0.0
    total_cost: float = 0.0
    rows: int = 0
    width: int = 0
    
    def __add__(self, other: 'OperationCost') -> 'OperationCost':
        return OperationCost(
            startup_cost=self.startup_cost + other.startup_cost,
            total_cost=self.total_cost + other.total_cost,
            rows=self.rows + other.rows,
            width=max(self.width, other.width)
        )
    
    def __str__(self) -> str:
        return f"cost={self.startup_cost:.2f}..{self.total_cost:.2f}, rows={self.rows}"


class CostModel:
    def __init__(self, 
                 seq_page_cost: float = SEQ_PAGE_COST,
                 random_page_cost: float = RANDOM_PAGE_COST,
                 cpu_tuple_cost: float = CPU_TUPLE_COST,
                 cpu_index_cost: float = CPU_INDEX_COST):
        self.seq_page_cost = seq_page_cost
        self.random_page_cost = random_page_cost
        self.cpu_tuple_cost = cpu_tuple_cost
        self.cpu_index_cost = cpu_index_cost
    
    def estimate_seq_scan(self, table: Table, selectivity: float = 1.0) -> OperationCost:
        pages = max(1, table.total_pages)
        rows = table.row_count
        
        io_cost = pages * self.seq_page_cost
        cpu_cost = rows * self.cpu_tuple_cost
        
        total = io_cost + cpu_cost
        output_rows = int(rows * selectivity)
        
        return OperationCost(
            startup_cost=0.0,
            total_cost=total,
            rows=output_rows,
            width=table.avg_row_size
        )
    
    def estimate_index_scan(self, table: Table, index: Index, 
                            selectivity: float) -> OperationCost:
        rows = table.row_count
        output_rows = max(1, int(rows * selectivity))
        
        if index.cardinality > 0:
            tree_height = max(1, int(2 + (index.cardinality ** 0.25)))
        else:
            tree_height = 3
        
        index_startup = tree_height * self.random_page_cost
        cost_per_tuple = self.random_page_cost + self.cpu_index_cost
        heap_cost = output_rows * self.random_page_cost * 0.5
        
        total = index_startup + (output_rows * cost_per_tuple) + heap_cost
        
        return OperationCost(
            startup_cost=index_startup,
            total_cost=total,
            rows=output_rows,
            width=table.avg_row_size
        )
    
    def estimate_index_only_scan(self, table: Table, index: Index,
                                  selectivity: float) -> OperationCost:
        rows = table.row_count
        output_rows = max(1, int(rows * selectivity))
        
        if index.cardinality > 0:
            tree_height = max(1, int(2 + (index.cardinality ** 0.25)))
        else:
            tree_height = 3
        
        index_startup = tree_height * self.random_page_cost
        cost_per_tuple = self.cpu_index_cost
        
        index_pages = max(1, index.pages)
        io_cost = min(output_rows, index_pages) * self.seq_page_cost
        
        total = index_startup + io_cost + (output_rows * cost_per_tuple)
        
        return OperationCost(
            startup_cost=index_startup,
            total_cost=total,
            rows=output_rows,
            width=50
        )
    
    def estimate_nested_loop_join(self, outer: OperationCost, 
                                   inner: OperationCost,
                                   inner_rescan_cost: float = 0.0) -> OperationCost:
        startup = outer.startup_cost + inner.startup_cost
        
        if inner_rescan_cost == 0:
            inner_rescan_cost = inner.total_cost * 0.9
        
        total = outer.total_cost + inner.total_cost
        if outer.rows > 1:
            total += (outer.rows - 1) * inner_rescan_cost
        
        cpu = outer.rows * inner.rows * CPU_OPERATOR_COST
        total += cpu
        
        output_rows = max(1, int(outer.rows * inner.rows * 0.1))
        
        return OperationCost(
            startup_cost=startup,
            total_cost=total,
            rows=output_rows,
            width=outer.width + inner.width
        )
    
    def estimate_hash_join(self, outer: OperationCost, 
                           inner: OperationCost,
                           join_selectivity: float = 0.1) -> OperationCost:
        startup = outer.startup_cost + inner.total_cost
        
        hash_build = inner.rows * self.cpu_tuple_cost * 5
        probe_cost = outer.total_cost + (outer.rows * self.cpu_tuple_cost * 2)
        
        total = startup + hash_build + probe_cost
        output_rows = max(1, int(outer.rows * inner.rows * join_selectivity))
        
        return OperationCost(
            startup_cost=startup,
            total_cost=total,
            rows=output_rows,
            width=outer.width + inner.width
        )
    
    def estimate_sort(self, input_cost: OperationCost, 
                      work_mem_kb: int = 4096) -> OperationCost:
        rows = input_cost.rows
        row_size = input_cost.width
        
        data_size_kb = (rows * row_size) / 1024
        in_memory = data_size_kb <= work_mem_kb
        
        if in_memory:
            import math
            comparisons = rows * max(1, math.log2(max(rows, 2)))
            sort_cost = comparisons * CPU_OPERATOR_COST * 2
        else:
            import math
            passes = max(1, math.ceil(math.log2(data_size_kb / work_mem_kb)))
            pages = data_size_kb / (PAGE_SIZE / 1024)
            sort_cost = passes * pages * self.seq_page_cost * 2
        
        startup = input_cost.total_cost + sort_cost
        
        return OperationCost(
            startup_cost=startup,
            total_cost=startup,
            rows=rows,
            width=input_cost.width
        )
    
    def estimate_filter(self, input_cost: OperationCost, 
                        selectivity: float) -> OperationCost:
        filter_cost = input_cost.rows * CPU_OPERATOR_COST
        output_rows = max(1, int(input_cost.rows * selectivity))
        
        return OperationCost(
            startup_cost=input_cost.startup_cost,
            total_cost=input_cost.total_cost + filter_cost,
            rows=output_rows,
            width=input_cost.width
        )
    
    def estimate_limit(self, input_cost: OperationCost, 
                       limit: int) -> OperationCost:
        if input_cost.rows <= limit:
            return input_cost
        
        fraction = limit / input_cost.rows
        
        return OperationCost(
            startup_cost=input_cost.startup_cost,
            total_cost=input_cost.startup_cost + 
                       (input_cost.total_cost - input_cost.startup_cost) * fraction,
            rows=limit,
            width=input_cost.width
        )
    
    def compare_scan_methods(self, table: Table, stats: TableStats,
                              predicates: list) -> dict:
        selectivity = 1.0
        predicate_columns = []
        predicate_operators = []
        
        for pred in predicates:
            col_sel = stats.get_selectivity(pred.column, pred.operator, pred.value)
            selectivity *= col_sel
            predicate_columns.append(pred.column)
            predicate_operators.append(pred.operator)
        
        results = {}
        
        seq_cost = self.estimate_seq_scan(table, selectivity)
        results['seq_scan'] = seq_cost
        
        best_index = stats.find_best_index(predicate_columns, predicate_operators)
        
        if best_index:
            idx_cost = self.estimate_index_scan(table, best_index, selectivity)
            results[f'index_scan_{best_index.name}'] = idx_cost
        
        return results

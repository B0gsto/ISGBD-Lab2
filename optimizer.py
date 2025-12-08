from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import time

from models import Schema, Table, TableStats, Index
from query import Query, Predicate, JoinCondition, JoinType
from cost_model import CostModel, OperationCost
from execution_plan import PlanNode, ExecutionPlan, PhysicalOperator


@dataclass
class JoinOrder:
    tables: List[str]
    estimated_cost: float
    intermediate_sizes: List[int]


class QueryOptimizer:
    def __init__(self, schema: Schema):
        self.schema = schema
        self.cost_model = CostModel()
        self.optimization_notes: List[str] = []
    
    def optimize(self, query: Query) -> ExecutionPlan:
        start_time = time.time()
        self.optimization_notes = []
        
        table_predicates = self._assign_predicates_to_tables(query)
        join_order = self._optimize_join_order(query, table_predicates)
        plan = self._build_physical_plan(query, table_predicates, join_order)
        plan = self._add_final_operators(plan, query)
        
        planning_time = (time.time() - start_time) * 1000
        
        return ExecutionPlan(
            root=plan,
            query_sql=query.to_sql(),
            planning_time_ms=planning_time,
            is_optimized=True,
            optimization_notes=self.optimization_notes.copy()
        )
    
    def build_naive_plan(self, query: Query) -> ExecutionPlan:
        start_time = time.time()
        
        if not query.tables:
            return ExecutionPlan(
                root=PlanNode(PhysicalOperator.RESULT),
                query_sql=query.to_sql()
            )
        
        first_table = query.tables[0]
        table = self.schema.get_table(first_table.name)
        stats = self.schema.get_stats(first_table.name)
        
        if not table:
            table = Table(name=first_table.name, row_count=1000)
        
        current_cost = self.cost_model.estimate_seq_scan(table)
        
        current_plan = PlanNode(
            operator=PhysicalOperator.SEQ_SCAN,
            table=first_table.name,
            alias=first_table.alias,
            startup_cost=current_cost.startup_cost,
            total_cost=current_cost.total_cost,
            estimated_rows=current_cost.rows,
            width=current_cost.width
        )
        
        if query.predicates:
            filter_cond = " AND ".join(str(p) for p in query.predicates)
            current_plan.filter_condition = filter_cond
        
        for i, join in enumerate(query.joins):
            joined_table_ref = query.tables[i + 1]
            joined_table = self.schema.get_table(joined_table_ref.name)
            
            if not joined_table:
                joined_table = Table(name=joined_table_ref.name, row_count=1000)
            
            inner_cost = self.cost_model.estimate_seq_scan(joined_table)
            
            inner_plan = PlanNode(
                operator=PhysicalOperator.SEQ_SCAN,
                table=joined_table_ref.name,
                alias=joined_table_ref.alias,
                startup_cost=inner_cost.startup_cost,
                total_cost=inner_cost.total_cost,
                estimated_rows=inner_cost.rows,
                width=inner_cost.width
            )
            
            join_cost = self.cost_model.estimate_nested_loop_join(
                OperationCost(
                    current_plan.startup_cost,
                    current_plan.total_cost,
                    current_plan.estimated_rows,
                    current_plan.width
                ),
                inner_cost
            )
            
            join_cond = f"{join.left_table}.{join.left_column} = {join.right_table}.{join.right_column}"
            
            join_node = PlanNode(
                operator=PhysicalOperator.NESTED_LOOP,
                startup_cost=join_cost.startup_cost,
                total_cost=join_cost.total_cost,
                estimated_rows=join_cost.rows,
                width=join_cost.width,
                join_condition=join_cond
            )
            join_node.add_child(current_plan)
            join_node.add_child(inner_plan)
            
            current_plan = join_node
        
        if query.order_by_columns:
            sort_keys = [f"{col} {'DESC' if desc else 'ASC'}" 
                        for col, desc in query.order_by_columns]
            
            sort_cost = self.cost_model.estimate_sort(
                OperationCost(
                    current_plan.startup_cost,
                    current_plan.total_cost,
                    current_plan.estimated_rows,
                    current_plan.width
                )
            )
            
            sort_node = PlanNode(
                operator=PhysicalOperator.SORT,
                startup_cost=sort_cost.startup_cost,
                total_cost=sort_cost.total_cost,
                estimated_rows=sort_cost.rows,
                width=sort_cost.width,
                sort_keys=sort_keys
            )
            sort_node.add_child(current_plan)
            current_plan = sort_node
        
        if query.limit_value is not None:
            limit_cost = self.cost_model.estimate_limit(
                OperationCost(
                    current_plan.startup_cost,
                    current_plan.total_cost,
                    current_plan.estimated_rows,
                    current_plan.width
                ),
                query.limit_value
            )
            
            limit_node = PlanNode(
                operator=PhysicalOperator.LIMIT,
                startup_cost=limit_cost.startup_cost,
                total_cost=limit_cost.total_cost,
                estimated_rows=limit_cost.rows,
                width=limit_cost.width,
                extra_info={"Rows": query.limit_value}
            )
            limit_node.add_child(current_plan)
            current_plan = limit_node
        
        planning_time = (time.time() - start_time) * 1000
        
        return ExecutionPlan(
            root=current_plan,
            query_sql=query.to_sql(),
            planning_time_ms=planning_time,
            is_optimized=False
        )
    
    def _assign_predicates_to_tables(self, query: Query) -> Dict[str, List[Predicate]]:
        result: Dict[str, List[Predicate]] = {}
        
        for table_ref in query.tables:
            result[table_ref.get_ref()] = []
            result[table_ref.name] = []
        
        for pred in query.predicates:
            if pred.table:
                target = pred.table
                if target in result:
                    result[target].append(pred)
                    self.optimization_notes.append(
                        f"Pushed predicate '{pred}' down to table '{target}'"
                    )
                else:
                    actual_name = query.get_table_name(target)
                    if actual_name in result:
                        result[actual_name].append(pred)
                        self.optimization_notes.append(
                            f"Pushed predicate '{pred}' down to table '{actual_name}'"
                        )
        
        return result
    
    def _optimize_join_order(self, query: Query, 
                             table_predicates: Dict[str, List[Predicate]]) -> JoinOrder:
        if len(query.tables) <= 1:
            return JoinOrder(
                tables=[t.get_ref() for t in query.tables],
                estimated_cost=0,
                intermediate_sizes=[]
            )
        
        table_sizes: List[Tuple[str, int]] = []
        
        for table_ref in query.tables:
            table = self.schema.get_table(table_ref.name)
            stats = self.schema.get_stats(table_ref.name)
            
            if not table:
                table_sizes.append((table_ref.get_ref(), 1000))
                continue
            
            selectivity = 1.0
            ref = table_ref.get_ref()
            
            if ref in table_predicates and stats:
                for pred in table_predicates[ref]:
                    sel = stats.get_selectivity(pred.column, pred.operator, pred.value)
                    selectivity *= sel
            
            effective_size = int(table.row_count * selectivity)
            table_sizes.append((ref, max(1, effective_size)))
        
        table_sizes.sort(key=lambda x: x[1])
        
        original_order = [t.get_ref() for t in query.tables]
        new_order = [t[0] for t in table_sizes]
        
        if original_order != new_order:
            self.optimization_notes.append(
                f"Reordered joins: {' -> '.join(new_order)} "
                f"(original: {' -> '.join(original_order)})"
            )
        
        intermediate_sizes = [size for _, size in table_sizes]
        
        total_cost = 0.0
        current_size = table_sizes[0][1]
        
        for i in range(1, len(table_sizes)):
            next_size = table_sizes[i][1]
            total_cost += current_size * next_size
            current_size = int(current_size * next_size * 0.1)
            current_size = max(1, current_size)
        
        return JoinOrder(
            tables=new_order,
            estimated_cost=total_cost,
            intermediate_sizes=intermediate_sizes
        )
    
    def _build_physical_plan(self, query: Query,
                             table_predicates: Dict[str, List[Predicate]],
                             join_order: JoinOrder) -> PlanNode:
        if not join_order.tables:
            return PlanNode(PhysicalOperator.RESULT)
        
        first_ref = join_order.tables[0]
        current_plan = self._build_scan_node(query, first_ref, table_predicates)
        
        for i in range(1, len(join_order.tables)):
            table_ref = join_order.tables[i]
            inner_plan = self._build_scan_node(query, table_ref, table_predicates)
            
            join_cond = self._find_join_condition(query, table_ref)
            
            current_plan = self._build_join_node(
                current_plan, inner_plan, join_cond
            )
        
        return current_plan
    
    def _build_scan_node(self, query: Query, table_ref: str,
                         table_predicates: Dict[str, List[Predicate]]) -> PlanNode:
        actual_name = query.get_table_name(table_ref)
        table = self.schema.get_table(actual_name)
        stats = self.schema.get_stats(actual_name)
        
        if not table:
            table = Table(name=actual_name, row_count=1000)
            stats = TableStats(table=table)
        
        predicates = table_predicates.get(table_ref, [])
        if not predicates:
            predicates = table_predicates.get(actual_name, [])
        
        selectivity = 1.0
        for pred in predicates:
            sel = stats.get_selectivity(pred.column, pred.operator, pred.value)
            selectivity *= sel
        
        best_index = None
        if predicates:
            columns = [p.column for p in predicates]
            operators = [p.operator for p in predicates]
            best_index = stats.find_best_index(columns, operators)
        
        if best_index and selectivity < 0.2:
            idx_cost = self.cost_model.estimate_index_scan(table, best_index, selectivity)
            
            self.optimization_notes.append(
                f"Using index '{best_index.name}' on '{actual_name}' "
                f"(selectivity: {selectivity:.2%})"
            )
            
            node = PlanNode(
                operator=PhysicalOperator.INDEX_SCAN,
                table=actual_name,
                alias=table_ref if table_ref != actual_name else None,
                index_name=best_index.name,
                startup_cost=idx_cost.startup_cost,
                total_cost=idx_cost.total_cost,
                estimated_rows=idx_cost.rows,
                width=idx_cost.width
            )
        else:
            seq_cost = self.cost_model.estimate_seq_scan(table, selectivity)
            
            if best_index and selectivity >= 0.2:
                self.optimization_notes.append(
                    f"Seq scan on '{actual_name}' (index not worth it for "
                    f"{selectivity:.1%} selectivity)"
                )
            
            node = PlanNode(
                operator=PhysicalOperator.SEQ_SCAN,
                table=actual_name,
                alias=table_ref if table_ref != actual_name else None,
                startup_cost=seq_cost.startup_cost,
                total_cost=seq_cost.total_cost,
                estimated_rows=seq_cost.rows,
                width=seq_cost.width
            )
        
        if predicates:
            node.filter_condition = " AND ".join(str(p) for p in predicates)
        
        return node
    
    def _find_join_condition(self, query: Query, table_ref: str) -> Optional[str]:
        actual_name = query.get_table_name(table_ref)
        
        for join in query.joins:
            if join.right_table in (table_ref, actual_name):
                return (f"{join.left_table}.{join.left_column} = "
                       f"{join.right_table}.{join.right_column}")
        
        return None
    
    def _build_join_node(self, outer: PlanNode, inner: PlanNode,
                         join_cond: Optional[str]) -> PlanNode:
        outer_cost = OperationCost(
            outer.startup_cost, outer.total_cost,
            outer.estimated_rows, outer.width
        )
        inner_cost = OperationCost(
            inner.startup_cost, inner.total_cost,
            inner.estimated_rows, inner.width
        )
        
        use_hash = (inner.estimated_rows > 100 and 
                    outer.estimated_rows > 100)
        
        if use_hash:
            join_cost = self.cost_model.estimate_hash_join(outer_cost, inner_cost)
            
            hash_node = PlanNode(
                operator=PhysicalOperator.HASH,
                startup_cost=inner.total_cost,
                total_cost=inner.total_cost * 1.1,
                estimated_rows=inner.estimated_rows,
                width=inner.width
            )
            hash_node.add_child(inner)
            
            join_node = PlanNode(
                operator=PhysicalOperator.HASH_JOIN,
                startup_cost=join_cost.startup_cost,
                total_cost=join_cost.total_cost,
                estimated_rows=join_cost.rows,
                width=join_cost.width,
                join_condition=join_cond
            )
            join_node.add_child(outer)
            join_node.add_child(hash_node)
            
            self.optimization_notes.append(
                f"Using Hash Join (tables > 100 rows each)"
            )
        else:
            join_cost = self.cost_model.estimate_nested_loop_join(outer_cost, inner_cost)
            
            join_node = PlanNode(
                operator=PhysicalOperator.NESTED_LOOP,
                startup_cost=join_cost.startup_cost,
                total_cost=join_cost.total_cost,
                estimated_rows=join_cost.rows,
                width=join_cost.width,
                join_condition=join_cond
            )
            join_node.add_child(outer)
            join_node.add_child(inner)
            
            self.optimization_notes.append(
                f"Using Nested Loop (small table: {inner.estimated_rows} rows)"
            )
        
        return join_node
    
    def _add_final_operators(self, plan: PlanNode, query: Query) -> PlanNode:
        current = plan
        
        if query.order_by_columns:
            sort_keys = [f"{col} {'DESC' if desc else 'ASC'}" 
                        for col, desc in query.order_by_columns]
            
            sort_cost = self.cost_model.estimate_sort(
                OperationCost(
                    current.startup_cost,
                    current.total_cost,
                    current.estimated_rows,
                    current.width
                )
            )
            
            sort_node = PlanNode(
                operator=PhysicalOperator.SORT,
                startup_cost=sort_cost.startup_cost,
                total_cost=sort_cost.total_cost,
                estimated_rows=sort_cost.rows,
                width=sort_cost.width,
                sort_keys=sort_keys
            )
            sort_node.add_child(current)
            current = sort_node
        
        if query.limit_value is not None:
            limit_cost = self.cost_model.estimate_limit(
                OperationCost(
                    current.startup_cost,
                    current.total_cost,
                    current.estimated_rows,
                    current.width
                ),
                query.limit_value
            )
            
            limit_node = PlanNode(
                operator=PhysicalOperator.LIMIT,
                startup_cost=limit_cost.startup_cost,
                total_cost=limit_cost.total_cost,
                estimated_rows=min(query.limit_value, current.estimated_rows),
                width=limit_cost.width,
                extra_info={"Rows": query.limit_value}
            )
            limit_node.add_child(current)
            current = limit_node
            
            self.optimization_notes.append(
                f"LIMIT {query.limit_value} reduces cost by early termination"
            )
        
        return current

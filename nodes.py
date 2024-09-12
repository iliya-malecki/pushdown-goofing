# TODO: try turning a join into a take and add op collation
from __future__ import annotations
from typing import Protocol
from typing import Mapping
from dataclasses import dataclass
from data import Chunk
from columns import Expr, Col, Func
from functools import cache

ColumnSpec = dict[str, Expr]


class Node(Protocol):
    projection: ColumnSpec

    def execute(self) -> Chunk | None: ...

    def plan_to_dict(self) -> dict: ...

    def projection_pushdown(self) -> None: ...

    def accept_projection_pushdown(self, projection: ColumnSpec) -> list[str]: ...


class ScanNode:
    """
    dummy scan node
    """

    def __init__(self, batch_size: int, data: dict[str, list]) -> None:
        assert data
        self.projection: dict[str, Expr] = {k: Col(k) for k in data.keys()}
        self.batch_size = batch_size
        self.data = data
        self.state = 0
        self.data_len = len(next(iter(data.values())))

    def execute(self) -> Chunk | None:
        if self.state >= self.data_len:
            return None
        res = {
            k: self.data[k][self.state : self.state + self.batch_size]
            for k in self.projection
        }
        self.state += self.batch_size
        return Chunk.from_dict(res)

    def plan_to_dict(self) -> dict:
        return {"kind": "scan", "projection": self.projection, "source": None}

    def accept_projection_pushdown(self, projection: ColumnSpec):
        return []

    def projection_pushdown(self):
        return


class ComputeSubnode:
    """
    imagine it works on a column-node tree, this is a simple example
    so im not gonna bother
    """

    def __init__(self, expressions: ColumnSpec) -> None:
        self.expressions = expressions

    def execute(self, data_references: dict[str | None, Mapping]):
        """
        mock of the main derived column computation engine
        """
        return {k: v.evaluate(data_references) for k, v in self.expressions.items()}


class SelectNode:
    def __init__(self, source: Node, projection: ColumnSpec) -> None:
        self.source = source
        self.projection = projection

    def execute(self):
        res = self.source.execute()
        if res is None:
            return None
        else:
            return Chunk(ComputeSubnode(self.projection).execute({None: res.data}))

    def plan_to_dict(self) -> dict:
        return {
            "kind": "select",
            "projection": self.projection,
            "source": self.source.plan_to_dict(),
        }

    def mutate_columns_to_fit_sources(self, source_columns: list[Col]):
        for column in source_columns:
            column.table = None

    def accept_projection_pushdown(self, projection: ColumnSpec):
        # dont accept exprs that rely on freshly computed stuff
        accepted = []
        own_sources = self.source.projection.keys()
        for name, definition in projection.items():
            source_columns = get_source_columns(definition)
            if all(source.name in own_sources for source in source_columns):
                self.mutate_columns_to_fit_sources(source_columns)
                self.projection[name] = definition
                accepted.append(name)
        return accepted

    def projection_pushdown(self):
        for accepted in self.source.accept_projection_pushdown(self.projection):
            self.projection[accepted] = Col(accepted)
        gc(self.source, self.projection)
        self.source.projection_pushdown()


def gc(node: Node, projection: ColumnSpec):
    needed_columns = [
        col.name for el in projection.values() for col in get_source_columns(el)
    ]
    for key in [*node.projection]:
        if key not in needed_columns:
            del node.projection[key]


# the most fun way to define a deep enum
class JoinHow: ...


@dataclass
class Inner(JoinHow):
    left_on: str
    right_on: str


class Cross(JoinHow): ...


class AlmostBlockNestedLoopJoinNode:
    """
    A simple nested loop join (idea). Does not actually implement BNLJ
    since that would require .reset() and .cache() on all Nodes which is annoying
    """

    def __init__(
        self,
        left_source: Node,
        right_source: Node,
        how: JoinHow,
        projection: ColumnSpec,
    ) -> None:
        self.projection = projection
        source_coulmns = [
            col for source in projection.values() for col in get_source_columns(source)
        ]
        assert all(col.table is not None for col in source_coulmns)
        self.how = how
        self.left_source = left_source
        self.right_source = right_source

    def execute(self) -> Chunk | None:
        left = self.left_source.execute()
        if left is None:
            return None
        right = self.right_source.execute()
        if right is None:
            return None

        res = []
        for left_row in left:
            for right_row in right:
                if isinstance(self.how, Cross) or (
                    isinstance(self.how, Inner)
                    and left_row[self.how.left_on] == right_row[self.how.right_on]
                ):
                    res.append(
                        ComputeSubnode(self.projection).execute(
                            {
                                "left": left_row,
                                "right": right_row,
                            }
                        )
                    )
        return Chunk.from_rows(res)

    def plan_to_dict(self) -> dict:
        return {
            "kind": "join",
            "projection": self.projection,
            "left": self.left_source.plan_to_dict(),
            "right": self.right_source.plan_to_dict(),
        }

    def mutate_columns_to_fit_sources(self, source_columns: list[Col]):
        for column in source_columns:
            if column.name in self.left_source.projection:
                column.table = "left"
            elif column.name in self.right_source.projection:
                column.table = "right"
            else:
                raise ValueError("unreachable")

    def accept_projection_pushdown(self, projection: ColumnSpec):
        own_sources = set(self.left_source.projection.keys()).union(
            self.right_source.projection.keys()
        )
        accepted = []
        for name, definition in projection.items():
            source_columns = get_source_columns(definition)
            if all(source.name in own_sources for source in source_columns):
                self.mutate_columns_to_fit_sources(source_columns)
                self.projection[name] = definition
                accepted.append(name)
        return accepted

    def projection_pushdown(self):
        for_left_pushdown = {
            name: definition
            for name, definition in self.projection.items()
            if all(source.table == "left" for source in get_source_columns(definition))
        }
        for_right_pushdown = {
            name: definition
            for name, definition in self.projection.items()
            if all(source.table == "right" for source in get_source_columns(definition))
        }

        for accepted in self.left_source.accept_projection_pushdown(for_left_pushdown):
            self.projection[accepted] = Col(accepted, table="left")
        for accepted in self.right_source.accept_projection_pushdown(
            for_right_pushdown
        ):
            self.projection[accepted] = Col(accepted, table="right")

        gc(self.left_source, self.projection)
        gc(self.right_source, self.projection)
        self.left_source.projection_pushdown()
        self.right_source.projection_pushdown()


@cache
def get_source_columns(expr: Expr) -> list[Col]:
    return [x for x in _get_source_columns(expr) if x is not None]


def _get_source_columns(expr: Expr) -> list[Col | None]:
    if isinstance(expr, Col):
        return [expr]
    elif isinstance(expr, Func):
        if isinstance(expr.of, tuple):
            left, right = expr.of
            return [*get_source_columns(left), *get_source_columns(right)]
        else:
            return _get_source_columns(expr.of)
    else:
        return [None]


def optimize(root: Node):
    root.projection_pushdown()

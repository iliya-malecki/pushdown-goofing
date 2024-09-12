from __future__ import annotations
from typing import Literal, Any, Mapping
from abc import ABC, abstractmethod
import numpy as np


class Expr(ABC):
    def __hash__(self) -> int:
        # ooh dirty
        return id(self)

    def __add__(self, other) -> Add:
        return Add((self, other))

    def __sub__(self, other) -> Sub:
        return Sub((self, other))

    def __mul__(self, other) -> Mul:
        return Mul((self, other))

    def __truediv__(self, other) -> Div:
        return Div((self, other))

    def max(self):
        return Max(self)

    def min(self):
        return Min(self)

    @abstractmethod
    def evaluate(self, data_references: DataReference) -> np.ndarray: ...


class Lit(Expr):
    def __init__(self, value) -> None:
        self.value = value

    def evaluate(self, data_references: DataReference):
        return self.value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value})"


class Col(Expr):
    def __init__(
        self, name: str, table: Literal["left", "right"] | None = None
    ) -> None:
        self.table: None | Literal["left"] | Literal["right"] = table
        self.name = name

    @classmethod
    def left(cls, name):
        return cls(name, table="left")

    @classmethod
    def right(cls, name):
        return cls(name, table="right")

    def evaluate(self, data_references: DataReference):
        return data_references[self.table][self.name]

    def __repr__(self) -> str:
        if self.table is not None:
            return f"<{self.table}.{self.name}>"
        else:
            return f"<{self.name}>"


class Func(Expr):
    def __init__(self, of: Expr | tuple[Expr, Expr]) -> None:
        self.of: Expr | tuple[Expr, Expr] = of

    def __repr__(self) -> str:
        if isinstance(self.of, tuple):
            of = ", ".join(repr(x) for x in self.of)
        else:
            of = repr(self.of)
        return f"{self.__class__.__name__}({of})"


DataReference = dict[str | None, Mapping[str, np.ndarray]]


class Add(Func):
    def evaluate(self, data_references: DataReference):
        assert isinstance(self.of, tuple)
        left, right = self.of
        return left.evaluate(data_references) + right.evaluate(data_references)


class Sub(Func):
    def evaluate(self, data_references: DataReference):
        assert isinstance(self.of, tuple)
        left, right = self.of
        return left.evaluate(data_references) - right.evaluate(data_references)


class Mul(Func):
    def evaluate(self, data_references: DataReference):
        assert isinstance(self.of, tuple)
        left, right = self.of
        return left.evaluate(data_references) * right.evaluate(data_references)


class Div(Func):
    def evaluate(self, data_references: DataReference):
        assert isinstance(self.of, tuple)
        left, right = self.of
        return left.evaluate(data_references) / right.evaluate(data_references)


class Max(Func):
    def evaluate(self, data_references: DataReference):
        assert not isinstance(self.of, tuple)
        return self.of.evaluate(data_references).max(axis=-1)


class Min(Func):
    def evaluate(self, data_references: DataReference):
        assert not isinstance(self.of, tuple)
        return self.of.evaluate(data_references).min(axis=-1)

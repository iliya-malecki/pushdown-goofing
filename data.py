import numpy as np
import polars as pl


class Chunk:
    @classmethod
    def from_dict(cls, data: dict[str, list]):
        return cls({k: np.array(v) for k, v in data.items()})

    @classmethod
    def from_rows(cls, data: list[dict[str, list]]):
        columns: list[str] = [*set().union(*(row.keys() for row in data))]
        try:
            return cls({k: np.array([row[k] for row in data]) for k in columns})
        except KeyError as e:
            raise ValueError(f"broken data with different field names per row: {e}")

    def __init__(self, data: dict[str, np.ndarray]) -> None:
        self.data = {k: np.array(v) for k, v in data.items()}
        self.data

    def __len__(self):
        for val in self.data.values():
            return len(val)
            raise ValueError("unreachable")
        return 0

    def __iter__(self):
        # imagine actually reasonable tuple streaming
        return ({k: v[i] for k, v in self.data.items()} for i in range(len(self)))

    def __repr__(self) -> str:
        if self.data is None:
            return "Chunk(None)"
        try:
            return repr(pl.DataFrame(self.data))
        except Exception:
            print(self.data)
            raise

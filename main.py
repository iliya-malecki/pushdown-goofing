# %%
import json
import numpy as np
from columns import Col, Lit
from nodes import ScanNode, SelectNode, AlmostBlockNestedLoopJoinNode, Inner
import timeit


def build_test():
    source1 = ScanNode(
        100,
        data={
            "id": [*range(100)],
            "squares": [np.arange(x, x + 3000000) ** 2 for x in range(100)],
        },
    )
    source2 = ScanNode(
        100,
        data={
            "id": [*range(100)],
            "cubes": [np.arange(x, x + 3000000) ** 3 for x in range(100)],
        },
    )

    select = SelectNode(
        source1,
        projection={
            "id": Col("id"),
            "newcol": Col("squares"),
            "squaretimes2": Col("squares") * Lit(2),
        },
    )
    join = AlmostBlockNestedLoopJoinNode(
        left_source=select,
        right_source=source2,
        how=Inner(left_on="id", right_on="id"),
        projection={
            "id": Col.left("id"),
            "squaretimes2": Col.left("squaretimes2"),
            "cubes": Col.right("cubes"),
        },
    )

    computation_cheaper_than_copy = SelectNode(
        join,
        {
            "id": Col("id"),
            "computed": Col("squaretimes2").max() + Col("cubes").max(),
        },
    )
    return computation_cheaper_than_copy


# %%
computation_cheaper_than_copy = build_test()
print(json.dumps(computation_cheaper_than_copy.plan_to_dict(), indent=8, default=str))
computation_cheaper_than_copy.projection_pushdown()
print(json.dumps(computation_cheaper_than_copy.plan_to_dict(), indent=8, default=str))
print(computation_cheaper_than_copy.execute())

# %%

print(
    "baseline:",
    timeit.timeit(stmt="x.execute()", setup="x=build_test()", globals=globals()),
)
print(
    "with projection pushdown:",
    timeit.timeit(
        stmt="x.execute()",
        setup="x=build_test();x.projection_pushdown()",
        globals=globals(),
    ),
)

# %%
# make a thing with smart join and useless select
# make a thing with stupid join and a select
calculated_after_copy = SelectNode(
    AlmostBlockNestedLoopJoinNode(
        left_source=ScanNode(
            100,
            data={
                "id": [*range(100)],
                "squares": [np.arange(x, x + 3000000) ** 2 for x in range(100)],
            },
        ),
        right_source=ScanNode(
            100,
            data={
                "id": [*range(100)],
                "cubes": [np.arange(x, x + 3000000) ** 3 for x in range(100)],
            },
        ),
        how=Inner(left_on="id", right_on="id"),
        projection={
            "id": Col.left("id"),
            "squares": Col.left("squares"),
            "cubes": Col.right("cubes"),
        },
    ),
    {
        "id": Col("id"),
        "computed": Col("squares").max() + Col("cubes").max(),
    },
)
calculated_on_the_fly = SelectNode(
    AlmostBlockNestedLoopJoinNode(
        left_source=ScanNode(
            100,
            data={
                "id": [*range(100)],
                "squares": [np.arange(x, x + 3000000) ** 2 for x in range(100)],
            },
        ),
        right_source=ScanNode(
            100,
            data={
                "id": [*range(100)],
                "cubes": [np.arange(x, x + 3000000) ** 3 for x in range(100)],
            },
        ),
        how=Inner(left_on="id", right_on="id"),
        projection={
            "id": Col.left("id"),
            "computed": Col.left("squares").max() + Col.right("cubes").max(),
        },
    ),
    {
        "id": Col("id"),
        "computed": Col("computed"),
    },
)
import timeit

print(
    "with copy: ", timeit.timeit("calculated_after_copy.execute()", globals=globals())
)
print(
    "without copy: ",
    timeit.timeit("calculated_on_the_fly.execute()", globals=globals()),
)

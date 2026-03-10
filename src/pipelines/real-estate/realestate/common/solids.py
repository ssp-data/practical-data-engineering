import dagster as dg
from typing import Optional


@dg.op(
    out={"foo_output": dg.Out(Optional[float], is_required=False)},
)
def condition_check_bool(condition: bool):
    if condition:
        yield 1.0

from dagster import op, Optional, Bool, Out


@op(
    out={"foo_output": Out(Optional[float], is_required=False)},
)
def condition_check_bool(condition: Bool):
    if condition:
        yield 1.0

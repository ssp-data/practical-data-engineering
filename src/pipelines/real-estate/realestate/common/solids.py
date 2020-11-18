from dagster import solid, OutputDefinition, Optional, Bool


@solid(
    output_defs=[OutputDefinition(Optional[float], name="foo_output", is_required=False),]
)
def condition_check_bool(context, condition: Bool):
    if condition:
        yield Output(1.0, 'foo_output')

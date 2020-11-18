from dagster import execute_pipeline, solid, pipeline


@solid
def not_much(_):
    return


@pipeline()
def parallel_pipeline():
    # example to loop through all dimension or also facts parallel e.g.
    for i in range(5):
        not_much.alias('not_much_' + str(i))()


def define_parallel_pipeline():
    return parallel_pipeline


def test_pipeline_success():
    res = execute_pipeline(parallel_pipeline)
    assert res.success


# def test_pipeline_failure():
#     res = execute_pipeline(parallel_pipeline, raise_on_error=False)
#     assert not res.success

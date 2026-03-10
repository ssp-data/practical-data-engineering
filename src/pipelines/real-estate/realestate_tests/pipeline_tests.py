# start with pytest pipeline_test.py

import dagster as dg

import yaml


from realestate.pipelines import collect_search_criterias, collect_properties


def read_yaml(path):
    with open(dg.file_relative_path(__file__, path)) as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def test_solid_collect_properties_input_list():

    input = [[111, 222, 333], [55, 666666, 7777], [99, 999, 9999]]
    context = dg.build_op_context()
    result = collect_properties(context, input)

    assert len(result) == 9


def test_collect_search_criterias():

    run_conf = read_yaml('../realestate/config_pipelines/scrape_realestate.yaml')
    # YAML now wraps each item as {value: {...}}, unwrap for direct invocation
    search_criterias = [item['value'] for item in run_conf['ops']['collect_search_criterias']['inputs']['search_criterias']]

    context = dg.build_op_context()
    results = list(collect_search_criterias(context, search_criterias))

    assert len(results) > 0

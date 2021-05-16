# start with pytest pipeline_test.py

from dagster import execute_solid, file_relative_path, SolidExecutionResult
from dagster.experimental import DynamicOutputDefinition, DynamicOutput

import yaml


from realestate.common.types_realestate import SearchCoordinate, PropertyDataFrame
from realestate.pipelines import collect_search_criterias, collect_properties


def read_yaml(path):
    with open(file_relative_path(__file__, path)) as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def test_solid_collect_properties_input_list():

    input = [[111, 222, 333], [55, 666666, 7777], [99, 999, 9999]]
    result = execute_solid(collect_properties, input_values={"properties": input})

    assert result.success
    assert len(result.output_value("properties")) == 9


def test_collect_search_criterias():

    run_conf = read_yaml('../realestate/config_pipelines/scrape_realestate.yaml')
    search_criterias = run_conf['solids']['collect_search_criterias']['inputs']['search_criterias']

    result = execute_solid(
        collect_search_criterias, input_values={"search_criterias": search_criterias}
    )

    ## playing around with the output values:
    # print(result.compute_output_events_dict["result"])

    # assert len(result.compute_output_events_dict["result"]) == 3

    # assert result.output_values == {
    #     "result": {
    #         "bern_buy_real_estate_0": {
    #             'city': 'Bern',
    #             'propertyType': 'real-estate',
    #             'radius': 0,
    #             'rentOrBuy': 'buy',
    #         }
    #     }
    # }
    assert result.success

''' Op for Apache Druid '''

from .types import DeltaCoordinate, DruidCoordinate

import requests
import json

from dagster import (
    op,
    Field,
    String,
)


def _druid_ingest_spec_replacer(
    spec: json, delta_coordinate: DeltaCoordinate, druid_coordinate: DruidCoordinate
):

    # replace source
    if spec['spec']['ioConfig']['inputSource']['type'] == "s3":
        spec['spec']['ioConfig']['inputSource']['prefixes'] = [
            "s3://"
            + delta_coordinate['s3_coordinate_bucket']
            + '/'
            + delta_coordinate['s3_coordinate_key']
            + '/'
            + 'part'  # part is the prefix for parquet-snappy files. Druid needs a prefix otherwise it does not work
        ]

    # replace name of datasource
    spec['spec']['dataSchema']['dataSource'] = druid_coordinate['datasource']

    return spec


@op(
    required_resource_keys={'pyspark', 's3', 'druid'},
    description='''This ingests data from your input delta table (sitting on s3 bucket) into druid.

    It will check first the connection to druid cluster
    then delete all segements in the interval specified
    before it loads the new parquet files (your delta table)''',
    config_schema={
        'status_health_api_postfix': Field(
            String,
            default_value='status/health',
            is_required=False,
            description=('druid api to get healt of druid cluster'),
        ),
        'datasource_health_api_postfix': Field(
            String,
            default_value='druid/coordinator/v1/datasources',
            is_required=False,
            description=('druid api to list datasources in druid cluster'),
        ),
        'status_index_task_api_postfix': Field(
            String,
            default_value='druid/indexer/v1/task',
            is_required=False,
            description=('druid api to ingest a index task to druid cluster'),
        ),
    },
)
def ingest_druid(
    context,
    delta_coordinate: DeltaCoordinate,
    druid_coordinate: DruidCoordinate,
) -> DruidCoordinate:

    # context.log.info(context.resources.druid.druid_router)

    context.log.info('router: ' + context.resources.druid.get_router_url())

    #
    # check Druid health
    #
    druidHealthAPI = (
        context.resources.druid.get_router_url()
        + '/'
        + context.solid_config['status_health_api_postfix']
    )
    context.log.debug("Druid healt API: " + druidHealthAPI)

    r = context.resources.druid.get_session().get(druidHealthAPI, verify=False)
    context.log.info(
        "1. Check Druid health: status_code: {status_code}, reason {reason}".format(
            status_code=r.status_code, reason=r.reason
        )
    )

    if r.status_code != 200:
        raise ValueError("1.2. Druid is unhealthy. Status_Code: {status_code}").format(
            status_code=r.status_code
        )
    else:
        #
        # submit deletion of segments
        #
        headers = {'Content-type': 'application/json'}

        druidDeleteSegmentAPI = (
            context.resources.druid.get_router_url()
            + '/'
            + context.solid_config['datasource_health_api_postfix']
            + '/'
            + druid_coordinate['datasource']
            + "/markUnused"
        )
        intervalToDelete = json.dumps({'interval': druid_coordinate['intervalToDelete']})

        r = context.resources.druid.get_session().post(
            druidDeleteSegmentAPI, data=intervalToDelete, headers=headers, verify=False
        )
        context.log.info(
            "2. submit deletion of segments: status_code: {status_code}, reason: {reason}, content: {content}".format(
                status_code=r.status_code, reason=r.reason, content=r.content
            )
        )

        # TODO: Ingest only latest Parquet-files from delta (delta keeps older versions for time-travel).
        # If we're not doing that, we will ingest old versions together agian

        #
        # get ingest spec:
        #
        try:
            spec_path = file_relative_path(__file__, druid_coordinate['PathToJsonIngestSpec'])
            with open(spec_path, 'r') as f:
                ingestSpec = f.read()
        except ValueError as error:
            raise ValueError("Json at path: '{fpath}' not found. {err}").format(
                fpath=druid_coordinate['PathToJsonIngestSpec'], err=error
            )
        #
        # parse json spec
        #
        try:
            ingestSpecJson = json.loads(ingestSpec)
        except ValueError as error:
            raise ValueError(
                "Not a valid JSON at path: '{fpath}'. Check the syntaxt: {err}"
            ).format(fpath=druid_coordinate['PathToJsonIngestSpec'], err=error)

        context.log.info(
            "3. Ingest spec successfully loaded from path '{fpath}'".format(fpath=spec_path)
        )

        ingestSpecJson = _druid_ingest_spec_replacer(
            ingestSpecJson, delta_coordinate, druid_coordinate
        )

        context.log.info("4. Replaced source-Bucket and DataSource in ingest spec")

        #
        # start ingest
        #
        druidTaskAPI = (
            context.resources.druid.get_router_url()
            + '/'
            + context.solid_config['status_index_task_api_postfix']
        )

        r = context.resources.druid.get_session().post(
            druidTaskAPI, data=json.dumps(ingestSpecJson), headers=headers, verify=False
        )
        context.log.info('full ingest spec: ' + json.dumps(ingestSpecJson))
        if r.status_code == 200:
            context.log.info(
                "5.1 Ingest sucessfully sent (status: {status_code}) to Druid. Respond-Reason {reason}".format(
                    status_code=r.status_code, reason=r.reason
                )
            )
            context.log.info("5.2 TaskId: '{task_id}'".format(task_id=r.json()['task']))
        else:
            raise ValueError(
                "5. Ingest to Druid error with status: {status_code}. Respond-Reason {reason}".format(
                    status_code=r.status_code, reason=r.reason
                )
            )
        # TODO: check report status of Druid task, if it failed directly, or if its loading
        #       - report-API: druid/indexer/v1/task/"+task+"/reports"
        #       - maybe wait a couple of seconds/minutes, to be sure it stared correctly?
    # return delta object
    return druid_coordinate

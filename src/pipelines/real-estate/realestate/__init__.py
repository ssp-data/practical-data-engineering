# from dagster import repository


# def get_realestate_pipelines():
#     from realestate.pipelines import parallel_pipeline

#     return [
#         parallel_pipeline,
#     ]


# def define_internal_dagit_repository():

#     # Lazy import here to prevent deps issues
#     @repository
#     def internal_dagit_repository():

#         pipeline_defs = get_realestate_pipelines()

#         return pipeline_defs

#     return internal_dagit_repository

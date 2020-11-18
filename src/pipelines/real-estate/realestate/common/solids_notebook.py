def _notebook_path(name):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "notebooks", name)


# start_solids_marker_3
def notebook_solid(name, notebook_path, input_defs, output_defs, required_resource_keys):
    return define_dagstermill_solid(
        name,
        _notebook_path(notebook_path),
        input_defs,
        output_defs,
        required_resource_keys=required_resource_keys,
    )
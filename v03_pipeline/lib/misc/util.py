def callset_project_pairs(
    callset_paths: list[str],
    project_guids: list[str],
    project_remap_paths: list[str],
    project_pedigree_paths: list[str],
):
    if len(callset_paths) == len(project_guids):
        return zip(
            callset_paths,
            project_guids,
            project_remap_paths,
            project_pedigree_paths,
            strict=True,
        )
    return (
        (callset_path, project_guid, project_remap_path, project_pedigree_path)
        for callset_path in callset_paths
        for (project_guid, project_remap_path, project_pedigree_path) in zip(
            project_guids,
            project_remap_paths,
            project_pedigree_paths,
            strict=True,
        )
    )

import os


def _strip_outputs_pre_save(model, **kwargs):
    if os.getenv("NOTEBOOK_SAVE_OUTPUTS") == "1":
        return
    if model.get("type") != "notebook":
        return

    content = model.get("content")
    if not content:
        return

    for cell in content.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        cell["outputs"] = []
        cell["execution_count"] = None
        metadata = cell.get("metadata")
        if isinstance(metadata, dict):
            metadata.pop("execution", None)


c.FileContentsManager.pre_save_hook = _strip_outputs_pre_save

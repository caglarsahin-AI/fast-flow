from pathlib import Path

def path_tags(file: str):
    """
    dags/<system>/<subsystem>/<dag_file>.py
    -> ["system:<system>", "subsystem:<subsystem>"]
    """
    parts = Path(file).resolve().parts
    # .../dags/system/subsystem/file.py  -> sondan say
    try:
        subsystem = parts[-2]
        system = parts[-3]
        return [f"system:{system}", f"subsystem:{subsystem}"]
    except Exception:
        return []

def std_tags(file: str, extra=None):
    base = path_tags(file)
    return base + (extra or [])
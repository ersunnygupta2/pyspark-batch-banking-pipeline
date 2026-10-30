import json
import os

def get_processed_files(control_file_path: str) -> set[str]:
    if not os.path.exists(control_file_path):
        return set()
    with open(control_file_path, "r") as control_file:
        control_data = json.load(control_file)
    return set(control_data.get("processed_files", []))

def mark_file_as_processed(
    control_file_path: str,
    file_name: str,
) -> None:
    processed_files = get_processed_files(control_file_path)
    processed_files.add(file_name)
    
    control_data = {
        "processed_files": sorted(processed_files)
    }
    
    with open(control_file_path, "w") as control_file:
        json.dump(control_data, control_file, indent=2)
    

def get_processed_batches(control_file_path: str) -> set[str]:
    if not os.path.exists(control_file_path):
        return set()
    with open(control_file_path, "r") as control_file:
        control_data = json.load(control_file)
    return set(control_data.get("processed_batches", []))

def mark_batch_as_processed(
    control_file_path: str,
    batch_id: str,
) -> None:
    processed_batches = get_processed_batches(control_file_path)
    processed_batches.add(batch_id)
    
    control_data = {
        "processed_batches": sorted(processed_batches)
    }
    with open(control_file_path, "w") as control_file:
        json.dump(control_data, control_file, indent=2)
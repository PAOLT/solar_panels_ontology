# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a96b8679-a934-40dd-9266-dcd36cac9629",
# META       "default_lakehouse_name": "birdbench",
# META       "default_lakehouse_workspace_id": "d8ed0771-df93-4eaa-8c89-33e2dc66b11c",
# META       "known_lakehouses": [
# META         {
# META           "id": "a96b8679-a934-40dd-9266-dcd36cac9629"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Merge ground-truth files
# 
# New notebookground truth file for all the Bird Bench databases

# CELL ********************

import argparse
import json
from pathlib import Path
from typing import Dict, List, Any

def load_jsonl(file_path: str) -> List[Dict[str, Any]]:
    """Load data from a JSONL file."""
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():  # Skip empty lines
                data.append(json.loads(line))
    return data

def save_jsonl(data: List[Dict[str, Any]], file_path: str) -> None:
    """Save data to a JSONL file, creating a backup only if the original exists."""
    path = Path(file_path)
    backup_path = path.with_suffix(path.suffix + '.bak')

    # Create a backup of the original file if it exists
    if path.exists():
        path.rename(backup_path)

    # Ensure the parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write the new data
    with open(path, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

def integrate_data(public_data: List[Dict[str, Any]], gt_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Integrate ground truth data with public data based on instance_id."""
    # Create lookup dictionary for ground truth data
    gt_lookup = {item['instance_id']: item for item in gt_data}
    
    # Integrate data
    integrated_data = []
    for item in public_data:
        instance_id = item['instance_id']
        if instance_id in gt_lookup:
            # Merge the ground truth fields
            gt_item = gt_lookup[instance_id]
            integrated_item = item.copy()
            # Add the protected fields from ground truth
            for field in ['sol_sql', 'test_cases', 'external_knowledge']:
                if field in gt_item:
                    integrated_item[field] = gt_item[field]
            integrated_data.append(integrated_item)
        else:
            # logger is not defined in this notebook; we simply keep the original item
            integrated_data.append(item)
    
    return integrated_data


# Integrate ground truth data with public dataset
gt_file = '/lakehouse/default/Files/livesqlbench-large-v1/livesqlbench_large_v1_gt_kg_testcases_20260302.jsonl'  # Path to the ground truth data file
public_file = '/lakehouse/default/Files/livesqlbench-large-v1/livesqlbench_large_v1_data.jsonl'  # Path to the public dataset file
output_file = '/lakehouse/default/Files/livesqlbench-large-v1/livesqlbench_large_v1_data_integrated.jsonl'

# Validate input files exist
if not Path(gt_file).exists():
    raise FileNotFoundError(f"Ground truth file not found: {gt_file}")
if not Path(public_file).exists():
    raise FileNotFoundError(f"Public dataset file not found: {public_file}")

# Load data
public_data = load_jsonl(public_file)
gt_data = load_jsonl(gt_file)

# Integrate data
integrated_data = integrate_data(public_data, gt_data)

# Save integrated data back to the output file
save_jsonl(integrated_data, output_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(integrated_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

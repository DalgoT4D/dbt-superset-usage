"""
check the version of the dashboard being processed
schema of all datasets will tell you this - eg. schema: prod_v2

Upgrade the current dashboard
This means 
1. Generate new "uuid" for each dataset
2. For each newly generated dataset uuid, update the corresponding "dataset_uuid" in the charts
3. Generate new "uuid" for each chart
4. Update the newly generated chart uuid of corresponding charts referenced in the dashboard
5. Update the title of the dashboard
6. Generate a new uuid for the dashboard if we want to have a new version of the dashboard 
   or keep the same uuid if we want to overwrite the current dashboard
"""
import os
from pathlib import Path
import uuid
from logging import basicConfig, getLogger, INFO
import glob
import argparse
import yaml

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("export_dir", help="directory containing the exported dashboard")
parser.add_argument("--upgrade", action="store_true", help="upgrade the dashboard")
args = parser.parse_args()

export_dir = args.export_dir  # "dashboard_export_20231026T182233"

dataset_dir = Path(export_dir) / "datasets"
charts_dir = Path(export_dir) / "charts"
dashboard_dir = Path(export_dir) / "dashboards"

upgrade = args.upgrade

# there will only be one dashboard
dashboard = None
for filename in os.listdir(dashboard_dir):
    if filename.endswith(".yaml"):
        with open(dashboard_dir / filename, "r", encoding="utf-8") as dashboard_file:
            dashboard = yaml.safe_load(dashboard_file)
            dashboard["file_path"] = dashboard_dir / filename
            break
# pylint:disable=logging-fstring-interpolation
logger.info(f"read the {dashboard['dashboard_title']} dashhboard")

charts = []
for filename in os.listdir(charts_dir):
    if filename.endswith(".yaml"):
        print(filename)
        with open(charts_dir / filename, "r", encoding="utf-8") as chart_file:
            chart = yaml.safe_load(chart_file)
            chart["file_path"] = charts_dir / filename
            charts.append(chart)
logger.info("read/loaded all charts")

datasets = []
for filename in glob.glob(str(dataset_dir) + "/**/*.yaml", recursive=True):
    if filename.endswith(".yaml"):
        with open(filename, "r", encoding="utf-8") as dataset_file:
            dataset = yaml.safe_load(dataset_file)
            dataset["file_path"] = filename
            datasets.append(dataset)
logger.info("read/loaded all datasets")

# current version of dashboard, schemas of the dataset will tell us
version = None
version_set = set()
for dataset in datasets:
    version = dataset["schema"].split("_")[-1]
    version_set.add(version)

if len(version_set) > 1:
    raise ValueError("multiple versions of datasets found")

version = int(version_set.pop().replace("v", ""))
logger.info(f"current version of dashboard is {version}")

for dataset in datasets:
    # 1. Generate new "uuid" for each dataset
    new_dataset_uuid = str(uuid.uuid4())

    # 2. For each newly generated dataset uuid, update the corresponding "dataset_uuid" in the charts
    for chart in charts:
        if chart["dataset_uuid"] == dataset["uuid"]:
            chart["dataset_uuid"] = new_dataset_uuid

    dataset["uuid"] = new_dataset_uuid

    if upgrade:
        # upgrade the version in schema
        logger.info(
            f"upgrading the version of schema {dataset['schema']} to {version+1} "
        )
        dataset["schema"] = dataset["schema"].replace(f"v{version}", f"v{version+1}")

    # overwrite the new dataset
    with open(dataset["file_path"], "w", encoding="utf-8") as dataset_file:
        del dataset["file_path"]
        yaml.safe_dump(dataset, dataset_file, allow_unicode=True)


# 3. Generate new "uuid" for each chart
for chart in charts:
    new_chart_uuid = str(uuid.uuid4())

    # 4. Update the newly generated chart uuid of corresponding charts referenced in the dashboard
    for key, config in dashboard["position"].items():
        if key.startswith("CHART"):
            if config["meta"]["uuid"] == chart["uuid"]:
                config["meta"]["uuid"] = new_chart_uuid

    chart["uuid"] = new_chart_uuid

    # overwrite the new chart
    with open(chart["file_path"], "w", encoding="utf-8") as chart_file:
        del chart["file_path"]
        yaml.safe_dump(chart, chart_file, allow_unicode=True)

# 5. Update the title of the dashboard
dashboard["title"] = "new title"

# 6. Generate a new uuid for the dashboard if we want to have a new version of the dashboard
#    or keep the same uuid if we want to overwrite the current dashboard
dashboard["uuid"] = str(uuid.uuid4())

# write the new dashboard
with open(dashboard["file_path"], "w", encoding="utf-8") as dashboard_file:
    del dashboard["file_path"]
    # upgrade the version in title
    if upgrade:
        dashboard["dashboard_title"] = f"Superset usage v{version+1}.0"
    yaml.safe_dump(dashboard, dashboard_file, allow_unicode=True)

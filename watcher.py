#!/usr/bin/env python

import argparse
import logging as log
import os
from random import random
from time import sleep

from bioblend import galaxy

# find . -type f -name "*.raw"
# example line from input file
# ./H2020_HBM4EU/2020/WP16_Specimen/HBM4EU_ESI_positive_WP_urine_MS1/HBM4EU_Fieldwork_1_Batch_1-20201001-CMV/RAW_profile/Tribrid_201001_051-350697_POS_MU.raw

log.basicConfig(level=log.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("--galaxy_url", help="URL of Galaxy")
parser.add_argument("--apikey", help="Galaxy API key")
parser.add_argument("--library_id", help="Galaxy library ID for the project")
parser.add_argument("--raw_list", help="List of raw files, one path per line, relative to the 'rcx-da' folder.")
parser.add_argument("--import_raw_only", help="Only link all raw files to the Galaxy Data Library", default=False)
parser.add_argument("--import_results_only", help="Only link all mzml/json/txt files to the Galaxy Data Library", default=False)
args = parser.parse_args()

EXPORT_PATH_PREFIX = "/mnt/sally/000020-Shares/rcx-da/"
LIBRARY_ID = "0a248a1f62a0cc04"
LIBRARY_NAME = "rcx-da"
LIBRARY_ROOT_FOLDER_ID = "F2f94e8ae9edff68a"
MZML_FOLDER_NAME = "mzML_profile"
ALLOWED_RAW_FILES_FOLDER_NAMES = ["RAW_profile", "RAW", "raw"]
CONVERSION_WORKFLOW_ID = "13cea0e6d733b865"
THERMO_FILE_TYPE = "thermo.raw"
MZML_FILE_TYPE = "mzml"
JSON_FILE_TYPE = "json"
TXT_FILE_TYPE = "txt"

gi = galaxy.GalaxyInstance(url=args.galaxy_url, key=args.apikey)
library_folders = gi.libraries.get_folders(library_id=LIBRARY_ID)


def is_converted(raw_file_path):
    """Check whether given raw file has already been converted."""
    is_converted = False
    mzml_file = get_mzml_path(raw_file_path)
    if os.path.exists(mzml_file):
        is_converted = True
    return is_converted


def get_library_dataset(file_path):
    """Check whether given file is already linked to library, return the dataset if found."""
    library_dataset = None
    file_dir, file_name = os.path.split(file_path)
    for folder in library_folders:
        if file_dir == folder["name"].lstrip("/"):
            # Checking for file name equality only, warning: libraries allow name duplicates.
            folder_detail = gi.folders.show_folder(folder_id=folder["id"], contents=True)
            for item in folder_detail["folder_contents"]:
                if file_name == item["name"]:
                    library_dataset = item
                    break
    return library_dataset


def get_mzml_path(raw_file_path):
    """Return the mzml path corresponding to the given .RAW path."""
    raw_dir, raw_file = os.path.split(raw_file_path)
    return os.path.join(EXPORT_PATH_PREFIX, os.path.split(raw_dir)[0], MZML_FOLDER_NAME, f"{raw_file[:-4]}.mzml")


def run_conversion_workflow(library_dataset, raw_file_path):
    remote_path = get_mzml_path(raw_file_path)
    wf_inputs = {'0': {'id': library_dataset["id"], 'src': 'ld'}}
    wf_params = {'2': {'remote_path': remote_path}}
    invocation = gi.workflows.invoke_workflow(workflow_id=CONVERSION_WORKFLOW_ID,
                                              inputs=wf_inputs,
                                              params=wf_params,
                                              history_name=f"conversion of {library_dataset['name']}")
    return invocation


def link_to_data_library(given_file_path, file_type):
    file_dir, _ = os.path.split(given_file_path)
    path = os.path.normpath(file_dir)
    folders = path.split(os.sep)
    # ['H2020_HBM4EU', '2020', 'WP16_Specimen', 'HBM4EU_ESI_negative_WP_urine_MS1', 'HBM4EU_Fieldwork_1_Batch_1-20201005-CMV', 'RAW_profile']
    parent_folder_id = LIBRARY_ROOT_FOLDER_ID
    remote_path = ""
    while folders:
        current_name = folders.pop(0)
        remote_path = remote_path + "/" + current_name  # DL folder names start with a slash
        remote_folder = gi.libraries.get_folders(library_id=LIBRARY_ID, name=remote_path)
        if len(remote_folder) > 1:
            raise Exception(f"Multiple remote folders found for {remote_path}")
        elif len(remote_folder) < 1:
            log.debug(f"Creating folder with name: {current_name}")
            new_folder = gi.libraries.create_folder(library_id=LIBRARY_ID, folder_name=current_name, base_folder_id=parent_folder_id)
            parent_folder_id = new_folder[0]["id"]
        else:
            # Folder already exists, moving down
            parent_folder_id = remote_folder[0]["id"]
    remote_folder = gi.libraries.get_folders(library_id=LIBRARY_ID, folder_id=parent_folder_id)
    filesystem_path = os.path.join(EXPORT_PATH_PREFIX, given_file_path)
    response = gi.libraries.upload_from_galaxy_filesystem(library_id=LIBRARY_ID, filesystem_paths=filesystem_path, folder_id=parent_folder_id, file_type=file_type, link_data_only="link_to_files")
    return response[0]


def is_allowed_raw_path(raw_file_path):
    """
    Only allow processing of raw files contained in the allowed folders.
    raw_file_path = "./H2020_HBM4EU/2020/WP16_Specimen/HBM4EU_ESI_negative_WP_urine_MS1/HBM4EU_Fieldwork_1_Batch_2-20201013-CMV/RAW_profile/Tribrid_201013_060-322704_NEG_MU.raw"
    """
    is_allowed = False
    raw_dir, raw_file = os.path.split(raw_file_path)
    path = os.path.normpath(raw_dir)
    for allowed_name in ALLOWED_RAW_FILES_FOLDER_NAMES:
        if path.rfind(allowed_name) != -1:
            is_allowed = True
    return is_allowed


def ensure_library_link(file_path, file_type):
    library_dataset = get_library_dataset(file_path)
    if not library_dataset:
        log.info(f"Importing {file_path} dataset to library.")
        library_dataset = link_to_data_library(file_path, file_type)
    return library_dataset


def ensure_converted_links(raw_file_path):
    mzml_path = os.path.relpath(get_mzml_path(raw_file_path), EXPORT_PATH_PREFIX)
    json_path = f"{mzml_path[:-5]}.json"
    # txt_path = f"{mzml_path[:-5]}.txt"
    ensure_library_link(mzml_path, MZML_FILE_TYPE)
    ensure_library_link(json_path, JSON_FILE_TYPE)
    # txt_ld = ensure_library_link(txt_path, TXT_FILE_TYPE)


def main():
    with open(args.raw_list, "rt") as raw_list:
        for line in raw_list.readlines():
            raw_file_path = os.path.normpath(line.strip().strip("\n"))
            log.info(f"Processing path: {raw_file_path}")
            if not is_allowed_raw_path(raw_file_path):
                log.error(f"Skipping a line from the input. Found an illegal path: {raw_file_path}")
                continue
            if args.import_raw_only:
                ensure_library_link(raw_file_path, THERMO_FILE_TYPE)
            elif args.import_results_only:
                if is_converted(raw_file_path):
                    ensure_converted_links(raw_file_path)
                else:
                    log.debug(f"The following .RAW file {raw_file_path} is not converted, skipping.")
            elif not is_converted(raw_file_path):
                raw_library_dataset = ensure_library_link(raw_file_path, THERMO_FILE_TYPE)
                log.info("Invoking a conversion workflow.")
                run_conversion_workflow(raw_library_dataset, raw_file_path)
            else:
                ensure_converted_links(raw_file_path)
                # if needs_metadata(raw_file_path):
            sleep(random())  # Give Galaxy some time to cope since many calls above are async.


if __name__ == "__main__":
    main()

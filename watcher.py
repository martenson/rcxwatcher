#!/usr/bin/env python

import argparse
import os
import logging as log
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
args = parser.parse_args()

SALLY_PATH_PREFIX = "/mnt/sally/000020-Shares/rcx-da/"
LIBRARY_ID = "0a248a1f62a0cc04"
LIBRARY_NAME = "rcx-da"
LIBRARY_ROOT_FOLDER_ID = "F2f94e8ae9edff68a"
ALLOWED_MZML_FOLDER_NAMES = ["mzML_profile", "mzML", "mzml"]
ALLOWED_RAW_FILES_FOLDER_NAMES = ["RAW_profile", "RAW", "raw"]

gi = galaxy.GalaxyInstance(url=args.galaxy_url, key=args.apikey)
library_folders = gi.libraries.get_folders(library_id=LIBRARY_ID)


def needs_conversion(raw_file_path):
    """Check whether given raw file is already converted"""
    needs_conversion = True
    raw_dir, raw_file = os.path.split(raw_file_path)
    profile_dir = os.path.split(raw_dir)[0]
    for allowed_name in ALLOWED_MZML_FOLDER_NAMES:
        mzml_dir = os.path.join(profile_dir, allowed_name)
        mzml_file = os.path.join(mzml_dir, raw_file.replace("raw", "mzml"))
        # We could check the size for more reliability
        if os.path.exists(mzml_file):
            needs_conversion = False
    return needs_conversion


def find_library_dataset(raw_file_path):
    """Check whether given file is already linked to library, return the dataset if found."""
    library_dataset = None
    raw_dir, raw_file = os.path.split(raw_file_path)
    for folder in library_folders:
        if raw_dir == folder["name"].lstrip("/"):
            # Checking for file name equality only, warning: libraries allow name duplicates
            folder_detail = gi.folders.show_folder(folder_id=folder["id"], contents=True)
            for item in folder_detail["folder_contents"]:
                if raw_file == item["name"]:
                    log.debug("Found name identity")
                    library_dataset = item
                    break
    return library_dataset


def run_conversion_workflow(library_dataset, raw_file_path):
    #WORKFLOW_ID = create a workflow in galaxy
    workflow = galaxy.workflows.WorkflowClient(gi)
    raw_dir, raw_file = os.path.split(raw_file_path)
    profile_dir = os.path.split(raw_dir)[0]
    raw_file_id = library_dataset["id"]
    workflow.invoke_workflow(workflow_id=WORKFLOW_ID, 
        inputs={'0': {'id': raw_file_id, 'src': 'ld'}},
        params={'2': {'export_dir': profile_dir}})
        #can create a dedicated history so we don't get a new one each time the workflow is run
    
    #then clear up the history


def link_to_data_library(raw_file_path):
    raw_dir, raw_file = os.path.split(raw_file_path)
    path = os.path.normpath(raw_dir)
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
    filesystem_path = raw_file_path
    filesystem_path = os.path.join(SALLY_PATH_PREFIX, filesystem_path)
    ld = gi.libraries.upload_from_galaxy_filesystem(library_id=LIBRARY_ID, filesystem_paths=filesystem_path, folder_id=parent_folder_id, file_type="thermo.raw", link_data_only="link_to_files")
    return ld


def is_allowed_path(raw_file_path):
    is_allowed = False
    raw_dir, raw_file = os.path.split(raw_file_path)
    path = os.path.normpath(raw_dir)
    for allowed_name in ALLOWED_RAW_FILES_FOLDER_NAMES:
        if path.rfind(allowed_name) != -1:
            is_allowed = True
    return is_allowed


def main():
    with open(args.raw_list, "rt") as raw_list:
        for line in raw_list.readlines():
            raw_file_path = os.path.normpath(line.strip().strip("\n"))
            log.info(f"Processing path: {raw_file_path}")
            if not is_allowed_path(raw_file_path):
                log.error(f"Skipping a line from the input. Found an illegal path: {raw_file_path}")
                continue
            if needs_conversion(raw_file_path):
                log.info("Dataset needs conversion.")
                library_dataset = find_library_dataset(raw_file_path)
                if library_dataset:
                    log.info("Dataset has a corresponding library entry")
                    run_conversion_workflow(library_dataset, raw_file_path)
                else:
                    log.info("Importing dataset to library.")
                    library_dataset = link_to_data_library(raw_file_path)
                    run_conversion_workflow(library_dataset, raw_file_path)
            else:
                # File is already converted. One tea please.
                pass


if __name__ == "__main__":
    main()

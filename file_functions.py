# functionality for moving files, getting paths, etc

import os
import re
import shutil
import configparser

# list all files matching a specified regex within one filepath, case-insensitive
def list_dir(filepath,my_regex):
    res = [f for f in os.listdir(filepath) if re.search(my_regex,f,re.IGNORECASE)]
    return res

# copy a csv file to a queue folder
def csv_shutil(source_filepath, dest_filepath, block_size=65536):
    with open(source_filepath, "rb") as s_file, open(dest_filepath, "wb") as d_file:
        shutil.copyfileobj(s_file, d_file, block_size)

# read the paths from a .ini config file
config_file = "local.ini"
config = configparser.ConfigParser()
config.read(config_file)
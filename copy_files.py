# copy files to a queue folder, ready to be parsed

from file_functions import list_dir,csv_shutil,config

raw_path = config.get("Default","RAW_DIR")
queue_path = config.get("Default","QUEUE_DIR")

# copy files to queue
csv_files = list_dir(raw_path,".+\.(csv|gz)$")
for file in csv_files:
    source_filepath = raw_path+file
    dest_filepath = queue_path+file
    csv_shutil(source_filepath,dest_filepath)
    print(f"{source_filepath} moved to {dest_filepath}.")
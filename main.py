import os
import shutil
import filecmp
import logging
from logging.handlers import RotatingFileHandler
import schedule
import time
import argparse

# Configure logging
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

def log_message(message):
    logger.info(message)

def synchronize_folders(source_folder, replica_folder):
    # Ensure both folders exist
    if not os.path.exists(source_folder) or not os.path.exists(replica_folder):
        log_message("Source or replica folder does not exist.")
        return

    # Walk through the replica folder and remove files not present in the source folder
    for root, dirs, files in os.walk(replica_folder):
        relative_path = os.path.relpath(root, replica_folder)
        source_root = os.path.join(source_folder, relative_path)

        for file in files:
            source_file = os.path.join(source_root, file)
            replica_file = os.path.join(root, file)

            # Check if the file exists in the source folder
            if not os.path.exists(source_file):
                log_message(f"Deleting {replica_file}")
                os.remove(replica_file)

    # Walk through the source folder and compare with the replica folder
    for root, dirs, files in os.walk(source_folder):
        relative_path = os.path.relpath(root, source_folder)
        replica_root = os.path.join(replica_folder, relative_path)

        # Create missing directories in the replica folder
        if not os.path.exists(replica_root):
            os.makedirs(replica_root)

        for file in files:
            source_file = os.path.join(root, file)
            replica_file = os.path.join(replica_root, file)

            # Check if the file exists in the replica folder and if it's different
            if not os.path.exists(replica_file) or not filecmp.cmp(source_file, replica_file, shallow=False):
                log_message(f"Copying {source_file} to {os.path.normpath(replica_file)}")
                shutil.copy2(source_file, replica_file)

    log_message("Folders synchronized successfully!")

def periodic_sync(source_folder, replica_folder, interval_minutes):
    log_message(f"Starting synchronization every {interval_minutes} minutes.")
    schedule.every(interval_minutes).minutes.do(synchronize_folders, source_folder, replica_folder)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synchronize two folders at regular intervals.")
    parser.add_argument("source_folder", help="Path to the source folder")
    parser.add_argument("replica_folder", help="Path to the replica folder")
    parser.add_argument("sync_interval_minutes", type=int, help="Synchronization interval in minutes")
    parser.add_argument("log_file", help="Path to the log file")

    args = parser.parse_args()

    # Configure log file handler
    file_handler = RotatingFileHandler(args.log_file, maxBytes=1024*1024, backupCount=1)
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)

    periodic_sync(args.source_folder, args.replica_folder, args.sync_interval_minutes)
"""
helping functions
"""
import os
import re
import shutil


def make_new_folder(new_folder_path, folder_name):
    """Create the folder"""
    try:
        os.makedirs(new_folder_path)
        print(f"Folder '{folder_name}' created in the current directory.")
    except FileExistsError:
        print(f"Folder '{folder_name}' already exists in the current directory.")


def remove_regix(var):
    """
    remove regixs i.e. "/"
    """
    var = re.sub(r"\s+", "_", var.strip())
    var = re.sub(r"[^a-zA-Z0-9_]", "", var)

    return var


def delete_contents_of_directory(directory_path):
    try:
        # Iterate over all files and subdirectories in the given directory
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)
            # Check if it's a file, then delete it
            if os.path.isfile(item_path):
                os.unlink(item_path)
            # If it's a directory, recursively delete its contents
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
        print(f"All files and folders in {directory_path} have been deleted.")
    except Exception as e:
        print(f"Error: {e}")

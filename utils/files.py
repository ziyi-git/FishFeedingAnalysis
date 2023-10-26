import os
import subprocess
import concurrent.futures
import shutil


def retrieve_files(dir_path, extension, identifier):
    """
    Return file paths with specified extension (e.g. mp4) and identifier.

    Example:
    >>> retrieve_files("/X", ".mp4", "ln-szln")
    ["/X/第1次投喂数据/乐农水面摄食视频/ln-szln-p001-s0006_main_20221201110010_42.mp4", ...]
    """
    # Check if the directory exists
    if not os.path.isdir(dir_path):
        print(f"{dir_path} is not a valid directory.")
        return []
    
    # List to store the paths of all matching files
    matching_files = []
    
    # Walk through the directory
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            # Check if the file has the desired extension and contains the identifier in its path
            if file.endswith(extension) and identifier in os.path.join(root, file):
                matching_files.append(os.path.join(root, file))
    
    return matching_files

def make_dir(destination_path):
    """
    Create directory if destination_path not exist.
    Example:
    >>> make_dir("/A/B/C.mp4") create "/A/B/" if "/A/B/" not exist
    >>> make_dir("/A/B/")      create "/A/B/" if "/A/B/" not exist
    >>> make_dir("/A/B")       create "/A/"   if "/A/" not exist
    """
    # Check if the destination directory exists
    destination_dir = os.path.dirname(destination_path)
    if not os.path.exists(destination_dir):
        try:
            # Try to create the directory
            os.makedirs(destination_dir)
            print(f"Directory {destination_dir} created.")
        except OSError:
            print(f"Could not create directory {destination_dir}.")
            return False

def copy_file(src, dst):
    try:
        shutil.copy(src, dst)
        print(f"Video file {src} copied to {dst}.")
        return True
    except shutil.Error as e:
        print(f"Coulds not copy video file: {e}")
        return False

def remove_ds_store(directory):
    # 遍历文件夹及其所有子文件夹
    for root, dirs, files in os.walk(directory):
        
        # 删除名为 .DS_Store 的文件
        if '.DS_Store' in files:
            try:
                os.remove(os.path.join(root, '.DS_Store'))
                print(f"Deleted .DS_Store from {root}")
            except Exception as e:
                print(f"Error deleting .DS_Store from {root}. Reason: {e}")
        
        # 删除名为 .DS_Store 的文件夹
        if '.DS_Store' in dirs:
            try:
                shutil.rmtree(os.path.join(root, '.DS_Store'))
                print(f"Deleted .DS_Store directory from {root}")
            except Exception as e:
                print(f"Error deleting .DS_Store directory from {root}. Reason: {e}")

def exec_command(command, message_callback=None):
    print(command)
    try:
        subprocess.run(command, check=True)
        print(f"Exec {command} successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Exec {command} failed: {e.output}")
        if message_callback is not None:
            message_callback(command)

    
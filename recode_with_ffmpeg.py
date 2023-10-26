import os
import time
import argparse
import concurrent.futures
from collections import defaultdict

from utils.files import retrieve_files, make_dir, exec_command

def concat_clips(src_root, dst_root, max_workers=4, extension=".mp4", identifier = "ln-szln"):
    """
    Concatenates video clips from the source directory and saves the resulting video to the destination directory.
    
    Parameters:
    - src_root (str): Path to the source directory containing video clips.
    - dst_root (str): Path to the destination directory where the concatenated video will be saved.
    - max_workers (int, optional): Number of processes to use for concatenation. Defaults to 4.
    - extension (str, optional): File extension to filter video clips in the source directory. Defaults to ".mp4".
    - identifier (str, optional): String that must be present in video filenames from the source directory. Used to filter relevant clips. Defaults to "ln-szln".
    
    Returns:
    None
    """
    src_files = retrieve_files(src_root, extension, identifier)
    src_dirs = list(set([os.path.dirname(rf) for rf in src_files]))

    ffmpeg_param1s = []
    ffmpeg_param2s = []

    for src_dir in src_dirs:
        for dir, _, files in os.walk(src_dir):
            # files = [f for f in files if '.DS_Store' not in f]
            groups = defaultdict(list)
            for f in files:
                substring = f[:18]
                groups[substring].append(f)
            grouped_files_list = list(groups.values())
            for gf in grouped_files_list:
                gf_sorted = sorted(gf)
                fp1 = [dir + "/" + gs for gs in gf_sorted]
                ffmpeg_param1s.append(fp1)
                fp2 = fp1[0]
                ffmpeg_param2s.append(fp2)
    
    ffmpeg_param2s = [fp2.replace(src_root, dst_root) for fp2 in ffmpeg_param2s]
    temp = []
    for (fp1, fp2) in zip(ffmpeg_param1s, ffmpeg_param2s):
        make_dir(fp2)
        filelist = fp2.replace(".mp4", ".txt") #  os.path.dirname(fp2) + "/filelist.txt"
        temp.append(filelist)
        with open(filelist, "w") as f:
            for element in fp1:
                f.write(f"file '{element}'\n")
    ffmpeg_param1s = temp
    
    t1 = time.time()

    base_commands = [
        ["ffmpeg", "-f", "concat", "-safe", "0", "-i", fp1, "-c", "copy", fp2] for (fp1, fp2) in zip(ffmpeg_param1s, ffmpeg_param2s)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(exec_command, base_command, fm_callback) for base_command in base_commands]
        for future in concurrent.futures.as_completed(futures):
            pass  # Optionally, do something with the results

    t2 = time.time()
    print("Duration for all files: ", t2 - t1)


def resize_video(src_root, dst_root, dst_size, max_workers=4, extension=".mp4", identifier = "ln-szln"):
    """
    Resizes videos from the source directory and saves them to the destination directory.
    
    Parameters:
    - src_root (str): Path to the source directory containing videos to be resized.
    - dst_root (str): Path to the destination directory where the resized videos will be saved.
    - dst_size (str): Size specification to which videos should be resized, e.g., "1280x720".
    - max_workers (int, optional): Number of processes to use for resizing. Defaults to 4.
    - extension (str, optional): File extension to filter videos in the source directory. Defaults to ".mp4".
    - identifier (str, optional): String that must be present in video filenames from the source directory. Used to filter relevant videos. Defaults to "ln-szln".
    
    Returns:
    None
    """
    src_files = retrieve_files(src_root, extension, identifier)
    dst_files = [sf.replace(src_root, dst_root) for sf in src_files]

    for df in dst_files:
        make_dir(df)

    import time
    t1 = time.time()

    base_commands = [['ffmpeg', '-i', sf, '-c:v', 'h264_videotoolbox', '-vf', dst_size, '-q:v', '50', df] for (sf, df) in zip(src_files, dst_files)]
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # futures = [executor.submit(resize_and_mask_video, sf, df) for (sf, df) in zip(src_files, dst_files)]
        futures = [executor.submit(exec_command, base_command) for base_command in base_commands]
        for future in concurrent.futures.as_completed(futures):
            pass  # Optionally, do something with the results

    t2 = time.time()
    print("Duration for recode all files: ", t2 - t1)  # Duration for recode all files:  6437.959105014801


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="A tool to either resize videos or concatenate clips from a source directory. "
                    "If 'dst_size' is provided, videos will be resized. Otherwise, clips will be concatenated.\n\n"
                    "Usage Examples:\n"
                    "1. Resize videos: python recode_with_ffmpeg.py src_path dst_path --dst_size 1280x720\n"
                    "2. Concatenate clips: python recode_with_ffmpeg.py src_path dst_path\n",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("src_root", type=str, help="Path to the source directory.")
    parser.add_argument("dst_root", type=str, help="Path to the destination directory.")
    parser.add_argument("--dst_size", type=str, help="Size specification to which videos should be resized, e.g., '1280x720'.")
    parser.add_argument("--max_workers", type=int, default=4, help="Number of processes to use.")
    parser.add_argument("--extension", type=str, default=".mp4", help="File extension to filter videos in the source directory.")
    parser.add_argument("--identifier", type=str, default="ln-szln", help="String that must be present in video filenames from the source directory.")
    
    args = parser.parse_args()
    
    if args.dst_size:
        resize_video(args.src_root, args.dst_root, args.dst_size, args.max_workers, args.extension, args.identifier)
    else:
        concat_clips(args.src_root, args.dst_root, args.max_workers, args.extension, args.identifier)


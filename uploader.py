from typing import List
import argparse 
import subprocess
import concurrent.futures
import os
from pathlib import Path

import dropbox
import filetype
from filetype.types import image

parser = argparse.ArgumentParser(description='Sync convert all images in given dir and upload to Dropbox')
parser.add_argument('folder', help='Folder name in your Dropbox')
parser.add_argument('rootdir', help='Local directory to use')
parser.add_argument('token', help='Dropbox sdk token')
parser.add_argument('--temp_dir', help='A temp dir to use') 

_ENV = {
    **os.environ, 
    "MAGICK_THREAD_LIMIT": "1",
    "MAGICK_MEMORY_LIMIT": "2GB",
}

def main():
    args = parser.parse_args()
    print("Uploading to: ", args.folder)
    print("Reading from: ", args.rootdir)

    dest_base = Path(args.folder)
    if not dest_base.is_absolute():
        raise Exception("Dropbox wants absolute paths.")
    rootdir = Path(args.rootdir)
    if not rootdir.exists():
        raise Exception("Rootdir doesn't exist")
    dbx = dropbox.Dropbox(args.token, user_agent="__DropboxUploader/1.0")

    if args.temp_dir is not None:
        _ENV["MAGICK_TEMPORARY_PATH"] = args.temp_dir

    present_files = build_cache(dbx, dest_base)
    print("Cache built, found %d files." % len(present_files))

    skipped_files: List[Path] = []
    excepted_files: List[Path] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_file = {}
        for f in rootdir.glob("**/*"):
            if not f.is_file():
                continue
            image_type = filetype.image_match(os.fspath(f))
            if image_type is None:
                skipped_files.append(f)
                continue
            future_to_file[executor.submit(upload_task, dbx, f, image_type, rootdir, dest_base, present_files)] = f

        for future in concurrent.futures.as_completed(future_to_file):
            file = future_to_file[future]
            try:
                data = future.result()
            except Exception as exc:
                excepted_files.append(file)
                print('File %s generated an exception: %s' % (file, exc))

    print("Finished uploading all files.")
    print("Skipped the following:")
    print('\n'.join(str(p) for p in skipped_files))
    print("The following generated execptions:")
    print('\n'.join(str(p) for p in excepted_files))


def build_cache(dbx, upload_base: Path) -> frozenset:
    entries = set()
    res = dbx.files_list_folder(str(upload_base), recursive=True)
    for entry in res.entries:
        entries.add(entry.path_lower)
    while res.has_more:
        res = dbx.files_list_folder_continue(res.cursor)
        for entry in res.entries:
            entries.add(entry.path_lower)

    return frozenset(entries)


def upload_task(dbx, file: Path, im_type, local_base: Path, upload_base:Path, present_files):
    dest_path = str(upload_base / file.relative_to(local_base).with_suffix('.jpg'))
    if dest_path.lower() in present_files:
        print("Finished %s (already present)" % dest_path)
        return

    if isinstance(im_type, image.Jpeg):
        upload_to_dropbox(dbx, dest_path, file.read_bytes())
        print("Finsihed %s (copied)" % dest_path)
    else:
        upload_to_dropbox(dbx, dest_path, convert(file))
        print("Finsihed %s (converted)" % dest_path)



def convert(src: Path) -> bytes:
    return subprocess.run(["convert", '-quality', '97', str(src), 'jpeg:-'], capture_output=True, check=True, env=_ENV).stdout


def upload_to_dropbox(dbx, path: str, data: bytes):
    dbx.files_upload(f=data, path=path, mute=True, mode=dropbox.files.WriteMode.add)


if __name__ == '__main__':
    main()

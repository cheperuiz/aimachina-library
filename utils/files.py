import zipfile
import tarfile
import os


def extract_archive(filename, path=None):
    path = path or os.path.dirname(filename)
    if tarfile.is_tarfile(filename):
        with tarfile.TarFile(filename) as tf:
            tf.extractall(path)
        # Extract tarball
    elif zipfile.is_zipfile(filename):
        with zipfile.ZipFile(filename) as zf:
            zf.extractall(path)
    else:
        raise ValueError("Archive format is not supported.")


def remove_dir(top):
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(top)


def extension(filename):
    return filename.rsplit(".", 1)[-1].lower()

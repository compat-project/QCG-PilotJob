import os
import re


AUX_DIR_PTRN = re.compile(r'\.qcgpjm-service-.*')


def find_aux_dirs(path):
    """
    Find in given path directories which names matches auxiliary directory name pattern.

    :param path: path where auxiliary directories will be searched
    :return: list of paths with directories matches auxiliary name pattern
    """
    apath = os.path.abspath(path)
    return [os.path.join(apath, entry) for entry in os.listdir(apath) \
            if AUX_DIR_PTRN.match(entry) and os.path.isdir(os.path.join(apath, entry))]


def find_single_aux_dir(path):
    """
    Find exactly one auxiliary directory in given path, in other case raise exception.

    :param path: path where to find auxiliary directory
    :return: auxiliary directory path in given path
    """
    paths = find_aux_dirs(path)
    if len(paths) == 0:
        raise Exception('No auxiliary directory exist in path {}'.format(path))

    if len(paths) > 1:
        raise Exception('Too many auxiliary directories in given path ({})'.format(','.join(paths)))

    return paths[0]

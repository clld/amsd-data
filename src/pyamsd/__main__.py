import os
import sys
import json
import shutil
import errno
import requests

from pathlib import Path
from collections import OrderedDict
from itertools import groupby

from tqdm import tqdm
from clldutils.clilib import ArgumentParserWithLogging, command
from clldutils.dsv import UnicodeWriter
from clldutils.path import md5, write_text
from cdstarcat import Catalog, Object

from pyamsd.api import Amsd


def get_catalog(args):
    return Catalog(
        args.repos / 'images' / 'catalog.json',
        cdstar_url=os.environ.get('CDSTAR_URL', 'https://cdstar.shh.mpg.de'),
        cdstar_user=os.environ.get('CDSTAR_USER'),
        cdstar_pwd=os.environ.get('CDSTAR_PWD'),
    )


def _api(args):
    return amsd(repos=args.repos)


@command()
def upload_mediafiles(args):
    """
    Uploads media files from the passed directory to the CDSTAR server,
    if an object identified by metadata's 'name' exists it will be deleted first
    """
    supported_types = {'imagefile': ['png', 'gif', 'jpg', 'jpeg', 'tif', 'tiff'],
                       'pdffile':   ['pdf'],
                       'moviefile': ['mp4']}

    with get_catalog(args) as cat:
        name_map = {obj.metadata['name']: obj for obj in cat}

        for ifn in sorted(Path(args.args[0]).iterdir()):
            print(ifn.name)

            fmt = ifn.suffix[1:].lower()
            meta_type = None
            for k in supported_types:
                if fmt in supported_types[k]:
                    meta_type = k
                    break
            if meta_type is None:
                print('No supported media format - skipping {0}'.format(fmt))
                continue

            md = {
                'collection': 'amsd',
                'name': ifn.stem,
                'type': meta_type,
                'path': ifn.name
            }

            # Create the new object
            for (fname, created, obj) in cat.create(str(ifn), md):
                args.log.info('{0} -> {1} object {2.id}'.format(
                    fname, 'new' if created else 'existing', obj))


def main():  # pragma: no cover
    parser = ArgumentParserWithLogging('pyamsd')
    parser.add_argument(
        '--repos',
        help="path to amsd-data repository",
        type=Path,
        default=Path('.'))
    sys.exit(parser.main())


if __name__ == '__main__':
    main()

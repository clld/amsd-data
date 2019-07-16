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
from clldutils.path import md5, write_text, as_unicode
from cdstarcat import Catalog, Object

from pyamsd.api import Amsd

from .to_csv import to_csv

def get_catalog(args):
    return Catalog(
        args.repos / 'images' / 'catalog.json',
        cdstar_url=os.environ.get('CDSTAR_URL', 'https://cdstar.shh.mpg.de'),
        cdstar_user=os.environ.get('CDSTAR_USER'),
        cdstar_pwd=os.environ.get('CDSTAR_PWD'),
    )


@command()
def upload_mediafiles(args):
    """
    Uploads media files from the passed directory to the CDSTAR server,
    if an object identified by metadata's 'name' exists it will be deleted first
    """
    supported_types = {'imagefile': ['png', 'gif', 'jpg', 'jpeg', 'tif', 'tiff'],
                       'pdffile':   ['pdf'],
                       'moviefile': ['mp4']}

    if not args.args or not Path(args.args[0]).exists():
        print("Error: Upload path does not exist")
        exit(1)

    with get_catalog(args) as cat:
        name_map = {obj.metadata['name']: obj for obj in cat}

        for ifn in sorted(Path(args.args[0]).iterdir()):
            print(ifn.name)

            fmt = ifn.suffix[1:].lower()
            meta_type = None
            for t, suffixes in supported_types.items():
                if fmt in suffixes:
                    meta_type = t
                    break
            if meta_type is None:
                print('No supported media format - skipping {0}'.format(fmt))
                continue

            md = {
                'collection': 'amsd',
                'name': as_unicode(ifn.stem),
                'type': meta_type,
                'path': as_unicode(ifn.name)
            }

            # Create the new object
            for (fname, created, obj) in cat.create(str(ifn), md):
                args.log.info('{0} -> {1} object {2.id}'.format(
                    fname, 'new' if created else 'existing', obj))


@command()
def check(args):
    Amsd(args.repos).validate()


def main():  # pragma: no cover
    parser = ArgumentParserWithLogging('pyamsd')
    parser.add_argument(
        '--repos',
        help="path to amsd-data repository",
        type=Path,
        default=Path(__file__).resolve().parent.parent.parent)
    sys.exit(parser.main())


if __name__ == '__main__':
    main()

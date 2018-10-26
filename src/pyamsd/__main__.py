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


def _get_catalog(args, cattype = 'imagefiles'):
    if cattype == 'imagefiles':
        return Catalog(
            args.repos / 'imagefiles' / 'catalog.json',
            cdstar_url=os.environ.get('CDSTAR_URL', 'https://cdstar.shh.mpg.de'),
            cdstar_user=os.environ.get('CDSTAR_USER'),
            cdstar_pwd=os.environ.get('CDSTAR_PWD'),
        )


def _api(args):
    return amsd(repos=args.repos)


@command()
def upload_images(args):
    """
    Uploads image files from the passed directory to the CDSTAR server,
    if an object identified by metadata's 'name' exists it will be deleted first
    """

    supported_image_types = ['png', 'gif', 'jpg', 'jpeg', 'tif', 'tiff']

    with _get_catalog(args, 'imagefiles') as cat:

        name_map = {obj.metadata['name']: obj for obj in cat}

        for ifn in sorted(Path(args.args[0]).iterdir()):

            print(ifn.name)

            if ifn.suffix[1:].lower() not in supported_image_types:
                print('No supported image format - skipping {0}'.format(ifn.name))
                continue

            # Lookup the image name in catalog:
            stem = ifn.stem
            cat_obj = name_map[stem] if stem in name_map else None

            # if it exists delete it
            if cat_obj:
                args.log.info('Delete exisiting object %s for %s' % (cat_obj.id, ifn.name))
                cat.delete(cat_obj.id)

            md = {'collection': 'amsd',
                    'name': stem,
                    'type': 'imagefile',
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
        default=Path(__file__).resolve().parent.parent.parent)
    parser.add_argument('--amsd-repo',
                        type=Path,
                        default=Path(__file__).resolve().parent.parent.parent / 'amsd')
    sys.exit(parser.main())


if __name__ == '__main__':
    main()

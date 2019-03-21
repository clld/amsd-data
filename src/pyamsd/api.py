import collections

from csvw.dsv import reader
from clldutils.apilib import API
from clldutils import jsonlib


class Amsd(API):
    @property
    def media_catalog(self):
        return jsonlib.load(self.repos / 'images' / 'catalog.json')

    @property
    def rows(self):
        return list(reader(self.repos / 'org_data' / 'records.tsv', delimiter='\t', dicts=True))

    def validate(self):
        media = set(v['metadata']['path'] for v in self.media_catalog.values())
        missing = collections.Counter()

        for row in self.rows:
            for name in row['Linked Filename'].split(';'):
                name = name.strip()
                if name:
                    if name not in media:
                        missing.update([name])
        for i, (k, v) in enumerate(sorted(missing.items(), key=lambda i: (-i[1], i[0]))):
            if i == 0:
                print('Missing files:')
            print('{0} -- {1}x'.format(k, v))

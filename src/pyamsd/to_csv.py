from clldutils.clilib import ArgumentParserWithLogging, command
from csvw.dsv import UnicodeWriter, UnicodeReader
from collections import OrderedDict, defaultdict
from pathlib import Path
from cdstarcat import Catalog
import csv
import sys
import re
import os


# [separated table, old header, new header, split regex]
fields = [
    [0, 'AMSD ID', 'amsd_id', ''],
    [0, 'Title', 'title', ''],
    [1, 'Keywords', 'keywords', r' {2,}'],
    [0, 'Description', 'description', ''],
    [0, 'Creator of Object', 'obj_creator', ''],
    [0, 'Date Created', 'date_created', ''],
    [0, 'Notes on date created', 'note_place_created', ''],
    [0, 'Place Created', 'place_created', ''],
    [1, 'Item type', 'item_type', ''],
    [1, 'Subtype', 'item_subtype', ''],
    [1, 'Cultural region', 'cultural_region', ''],
    [1, 'Linguistic area', 'ling_area_1', r'Chirila\s*:\s*(.*?)  +Austlang\s*:\s*(.*?)\s*:(.*?)  +Glottolog\s*:\s*(.*)\s*'],
    [1, 'Linguistic area 2', 'ling_area_2', r'Chirila\s*:\s*(.*?)  +Austlang\s*:\s*(.*?)\s*:(.*?)  +Glottolog\s*:\s*(.*)\s*'],
    [1, 'Linguistic area 3', 'ling_area_3', r'Chirila\s*:\s*(.*?)  +Austlang\s*:\s*(.*?)\s*:(.*?)  +Glottolog\s*:\s*(.*)\s*'],
    [0, 'Notes on Linguistic area(s)', 'notes_ling_area', ''],
    [0, "Term for 'message stick' (or related) in language", 'stick_term', ''],
    [0, 'Message', 'message', ''],
    [0, 'Motifs', 'motifs', ''],
    [0, 'Motif transcription', 'motif_transcription', ''],
    [1, 'Semantic domain', 'sem_domain', r' {2,}'],
    [0, 'Dimension 1 (mm)', 'dim_1', ''],
    [0, 'Dimension 2 (mm)', 'dim_2', ''],
    [0, 'Dimension 3 (mm)', 'dim_3', ''],
    [1, 'Material', 'material', r' *, *|  +'],
    [1, 'Technique', 'technique', r' *, *'],
    [1, 'Source citation', 'source_citation', r'  +| *; '],
    [1, 'Source type', 'source_type', r'  +'],
    [0, 'Date Collected', 'date_collected', ''],
    [1, 'Institution/Holder: file', 'holder_file', r'  +'],
    [0, 'Institution/Holder: object identifier', 'holder_obj_id', ''],
    [0, 'Collector', 'collector', ''],
    [0, 'Place Collected', 'place_collected', ''],
    [0, 'Creator Copyright', 'creator_copyright', ''],
    [0, 'File Copyright', 'file_copyright', ''],
    [0, 'Latitude', 'lat', ''],
    [0, 'Longitude', 'long', ''],
    [0, 'Notes on coordinates', 'notes_coords', ''],
    [0, 'URL (collecting institution)', 'url_institution', ''],
    [0, 'URL (source document)', 'url_source_1', ''],
    [0, 'URL (source document 2)', 'url_source_2', ''],
    [0, 'IRN', 'irn', ''],
    [0, 'Notes', 'notes', ''],
    [1, 'Data entry (OCCAMS)', 'data_entry', r'  +'],
    [1, 'Linked Filename', 'linked_filenames', r' *; *']
]

fields_not_in_sticks = [
    'material',
    'technique',
    'keywords',
    'sem_domain',
    'linked_filenames',
    'item_type',
    'source_type',
    'source_citation'
]


def sim(s, t):
    if s == t:
        return 0
    elif len(s) == 0:
        return len(t)
    elif len(t) == 0:
        return len(s)
    v0 = [None] * (len(t) + 1)
    v1 = [None] * (len(t) + 1)
    for i in range(len(v0)):
        v0[i] = i
    for i in range(len(s)):
        v1[0] = i + 1
        for j in range(len(t)):
            cost = 0 if s[i] == t[j] else 1
            v1[j + 1] = min(v1[j] + 1, v0[j + 1] + 1, v0[j] + cost)
        for j in range(len(v0)):
            v0[j] = v1[j]
    return v1[len(t)]


def dms2dec(c):
    deg, min, sec, dir = re.split('[°\'"]', c)
    return round(
        (float(deg) + float(min) / 60 + float(sec) / 3600) * (
            -1 if dir.strip().lower() in ['w', 's'] else 1), 6)


def get_catalog():
    return Catalog(
        Path(__file__).resolve().parent.parent.parent / 'images' / 'catalog.json',
        cdstar_url=os.environ.get('CDSTAR_URL', 'https://cdstar.shh.mpg.de'),
        cdstar_user=os.environ.get('CDSTAR_USER'),
        cdstar_pwd=os.environ.get('CDSTAR_PWD'),
    )


@command()
def to_csv(args):
    """
    Parses data file 'org_data/records.tsv' into single csv files into 'raw'.
    In addition it outputs given warnings while parsing. If you only want to check
    the data integrity of data file 'org_data/records.tsv' then pass the argument
    'check' -> amsd to_csv check.
    """

    raw_path = Path(__file__).resolve().parent.parent.parent / 'raw'
    if not raw_path.exists():
        raw_path.mkdir()

    csv_dataframe = {
        'sticks': [],
        'keywords': {},
        'sem_domain': {},
        'linked_filenames': {},
        'item_type': {},
        'item_subtype': {},
        'cultural_region': {},
        'material': {},
        'technique': {},
        'ling_area': {},
        'source_citation': {},
        'source_type': {},
        'holder_file': {},
        'data_entry': {},
    }

    datafile = Path(__file__).resolve().parent.parent.parent / 'org_data' / 'records.tsv'

    with UnicodeReader(datafile, delimiter='\t') as reader:
        for i, row in enumerate(reader):
            data = []
            if i == 0:  # header
                data.append('pk')  # add pk
                for j, col in enumerate(row):
                    data.append(fields[j][2].strip())
            else:
                data.append(i)  # add id
                for j, col_ in enumerate(row):
                    if j > 43 and len(col_):
                        print('Error: too many filled columns for line {0}'.format(i + 1))
                        continue
                    if re.sub(r'[ ]+', '', col_) == '':
                        data.append('')
                    else:
                        col = col_.strip()
                        if fields[j][2] in fields_not_in_sticks \
                                and fields[j][2] not in ['linked_filenames', 'source_citation']:
                            col = col.lower()
                        if fields[j][0] == 0:
                            if fields[j][2] in ['lat', 'long']:
                                try:
                                    data.append(dms2dec(col))
                                except ValueError:
                                    print('Error: check lat/long notation in line {0} for "{1}"'.format(
                                        i + 1, col))
                                    data.append(None)
                            else:
                                data.append(col)
                        elif fields[j][0] == 1 and len(fields[j][3]) == 0:
                            if col not in csv_dataframe[fields[j][2]]:
                                csv_dataframe[fields[j][2]][col] = len(csv_dataframe[fields[j][2]]) + 1
                            data.append(csv_dataframe[fields[j][2]][col])
                        elif fields[j][0] == 1 and len(fields[j][3]) > 1:
                            ref_data = []
                            if re.match(r'^ling_area_\d+$', fields[j][2]):
                                try:
                                    data_array = ["|".join([i.strip() for i in list(
                                        re.findall(fields[j][3], col)[0])])]
                                except IndexError:
                                    print('Error: {0} in line {1} has wrong structure: {2}'.format(
                                        fields[j][2], i + 1, col))
                                    data_array = []
                            else:
                                data_array = re.split(fields[j][3], col)
                            for item_ in data_array:
                                item = item_.strip()
                                col_name = fields[j][2]
                                if re.match(r'^ling_area_\d+$', col_name):
                                    col_name = 'ling_area'
                                    if item not in csv_dataframe[col_name]:
                                        csv_dataframe[col_name][item] = len(csv_dataframe[col_name]) + 1
                                    ref_data.append(csv_dataframe[col_name][item])
                                elif col_name in ['holder_file']:
                                    if item not in csv_dataframe[col_name]:
                                        csv_dataframe[col_name][item] = len(csv_dataframe[col_name]) + 1
                                    ref_data.append(csv_dataframe[col_name][item])
                                else:
                                    dfkey = 'x_sticks_' + col_name
                                    if item not in csv_dataframe[col_name]:
                                        csv_dataframe[col_name][item] = len(csv_dataframe[col_name]) + 1
                                    if not csv_dataframe[col_name][item] in ref_data:
                                        ref_data.append(csv_dataframe[col_name][item])
                                        if dfkey not in csv_dataframe:  # header
                                            csv_dataframe[dfkey] = []
                                            csv_dataframe[dfkey].append(['stick_pk', col_name + '_pk'])
                                        csv_dataframe[dfkey].append([i, csv_dataframe[col_name][item]])
                            # save ids to related table as semicolon separated lists of ids
                            data.append(';'.join(map(str, ref_data)))
            csv_dataframe['sticks'].append(data)

    with get_catalog() as cat:
        images_objs = {obj.metadata['name']: obj for obj in cat}

    # look for similar entries
    for t, k in [('source_citation', 5), ('holder_file', 4), ('ling_area', 10), ('material', 1),
                 ('data_entry', 2), ('item_type', 2), ('item_subtype', 2), ('cultural_region', 2)]:
        check_sim = list(csv_dataframe[t].keys())
        for i in range(len(check_sim)):
            for j in range(i + 1, len(check_sim)):
                if sim(check_sim[i], check_sim[j]) < k:
                    print('sim check: %s\n%s\n%s\n' % (t, check_sim[i], check_sim[j]))

    # look for unique AMSD IDs
    unique_ids_check = defaultdict(int)
    for s in csv_dataframe['sticks']:
        if s[1].strip():
            unique_ids_check[s[1]] += 1
    for k, v in unique_ids_check.items():
        if v > 1:
            print('AMSD ID check: {0} occurs {1} times'.format(k, v))

    if not args.args or args.args[0].lower() != 'check':
        for filename, data in csv_dataframe.items():
            with UnicodeWriter(raw_path.joinpath(filename + '.csv')) as writer:
                if type(data) is list:
                    for item in data:
                        writer.writerow(item)
                else:
                    d = []
                    if filename == 'ling_area':
                        d.append(['pk', 'chirila_name', 'austlang_code',
                                 'austlang_name', 'glottolog_code'])
                        for k, v in data.items():
                            c, ac, an, g = re.split(r'\|', k)
                            if g == 'no code':
                                g = ''
                            d.append([v, c, ac, an, g])
                    elif filename == 'linked_filenames':
                        d.append(['pk', 'name', 'oid', 'path'])
                        for k, v in data.items():
                            k_ = os.path.splitext(k)[0]
                            if k_ in images_objs:
                                url_path = ''
                                for o in images_objs[k_].bitstreams:
                                    if o.id not in ['thumbnail.jpg', 'web.jpg']:
                                        url_path = o.id
                                        break
                                if url_path == '':
                                    print("no path found for %s" % (k_))
                                d.append([v, k, images_objs[k_].id, url_path])
                            else:
                                print("no image match for '%s'" % (k))
                                d.append([v, k, ''])
                    else:
                        d.append(['pk', 'name'])
                        for k, v in data.items():
                            d.append([v, k])
                    for item in d:
                        writer.writerow(item)

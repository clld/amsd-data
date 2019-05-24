from clldutils.clilib import ArgumentParserWithLogging
from clldutils.dsv import UnicodeWriter, UnicodeReader
from collections import OrderedDict
from pathlib import Path
from cdstarcat import Catalog
import csv
import sys
import re
import os


# usage: python to_csv.py {PATH_TO_records.csv}

# [separated table, old header, new header, split regex]
fields = [
    [0,'AMSD ID', 'amsd_id', ''],
    [0,'Title', 'title', ''],
    [1,'Keywords', 'keywords', r' {2,}'],
    [0,'Description', 'description', ''],
    [0,'Creator of Object', 'obj_creator', ''],
    [0,'Date Created', 'date_created', ''],
    [0,'Notes on date created', 'note_place_created', ''],
    [0,'Place Created', 'place_created', ''],
    [1,'Item type', 'item_type', ''],
    [1,'Linguistic area',   'ling_area_1', r'Chirila\s*:\s*(.*?)  +Austlang\s*:\s*(.*?)\s*:(.*?)  +Glottolog\s*:\s*(.*)\s*'],
    [1,'Linguistic area 2', 'ling_area_2', r'Chirila\s*:\s*(.*?)  +Austlang\s*:\s*(.*?)\s*:(.*?)  +Glottolog\s*:\s*(.*)\s*'],
    [1,'Linguistic area 3', 'ling_area_3', r'Chirila\s*:\s*(.*?)  +Austlang\s*:\s*(.*?)\s*:(.*?)  +Glottolog\s*:\s*(.*)\s*'],
    [0,'Notes on Linguistic area(s)', 'notes_ling_area', ''],
    [0,"Term for 'message stick' (or related) in language", 'stick_term', ''],
    [0,'Message', 'message', ''],
    [0,'Motifs', 'motifs', ''],
    [0,'Motif transcription', 'motif_transcription', ''],
    [1,'Semantic domain', 'sem_domain', r' {2,}'],
    [0,'Dimension 1 (mm)', 'dim_1', ''],
    [0,'Dimension 2 (mm)', 'dim_2', ''],
    [0,'Dimension 3 (mm)', 'dim_3', ''],
    [1,'Material', 'material', r' *, *|  +'],
    [1,'Technique', 'technique', r' *, *'],
    [1,'Source citation', 'source_citation', r'  +| *; '],
    [1,'Source type', 'source_type', r'  +'],
    [0,'Date Collected', 'date_collected', ''],
    [1,'Institution/Holder: file', 'holder_file', r'  +'],
    [0,'Institution/Holder: object identifier', 'holder_obj_id', ''],
    [0,'Collector', 'collector', ''],
    [0,'Place Collected', 'place_collected', ''],
    [0,'Creator Copyright', 'creator_coyright', ''],
    [0,'File Copyright', 'file_copyright', ''],
    [0,'Latitude', 'lat', ''],
    [0,'Longitude', 'long', ''],
    [0,'Notes on coordinates', 'notes_coords', ''],
    [0,'URL (collecting institution)', 'url_institution', ''],
    [0,'URL (source document)', 'url_source_1', ''],
    [0,'URL (source document 2)', 'url_source_2', ''],
    [0,'IRN', 'irn', ''],
    [0,'Notes', 'notes', ''],
    [1,'Data entry (OCCAMS)', 'data_entry', r'  +'],
    [1,'Linked Filename', 'linked_filenames', r' *; *']
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

# def write(self):
#     with dsv.UnicodeWriter(self.path) as writer:
#         writer.writerow(self.header)
#         for item in self.items:
#             writer.writerow(item.csv_row())

def dms2dec(c):
    deg, min, sec, dir = re.split('[°\'"]', c)
    return round((float(deg) + float(min)/60 + float(sec)/(60*60)) * (-1 if dir.lower() in ['w', 's'] else 1), 6)

def get_catalog():
    return Catalog(
        Path(__file__).resolve().parent.parent.parent / 'images' / 'catalog.json',
        cdstar_url=os.environ.get('CDSTAR_URL', 'https://cdstar.shh.mpg.de'),
        cdstar_user=os.environ.get('CDSTAR_USER'),
        cdstar_pwd=os.environ.get('CDSTAR_PWD'),
    )

def main():

    raw_path = Path(__file__).resolve().parent.parent.parent / 'raw'
    if not raw_path.exists():
        raw_path.mkdir()

    csv_dataframe = {
        'sticks': []
        ,'keywords': {} 
        ,'sem_domain': {}
        ,'linked_filenames': {}
        ,'item_type': {}
        ,'material': {}
        ,'technique': {}
        ,'ling_area': {}
        ,'source_citation': {}
        ,'source_type': {}
        ,'holder_file': {}
        ,'data_entry': {}
    }

    with UnicodeReader(Path(sys.argv[1]), delimiter='\t') as reader:
        for i, row in enumerate(reader):
            if len(row) != 42:
                print("Error count of columns in line " + i)
                exit(1)
            data = []
            if i == 0: #header
                data.append('pk') # add pk
                for j, col in enumerate(row):
                    data.append(fields[j][2].strip())
            else:
                data.append(i) # add id
                for j, col_ in enumerate(row):
                    if re.sub(r'[ ]+', '', col_) == '':
                        data.append('')
                    else:
                        col = col_.strip()
                        if fields[j][2] in fields_not_in_sticks \
                                and fields[j][2] not in ['linked_filenames', 'source_citation']:
                            col = col.lower()
                        if fields[j][0] == 0:
                            if fields[j][2] in ['lat', 'long']:
                                data.append(dms2dec(col))
                            else:
                                data.append(col)
                        elif fields[j][0] == 1 and len(fields[j][3]) == 0:
                            if col not in csv_dataframe[fields[j][2]]:
                                csv_dataframe[fields[j][2]][col] = len(csv_dataframe[fields[j][2]]) + 1
                            data.append(csv_dataframe[fields[j][2]][col])
                        elif fields[j][0] == 1 and len(fields[j][3]) > 1:
                            ref_data = []
                            if re.match(r'^ling_area_\d+$', fields[j][2]):
                                data_array = ["|".join([i.strip() for i in list(
                                                re.findall(fields[j][3], col)[0])])]
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
                                    ref_data.append(csv_dataframe[col_name][item])
                                    if dfkey not in csv_dataframe: # header
                                        csv_dataframe[dfkey] = []
                                        csv_dataframe[dfkey].append(['stick_pk', col_name + '_pk'])
                                    csv_dataframe[dfkey].append([i, csv_dataframe[col_name][item]])
                            # save ids to related table as semicolon separated lists of ids
                            data.append(';'.join(map(str, ref_data)))
            csv_dataframe['sticks'].append(data)

    with get_catalog() as cat:
        images_objs = {obj.metadata['name']: obj for obj in cat}

    for filename, data in csv_dataframe.items():
        with UnicodeWriter(raw_path.joinpath(filename + '.csv')) as writer:
            if type(data) is list:
                for item in data:
                    writer.writerow(item)
            else:
                d = []
                if filename == 'ling_area':
                    d.append(['pk', 'chirila_name', 'austlang_code', 'austlang_name', 'glottolog_code'])
                    for k, v in data.items():
                        c,ac,an,g = re.split(r'\|', k)
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


if __name__ == '__main__':
    main()


from clldutils.clilib import ArgumentParserWithLogging
from clldutils.dsv import UnicodeWriter, UnicodeReader
from collections import OrderedDict
from pathlib import Path
import csv
import sys
import re
import pprint

# call ... to_csv.py {PATH_TO_records.csv}

# [separated table, old header, new header, split on]
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
    [1,'Linguistic area', 'ling_area_1', r'  +'],
    [1,'Linguistic area 2', 'ling_area_2', r'  +'],
    [1,'Linguistic area 3', 'ling_area_3', r'  +'],
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
    [1,'Data entry (OCCAMS)', 'occams', r'  +'],
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

def main():

    csv_dataframe = {
        'sticks': []
        ,'keywords': {} 
        ,'sem_domain': {}
        ,'linked_filenames': {}
        ,'ling_area': {}
        ,'item_type': {}
        ,'material': {}
        ,'technique': {}
        ,'glottolog_codes': {}
        ,'source_citation': {}
        ,'source_type': {}
        ,'holder_file': {}
        ,'occams': {}
    }

    with UnicodeReader(Path(sys.argv[1]), delimiter='\t') as reader:
        for i_, row in enumerate(reader):
            if len(row) != 42:
                print("Error count of columns in line " + i)
                exit(1)
            data = []
            if i_ == 0: #header
                data.append('id') # add id
                for j, col in enumerate(row):
                    if fields[j][2].strip() not in fields_not_in_sticks:
                        data.append(fields[j][2].strip())
            else:
                i = i_ - 1
                data.append(i) # add id
                for j, col_ in enumerate(row):
                    if re.sub(r'[ ]+', '', col_) == '':
                        data.append('')
                    else:
                        col = col_.strip()
                        if fields[j][2] in fields_not_in_sticks and not fields[j][2] == 'linked_filenames':
                            col = col.lower()
                        if fields[j][0] == 0:
                            data.append(col)
                        elif fields[j][0] == 1 and len(fields[j][3]) == 0:
                            if col not in csv_dataframe[fields[j][2]]:
                                csv_dataframe[fields[j][2]][col] = len(csv_dataframe[fields[j][2]]) + 1
                            data.append(csv_dataframe[fields[j][2]][col])
                        elif fields[j][0] == 1 and len(fields[j][3]) > 1:
                            data_array = re.split(fields[j][3], col)
                            ref_data = []
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
                                        csv_dataframe[dfkey].append(['stick_id', col_name + '_id'])
                                    csv_dataframe[dfkey].append([i, csv_dataframe[col_name][item]])
                            # save ids to related table as space separated lists of ids
                            data.append(' '.join(map(str, ref_data)))
            csv_dataframe['sticks'].append(data)
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(csv_dataframe)

    for filename, data in csv_dataframe.items():
        with UnicodeWriter(filename + '.csv') as writer:
            if type(data) is list:
                for item in data:
                    writer.writerow(item)
            else:
                d = []
                d.append(['id', 'name'])
                for k, v in data.items():
                    d.append([v, k])
                for item in d:
                    writer.writerow(item)


if __name__ == '__main__':
    main()


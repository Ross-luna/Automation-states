#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Created on Tue Feb 02  13:18:03 2021
Tamaulipas S3 Script
author: @rluna10
reference script: @shackles & @gerardo.avelar https://drive.google.com/file/d/1JSbfxgf2NUIteXFBgByJh62L791D-XAR/view?usp=sharing
latest update: 2020-02-02 [Addition Insurance(poliza_seguro)  --@rluna10]
latest update: 2020-08-27 [Remove driver's cnap  --@carloslme]
latest update: 2020-08-21 [Remove driver's toxi and include toxicologic exam --@gerardo.avelar]
latest update: 2020-08-21 [Addition of proof of address (optional document) --@carloslme]
Refer to this doc for how to run:
https://docs.google.com/...
Description: This script take the output of the Tamaulipas's query in format .csv, UUIDs and formats as them exist, and then generates the two columns
            in S3 Link Generator format, this last one (online) generates another .csv with the UUID & Doc Link
            After that the script take the output of S3 and proceed to download every file.
            Finally it sort the folders with their respective files.
'''

import pandas as pd
import os
import csv
from multiprocessing.dummy import Pool as ThreadPool
import requests

from tqdm import tqdm
import time
from random import randrange
from tqdm import trange
from colorama import Fore

cwd = os.getcwd()
current_date = pd.to_datetime('today')
current_date_str = str(current_date)[:10]

licensing_path = cwd + '/licensing_' + current_date_str

if not os.path.exists(licensing_path):
        os.mkdir(licensing_path)

doc_map = {}
doc_renamer = []


df = pd.read_csv("query.csv")
df = df.drop_duplicates(['vin'], keep='last')
df.to_csv("querytam.csv")

def import_csv():
    query_output = input('//Drop the query output here: ').strip()
    query_df = pd.read_csv(query_output)
    return query_df

def create_s3_file_map(df_lic_norm):
    print ('//Mapping document to IDs')
    with open(licensing_path+"/s3_tamaulipas_upload.csv",'w') as f:
            thewriter = csv.writer(f)
            thewriter.writerow(['poliza_uuid','poliza_format'])
            for index, row in df_lic_norm.iterrows():
                 
                    thewriter.writerow([row['poliza_uuid'], row['poliza_format']])
                    doc_map[row['poliza_uuid']] = [row['vin'], 'poliza_seguro', row['poliza_format']]
                     
        
#                thewriter.writerow([row['vehicle_registration_document_uuid'], row['vehicle_registration_format']])
#                doc_map[row['vehicle_registration_document_uuid']] = [row['curp_id'], 'tarjeta_circulacion',   row['vehicle_registration_format']]                                    

#                thewriter.writerow([row['id_card_document_uuid'], row['id_card_format']])
#                doc_map[row['id_card_document_uuid']] = [row['curp_id'], 'identificacion_oficial',row['id_card_format']]

#                thewriter.writerow([row['toxi_document_uuid'], row['toxi_format']])
#                doc_map[row['toxi_document_uuid']] = [row['curp_id'], 'toxi', row['toxi_format']]
                
#               thewriter.writerow([row['proof_address_document_uuid'], row['proof_address_format']])
#               doc_map[row['proof_address_document_uuid']] = [row['curp_id'], 'comprobante', row['proof_address_format']]

                # thewriter.writerow([row['no_criminal_record_document_uuid'], row['no_criminal_record_format']])
                # doc_map[row['no_criminal_record_document_uuid']] = [row['curp_id'], 'certificado_no_antecedentes', row['no_criminal_record_format']]
    return None

def create_folders():
    print ('//Creating folders')
    dri = licensing_path + '/drivers_owners'
    if not os.path.exists(dri):
        os.mkdir(dri)
    for key in doc_map:
        if not os.path.exists(licensing_path + '/drivers_owners/' + doc_map[key][0]):
            os.mkdir(licensing_path + '/drivers_owners/' + doc_map[key][0])

def create_s3_rename_map():
    s3_file = input('//Drop the S3 output for drivers owners: ').strip()
    s3_df = pd.read_csv(s3_file)
    for index, row in s3_df.iterrows():
        if row['UUID'] in doc_map:
            rename = doc_map[row['UUID']][0] + '/' + doc_map[row['UUID']][1]  + '.' + doc_map[row['UUID']][2]
            doc_renamer.append((rename, row['Doc Link']))
    return None

def download_s3(entry):
    path, url = entry
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open(licensing_path + '/drivers_owners/' + path, 'wb') as f:
            f.write(r.content)
    else:
        print (path)
        print ("Error Code: {}".format(r.status_code))

    return path

def func_call(position, total):
    text = 'progressbar #{position}'.format(position=position)
    with  tqdm(total=total, position=position, desc=text) as progress:
        for _ in range(0, total, 5):
            progress.update(5)
            time.sleep(randrange(3))

def download_s3_parallel(files_list, threads=2):
    print ('//Downloading driver files')
    pool = ThreadPool(threads)
    results = pool.map(download_s3, files_list)
    tasks = range(threads)
    for i, url in enumerate(tasks, 1):
        pool.apply_async(func_call, args=(i, 100))
    pool.close()
    pool.join()
    print ('//Download complete')
    return results


def main():
    csv_raw = import_csv()

    create_s3_file_map(csv_raw)
    create_s3_rename_map()

    create_folders()
    download_s3_parallel(doc_renamer, 8)

main()

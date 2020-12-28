#!/usr/bin/python3
# coding=utf8
import os
import argparse
import pandas as pd
from configparser import ConfigParser
from qcloud_cos import CosS3Client, CosConfig
import json
"""
python3 cos_inventory.py -prefix xxxxxxx/xxxxxxx/manifest.json
"""

parser = argparse.ArgumentParser(description="Anylysis inventory")
parser.add_argument("-prefix", help="COS Bucket Prefix", action="store")
args = parser.parse_args()
config = ConfigParser()
config.read('config.ini')
COS_CONFIG = config['common'] if config.sections() else None

log_tmp_folder = 'log_tmp'


def download_folder(cos_path, local_path):
    cos_config = CosConfig(Region=COS_CONFIG['region'],
                           SecretId=COS_CONFIG['secret_id'],
                           SecretKey=COS_CONFIG['secret_key'])
    cos_client = CosS3Client(cos_config)
    file_list = []
    response = cos_client.get_object(Bucket=COS_CONFIG['bucket'], Key=cos_path)
    response['Body'].get_stream_to_file('manifest.json')
    with open('manifest.json', 'r') as f:
        manifest_data = json.load(f)
    csv_file_list = manifest_data['files']
    headers = [x.strip() for x in manifest_data['fileSchema'].split(',')]
    file_size_list = []
    object_num = []
    for file in csv_file_list:
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        local_filenames = os.listdir(local_path)
        filename = file['key'].split('/')[-1]
        local_file_path = '{}/{}'.format(local_path, filename)
        if filename not in local_filenames:
            resp = cos_client.get_object(Bucket=COS_CONFIG['bucket'], Key=file['key'])
            resp['Body'].get_stream_to_file(local_file_path)
            file_list.append(local_file_path)
        # 读取csv文件
        dataFrame = pd.read_csv(local_file_path, compression='gzip', names=headers)
        # 判断归档对象类型
        dataFrame.loc[(dataFrame['StorageClass'] == 'Archive')
                      & (dataFrame['Size'] <= 65536), 'Size'] = 65536
        """
        判断前缀
        dataFrame['Key'].str.startswith('')
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.str.startswith.html
        """
        storage_size = dataFrame[dataFrame['StorageClass'] == 'Archive']['Size'].sum()
        storage_num = len(dataFrame[dataFrame['StorageClass'] == 'Archive'])
        file_size_list.append(storage_size)
        object_num.append(storage_num)
        print(file)
        print(storage_size)
    print(sum(file_size_list), sum(object_num))


def main():
    inventory_json_file = args.prefix
    download_folder(inventory_json_file, log_tmp_folder)


if __name__ == '__main__':
    main()
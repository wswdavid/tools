#!/usr/bin/python3
# coding=utf8
import os
import argparse
import time
import datetime
import pandas as pd
from configparser import ConfigParser
from urllib.parse import unquote
from qcloud_cos import CosS3Client, CosConfig

parser = argparse.ArgumentParser(description="Anylysis Log")
parser.add_argument("-f", help="Local File Path", action="store")
parser.add_argument("-prefix", help="COS Bucket Prefix", action="store")
parser.add_argument("-csv", help="Save Log File", action='store_true')
parser.add_argument("-e", help="COS Event Name", action='store')
parser.add_argument("-op",
                    help="Fuzzy Match Operations of URL",
                    action='store')
parser.add_argument("-u", help="Retrieve URLs In/Out Flow", action='store')
args = parser.parse_args()
config = ConfigParser()
config.read('config.ini')
COS_CONFIG = config['common'] if config.sections() else None
TABLE_HEADER = [
    'eventVersion', 'bucketName', 'qcsRegion', 'eventTime', 'eventSource',
    'eventName', 'remoteIp', 'userSecretKeyId', 'reservedFiled',
    'reqBytesSent', 'deltaDataSize', 'reqPath', 'reqMethod', 'userAgent',
    'resHttpCode', 'resErrorCode', 'resErrorMsg', 'resBytesSent',
    'resTotalTime', 'logSourceType', 'storageClass', 'accountId',
    'resTurnAroundTime', 'requester', 'requestId', 'objectSize', 'versionId',
    'targetStorageClass', 'referer', 'requestUri'
]
event_name = args.e if args.e else None
log_tmp_folder = 'log_tmp'
out_file_name = '{}.csv'.format(time.strftime("%m%d-%H%M%S", time.localtime()))
out_csv_file_name = args.prefix.split(
    '/')[0] + '-' + out_file_name if args.prefix else None
result_frame = pd.DataFrame(columns=TABLE_HEADER)
single_url = (lambda x: unquote(x, 'utf-8')
              if x.startswith('/') else unquote(('/' + x), 'utf-8'))
single_url_ops = [
    lambda x, y, z: single_url_operation_record(x, y, z),
    lambda x, y: single_url_analysis(x, y)
]


def utc_to_local(utc_time_str):
    utc_date = datetime.datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
    local_date = utc_date + datetime.timedelta(hours=8)
    return (datetime.datetime.strftime(local_date, '%Y-%m-%d %H:%M:%S'))


def convert_flow_size(size):
    kb = 1000
    mb = kb * 1000
    gb = mb * 1000
    tb = gb * 1000

    if size >= tb:
        return "%.3f TB" % float(size / tb)
    if size >= gb:
        return "%.3f GB" % float(size / gb)
    if size >= mb:
        return "%.3f MB" % float(size / mb)
    if size >= kb:
        return "%.3f KB" % float(size / kb)
    else:
        return None


def Read_log(local_path, LocalMarker=False):
    if LocalMarker:
        data = pd.read_csv(local_path, names=TABLE_HEADER)
    else:
        data = pd.read_table(local_path,
                             sep=' ',
                             header=None,
                             names=TABLE_HEADER)
        data['reqPath'] = data['reqPath'].apply(lambda x:
                                                (unquote(x, 'utf-8')))
        data['eventTime'] = data['eventTime'].apply(lambda x:
                                                    (utc_to_local(x)))
    return data


def download_folder(cos_path, local_path):
    if cos_path.endswith('/') is False:
        cos_path += '/'
    cos_config = CosConfig(Region=COS_CONFIG['region'],
                           SecretId=COS_CONFIG['secret_id'],
                           SecretKey=COS_CONFIG['secret_key'])
    cos_client = CosS3Client(cos_config)
    NextMarker = ""
    IsTruncated = "true"
    file_list = []
    while IsTruncated == "true":
        try:
            response = cos_client.list_objects(Bucket=COS_CONFIG['bucket'],
                                               Prefix=cos_path,
                                               Marker=NextMarker,
                                               MaxKeys=5)
            if 'IsTruncated' in response:
                IsTruncated = response['IsTruncated']
            if 'NextMarker' in response:
                NextMarker = response['NextMarker']
            for file in response['Contents']:
                if not os.path.exists(local_path):
                    os.makedirs(local_path)
                local_filenames = os.listdir(local_path)
                filename = file['Key'].split('/')[-1]
                local_file_path = '{}/{}'.format(local_path, filename)
                file_list.append(local_file_path)
                if filename not in local_filenames:
                    response = cos_client.get_object(
                        Bucket=COS_CONFIG['bucket'], Key=file['Key'])
                    response['Body'].get_stream_to_file(local_file_path)
        except Exception as e:
            return -1
    return file_list


def single_url_operation_record(frame, urlpath, event):
    operation_frame = frame[frame['reqPath'].str.contains(urlpath, regex=False)
                            & frame['eventName'].str.lower().str.contains(
                                event.lower())] if event else frame[
                                    frame['reqPath'].str.contains(urlpath,
                                                                  regex=False)]
    return operation_frame


def single_url_analysis(frame, urlpath):
    try:
        url_frame = frame.groupby('reqPath').get_group(urlpath)
        return url_frame
    except KeyError:
        return pd.DataFrame()


def main():
    global result_frame
    tmpFrame = pd.DataFrame()
    op_reqpath = args.op if args.op else None
    u_reqpath = single_url(args.u) if args.u else None
    if args.f:
        result_frame = Read_log(args.f, LocalMarker=True)
        if args.op:
            result_frame = single_url_ops[0](result_frame, op_reqpath,
                                             event_name)
        if args.u:
            result_frame = single_url_ops[1](result_frame, u_reqpath)
    else:
        local_file_list = download_folder(args.prefix, log_tmp_folder)
        if local_file_list:
            for file in local_file_list:
                dataFrame = Read_log(file)
                if args.op:
                    tmpFrame = single_url_ops[0](dataFrame, op_reqpath,
                                                 event_name)
                if args.u:
                    tmpFrame = single_url_ops[1](dataFrame, u_reqpath)
                result_frame = pd.concat([result_frame, tmpFrame], axis=0)
                if args.csv:
                    dataFrame.to_csv(out_csv_file_name,
                                     index=False,
                                     header=False,
                                     mode='a')
        result_frame.sort_values(by='eventTime', ascending=True, inplace=True)
    if args.op:
        result_frame.to_csv(out_file_name, index=False)
        print('需要检索的 URL: {}'.format(args.op))
        print('分析结果已保存: -> {}'.format(out_file_name))
    if args.u:
        print('需要检索的 URL: {}'.format(args.u))
        print('1: URL 总请求量: {}'.format(result_frame.shape[0]))
        print('2: URL 总出流量: {}'.format(
            convert_flow_size(result_frame['resBytesSent'].sum())))
        print('3. URL 总入流量: {}'.format(
            convert_flow_size(result_frame['reqBytesSent'].sum())))


if __name__ == '__main__':
    main()
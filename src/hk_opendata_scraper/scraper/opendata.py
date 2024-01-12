import requests
import logging

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()

URL_DATASET_LIST = 'https://data.gov.hk/en-data/api/3/action/package_show'
URL_HISTORY_DATASET_VERSION = 'https://api.data.gov.hk/v1/historical-archive/list-file-versions'
URL_DATASET_DOWNLOAD = 'https://api.data.gov.hk/v1/historical-archive/get-file'

# return dict of datasets 
def list_datasets(datasetGroupId):
    payload = { 'id': datasetGroupId }
    logger.info(f'Requesting to {URL_DATASET_LIST}?id={datasetGroupId}')
    datasets_res = requests.get(URL_DATASET_LIST, params=payload).json()
    return {
        'data_dictionary_url': datasets_res['result']['data_dictionary'],
        'datasets': datasets_res['result']['resources']
    }

def listHistoryDatasetVersion(dataset_url, start, end):
    """
        return history dataset downloadable url
        :params dataset_url url of dataset
        :params start, in the format of '20240101'
        :params end, in the format of '20240101'
    """
    payload = { 'url': dataset_url, 'start': start, 'end': end}
    logger.info(f'Requesting to {URL_HISTORY_DATASET_VERSION}?url={dataset_url}&start={start}&end={end}')
    file_version_dict = requests.get(URL_HISTORY_DATASET_VERSION, params=payload).json()
    download_version_list = [ 
        { 'url': buildDowloadUrl(dataset_url, timestamp), 'timestamp': timestamp}
            for timestamp in file_version_dict['timestamps'] 
    ]
    return download_version_list

def buildDowloadUrl(url, timestamp):
    return URL_DATASET_DOWNLOAD + '?url=' + url + "&time=" + timestamp


# datasets_dict = list_datasets('hk-dsd-dsd_psi_1-drainage-record')
# dataset_url = datasets_dict['datasets'][52]['url'].replace('https', 'http')
# print(f'dataset_url: {dataset_url}')
# print(f'downloadable url:')
# print(listHistoryDatasetVersion(dataset_url, '20240108', '20240108'))


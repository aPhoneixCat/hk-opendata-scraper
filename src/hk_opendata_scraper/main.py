import logging
import time

from scraper import opendata
from scraper import csdi
import utils

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()

utils.createPath('target/')
SCRAPE_START_INDEX = 0
SCRAPE_END_INDEX = 100

# Step One. Get all dataset to scrape
logger.info('Step One. Retrieve all datasets for scaping...')
DATASET_GROUPID = 'hk-dsd-dsd_psi_1-drainage-record'
datasetListDictToScape = opendata.list_datasets(DATASET_GROUPID)

# Step Two. Download dataset group data dictionary
DATA_DICTIONARY_FILENAME = 'target/data_dictionary.pdf'
logger.info(f'Step Two. Download data dictionanry to {DATA_DICTIONARY_FILENAME}...')
dataDictionaryPDFURL = datasetListDictToScape['data_dictionary_url']
utils.downloadFileFromURL(dataDictionaryPDFURL, DATA_DICTIONARY_FILENAME)

# Step Three. Download datasets overview CSV
DATASET_OVERVIEW_FILENAME = 'target/overview.csv'
logger.info(f'Step Three. Writing dataset overview CSV to {DATASET_OVERVIEW_FILENAME}...')
CSV_FIELD_NAMES = ['name', 'description', 'format', 'url']
csvDataList = [ 
    { 'name': dataset['name'], 'description': dataset['description'], 'format': dataset['format'], 'url': dataset['url']} 
    for dataset in datasetListDictToScape['datasets']
]
utils.writeToCSV(DATASET_OVERVIEW_FILENAME, CSV_FIELD_NAMES, csvDataList)

# Step Four.1. Scrape JSON format dataset
logging.info('Step Four.1. Scrape JSON format dataset...')
datasetList = datasetListDictToScape['datasets']
datasetListOfJSON = [ dataset for dataset in datasetList if dataset['format'] == 'JSON'][22:SCRAPE_END_INDEX]
for dataset in datasetListOfJSON:
    normalizedDatasetName = utils.normalizeDatasetName(dataset['name'])
    targetPath = f'target/JSON/{normalizedDatasetName}'
    utils.createPath(targetPath)

    datasetURL = dataset['url'].replace('https', 'http')
    logging.info(f'Scraping JSON dataset: {datasetURL}')
    # Query history dataset data for past year
    versionList = opendata.listHistoryDatasetVersion(datasetURL, '20230110', '20240110')
    for version in versionList:
        utils.downloadFileFromURL(version['url'], f"{targetPath}/{version['timestamp']}")
    time.sleep(1) # Sleep 1 seconds to avoid request flooding to let the system down

# Step Four.2. Scrape API format dataaset
logger.info('Step Four.2. Scrape API format dataaset...')
datasetList = datasetListDictToScape['datasets']
datasetListOfAPI = [ dataset for dataset in datasetList if dataset['format'] == 'API'][SCRAPE_START_INDEX:SCRAPE_END_INDEX]
for dataset in datasetListOfAPI:
    normalizedDatasetName = utils.normalizeDatasetName(dataset['name'])
    targetPath = f'target/API/{normalizedDatasetName}'
    utils.createPath(targetPath)

    logging.info(f"Scraping API dataset: {dataset['url']}")
    datasetIdInCSDI = csdi.getDatasetIdFromDatasetURL(dataset['url'])
    datasetMetaData = csdi.getDatasetMetaData(datasetIdInCSDI)
    # Disable this as not lib support in laptop
    # specifictionaURL = csdi.buildDataSpecificationUrl(fileId=datasetMetaData['_source']['fileid'])
    # utils.writePageToPDF(specifictionaURL, f'{targetPath}/specification.pdf')
    fileVersionList = csdi.getArchivedDatasetVersions(datasetIdInCSDI)
    for version in fileVersionList:
        for subVersion in version:
            versionId = subVersion['version']
            fileURL = subVersion['fileURL']
            format = subVersion['format']
            utils.createPath(f'{targetPath}/{versionId}')
            utils.downloadFileFromURL(fileURL, f'{targetPath}/{versionId}/{normalizedDatasetName}_{format}.zip')
    time.sleep(1) # Sleep 1 seconds to avoid request flooding to let the system down
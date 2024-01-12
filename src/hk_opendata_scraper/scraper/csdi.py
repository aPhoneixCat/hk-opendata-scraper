import requests
import logging
from furl import furl

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()

# sample datasetId: dsd_rcd_1629267205231_8534
URL_DATASET_METADATA = 'https://portal.csdi.gov.hk/geoportal/rest/metadata/item/{datasetId}'
URL_ARCHIVED_DATASET_VERSIONS = 'https://portal.csdi.gov.hk/csdi-webpage/achivedDatasetFileList/{datasetId}'

def getDatasetMetaData(datasetId):
    url = URL_DATASET_METADATA.format(datasetId = datasetId)
    logger.info(f'Requesting dataset metadata: {url}')
    return requests.get(url).json()

def getArchivedDatasetVersions(datasetId):
    url = URL_ARCHIVED_DATASET_VERSIONS.format(datasetId = datasetId)
    logger.info(f'Requesting dataset history dataset version files: {url}')
    versions = requests.get(url).json()
    ''' format
         [
            {
                "fileType": "FGDB",
                "pos": 0
            }
        ]
    '''
    sourceFormats = versions['archivedDatasetFileFormatListVO']['sourceFormat']
    convertedFormats = versions['archivedDatasetFileFormatListVO']['convertedFormat']
    datasetVersionList = [ 
        flattenVersion(version, sourceFormats, convertedFormats) for version in versions['archivedDatasetVersionList'] if version['year'] == 2023
    ]
    
    return datasetVersionList
    

def buildDataSpecificationUrl(fileId):
    return f"https://static.csdi.gov.hk/csdi-webpage/view/{fileId.replace('-', '')}/sds_view"

def getDatasetIdFromDatasetURL(datasetURL):
    # https://portal.csdi.gov.hk/geoportal/?datasetId=dsd_rcd_1629267205231_84346
    f = furl(datasetURL) 
    return f.args['datasetId']

def flattenVersion(version, sourceFormats, convertedFormats):
    fileList = version['fileList']
    versionDate = f"{version['year']}-{version['quarter']}"
    fileVersions = []
    for fileInfo in fileList:
        if fileInfo['sourceFormat']:
            for format in sourceFormats:
                if format['pos'] == fileInfo['pos']:
                    fileVersions.append(buildVersionBody(
                        versionDate, format['fileType'], fileInfo['url']
                    ))
        else:
            for format in convertedFormats:
                if format['pos'] == fileInfo['pos']:
                    fileVersions.append(buildVersionBody(
                        versionDate, format['fileType'], fileInfo['url']
                    ))
    
    return fileVersions

def buildVersionBody(versionDate, fileType, url):
    return { 'version': versionDate, 'fileURL': url, 'format': fileType }

def contains(list, filter):
    for x in list:
        if filter(x):
            return True
    return False

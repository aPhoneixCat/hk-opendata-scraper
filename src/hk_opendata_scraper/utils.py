import requests
import csv
import logging
# import weasyprint
from pathlib import Path

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()

BLANK = ' '

def downloadFileFromURL(url, filename):
    r = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=16*1024):
            f.write(chunk)

def writeToCSV(filename, fieldnames = [], dataList = []):
    if not fieldnames or not dataList:
        logger.warning('Cannot write to CSV due to missing fieldname or dataList')
        return
    
    with open(filename, 'w+', newline='', encoding='utf-8-sig') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        csv_writer.writeheader()
        for data in dataList:
            csv_writer.writerow(data)

# def writePageToPDF(url, pdfFileName):
#     pdf = weasyprint.HTML(url).write_pdf()
#     with open(pdfFileName, 'wb+') as f:
#             f.write(pdf)

def createPath(pathName):
    Path(pathName).mkdir(parents=True, exist_ok=True)

def normalizeDatasetName(datasetName):
    return '_'.join(datasetName.split(BLANK)).replace('/', '_')
import pandas as pd
import os
import requests
import zipfile
from pymongo import MongoClient, DESCENDING

def unzipFile(filePath):
    with zipfile.ZipFile(filePath, 'r') as zip_ref:
        zip_ref.extractall("./extracted/")
    os.remove(filePath)

def download_files():
    link = 'https://archives.nseindia.com/content/nsccl/combineoi.zip'
    data = requests.get(link, allow_redirects=True)
    open('temp.zip', 'wb').write(data.content)
    unzipFile("./temp.zip")

def loadToMongo(load_file):
    data = pd.read_csv("./extracted/"+load_file)
    df = pd.DataFrame(data, columns=['Date', ' NSE Symbol', ' MWPL', ' Open Interest', ' Limit for Next Day'])
    df['Percent'] = (df[" Open Interest"] / df[" MWPL"])*100

    uri = "mongodb+srv://iovalues.lgijy.mongodb.net/myFirstDatabase?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
    client = MongoClient(uri, tls=True, tlsCertificateKeyFile='X509-cert-3528890396476155861.pem')

    db = client['oiDB']
    collection = db['oiData']

    # Index run once one If creating a new DB
    collection.create_index([("Date", DESCENDING), (" NSE Symbol", DESCENDING)], background=True, unique=True)
    collection.insert_many(df.to_dict('records'))

def processFiles():
    for filename in os.listdir('./extracted/'):
        root, ext = os.path.splitext(filename)
        if ext == '.csv':
            loadToMongo(filename)
            os.replace("./extracted/"+filename, "./extracted/Archive/"+filename)
        if ext == '.xml': 
            os.remove('./extracted/'+filename)
        if ext == '.zip':
            unzipFile('./extracted/'+filename)

download_files()

processFiles()
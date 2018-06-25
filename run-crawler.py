#!/usr/bin/python
import requests
import simplejson as json
import sys
import pymongo
import uuid
import time
import string
from decimal import Decimal
from pymongo import MongoClient
from scrapinghub import ScrapinghubClient

# scrapinhub info
projectId = '300910'
spider = 'public_mutual'
spiderId = '1'
APIKEY = '365a2917e592402db0ae212a0dbc64a5'
mongoURI = 'mongodb://readwrite:MFcraw1er@ds017193.mlab.com:17193/mfcrawler'


def makeRequest(url, type, data={}):
    r = requests.request(type, url, data=data, auth=(APIKEY, ''))
    print(r.content)
    return json.loads(r.content)


def saveToMongo(items):
    
    jsonArr = []
    for fund in items.iter():
        obj = {}
        obj['abbreviation'] = fund['fund_abbr'][0]
        obj['name'] = fund['fund'][0]
        obj['date'] = fund['date'][0]
        obj['nav'] = fund['nav'][0]
        jsonArr.append(obj)

    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    data = '{"query":"mutation upsertFundsAndPrices($scrapData: JSON){ upsertFundsAndPrices(scrapData:$scrapData){ id } }","variables":{"scrapData":'+ json.dumps(jsonArr, separators=(',',':')) +'}}'

    r = requests.post('http://mfwatch.online/graphql', headers=headers, data=data)

def main():
    requestsMade = 0
    while requestsMade < 3:
        # running the job
        client = ScrapinghubClient(APIKEY)
        project = client.get_project(projectId)
        job = project.jobs.run(spider)

        if job.metadata.get('state') == 'running' or job.metadata.get('state') == 'pending' or job.metadata.get('state') == 'finished':
            requestsMade = 10
            
            # getting result from job
            lastFinishedJob =  project.jobs.iter(spider=spider, state='finished', count=1)

            for job in lastFinishedJob:
                lastJobId = job['key']
                jobData = client.get_job(lastJobId)
                saveToMongo(jobData.items)

        else:
            requestsMade += 1
            time.sleep(5)

main()

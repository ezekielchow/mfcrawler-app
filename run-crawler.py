#!/usr/bin/python
import requests
import json
import sys
import pymongo
from pymongo import MongoClient
import uuid
import time
from apscheduler.schedulers.background import BackgroundScheduler

# scrapinhub info
projectId = '300910'
spider = 'public_mutual'
spiderId = '1'
APIKEY = '365a2917e592402db0ae212a0dbc64a5'
mongoURI = 'mongodb://readwrite:MFcraw1er@ds017193.mlab.com:17193/mfcrawler'


def makeRequest(url, type, data={}):
    r = requests.request(type, url, data=data, auth=(APIKEY, ''))
    return json.loads(r.content)


def saveToMongo(jsonData):
    client = MongoClient(mongoURI)
    funds = client.mfcrawler.funds
    fundPrices = client.mfcrawler.fund_prices
    for fund in jsonData:
        obj = {
            'abbreviation': fund['fund_abbr'],
            'name': fund['fund']
        }
        funds.update({'abbreviation': fund['fund_abbr']}, obj, upsert=True)
        fundId = funds.find_one({'abbreviation': fund['fund_abbr']})['_id']

        priceObj = {
            'nav': fund['nav'],
            'date': fund['date'],
            'fund_id': fundId
        }
        fundPrices.update({'date': fund['date']}, priceObj, upsert=True)


def main():
    requestsMade = 0
    while requestsMade < 5:
        # running the job
        crawlURL = 'https://app.scrapinghub.com/api/run.json'
        data = {
            "project": projectId,
            "spider": spider
        }
        crawlResult = makeRequest(crawlURL, 'post', data)

        if crawlResult['status'] == "ok":
            requestsMade = 10
            jobId = crawlResult['jobid'].rpartition('/')[2]
            # getting result from job
            jobResultURL = 'https://storage.scrapinghub.com/items/' + \
                projectId + '/' + spiderId + '/' + jobId + '?format=json'
            saveToMongo(makeRequest(jobResultURL, 'get'))
        else:

            time.sleep(5)


scheduler = BackgroundScheduler()
scheduler.start()

scheduler.add_job(main, 'interval', hours=2)

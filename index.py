#!/usr/bin/env python
"""
by Debarshi Banerjee, Laddu, @madcaplaughsha
Copyright (c) 2016 ScoopWhoop Media Pvt. Ltd.
Feb, 2016.

May 2016 ~ Adding websockets ~ Debarshi, Laddu

An app to collect consume data real time. 
"""
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.options import define, options, parse_command_line, parse_config_file
from tornado.web import Application, RequestHandler
import tornado.autoreload
import os
import json
import time
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

from defines import *
from cassapp import CassRecommendationService


define('port', default=8888, help="port to listen on")

"""
Base Handle Class for all requests 
"""
class BaseHandler(RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers",
                        "Origin, X-Requested-With, Content-Type, Accept")
        self.set_header("Access-Control-Allow-Methods", "POST")

class CassHandler():
    def connect(self):
        try:
            cluster = Cluster(clusterIPs)
            session = cluster.connect(keyspace)
            session.row_factory = dict_factory
            self.session = session
            mylogr.info("Cassandra Cluster Connected")
        except Exception as e:
            mylogr.error("error: {0}".format(e))

    def getSession(self):
        return self.session
    
class IndexHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        self.write("Welcome to Dovetail Joint")
        
class RecommendationHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        try:
            articleid = self.get_query_argument('articleid', False)
            userid = self.get_query_argument('userid', False)
            
            reco_articles = reco_service.get_recommended_articles(articleid,
                                                                  userid)
            sendData = {"status": 1, "data": [], "next_offset": -1}

            for recoart in reco_articles:
                tempDict = {}
                tempDict = {'title': recoart['title'],
                            'slug': recoart['slug'],
                            'feature_img': recoart['feature_img'],
                            '_type': recoart['type'],
                            'pub_date': recoart['pub_date']}
                sendData['data'].append(tempDict)

            sendData['total_articles'] = len(sendData['data'])
            self.write(json.dumps(sendData))

        except Exception as e:
            mylogr.info("Reco error %s" % (str(e)))
            
            
def main():
    mylogr.info("Welcome to the Dovetail Joint, a place for sending payloads.")
    parse_command_line(final=False)

    #Routes
    app = Application([
        ('/reco', RecommendationHandler),
        ('/', IndexHandler),
    ])
    app.listen(options.port)

    mylogr.info("DJ is ready for payload.")

    tornado.autoreload.start()
    for dir, _, files in os.walk('static'):
        [tornado.autoreload.watch(dir + '/' + f) for f in files
         if not f.startswith('.')]

    IOLoop.current().start()
            
if __name__ == "__main__":
    cass = CassHandler()
    cass.connect()
    session = cass.getSession()
    reco_service = CassRecommendationService(session, mylogr)
    main()
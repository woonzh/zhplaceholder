# -*- coding: utf-8 -*-
"""
Created on Sat Mar 24 17:34:54 2018

@author: woon.zhenhao
"""
import flask
from flask import Flask, request, make_response, render_template, redirect
from flask_cors import CORS
from flask_restful import Resource, Api
import json
import pandas as pd
#import testscraper as ts
#import sgx

##Test disable
import orchestrator as orc
import util
import analysis

app = Flask(__name__)
api = Api(app)
CORS(app)

@app.route('/')
def hello():
    return render_template('inventory.html')

@app.route('/inventory')
def inventory():
    return render_template('inventory.html')

@app.route('/test', methods=['GET', 'OPTIONS'])
def sessioncheck():
    ret={}
    if request.method == 'GET':
        ret={
            'answer':ts.test()}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp

@app.route('/keepalive', methods=['GET', 'OPTIONS'])
def keepAlive():
    ret={}
    if request.method == 'GET':
        ret={
            'answer':"yes"}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp
    
@app.route('/getFilterResult', methods=['GET', 'OPTIONS'])
def filterResult():
    ret={}
    if request.method == 'GET':
        industries = request.args.get("industries" ,type = str, default="") #split by ,
        industries=list(filter(lambda x: x!='',[x.strip() for x in industries.split(',')]))
        filters=request.args.get("filters", default=None)
        print(industries)
        
        ret={
            'answer':analysis.getFilteredResult(industry=industries, filters=filters).to_json(orient='split')}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp

@app.route('/testWorker', methods=['GET', 'OPTIONS'])
def workerCheck():
    ret={}
    if request.method == 'GET':
        params = request.args.get("params" ,type = str, default="")
        result=orc.wc.queueFunc('test', orc.testFunc, None)
        ret={
            'answer':result}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp

@app.route('/sgxUpdate', methods=['GET', 'OPTIONS'])
def workerSGXUpdate():
    ret={}
    if request.method == 'GET':
        intJobId=util.stringGenerator()
        dragIndex = request.args.get("dragIndex", default=None)
        sumTries = request.args.get("sumTries", default=None)
        result=orc.wc.queueFunc('sgx update', orc.runSGXUpdate, (dragIndex, sumTries), intJobId)
        ret={
            'answer':result}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp

@app.route('/sgxWorker', methods=['GET', 'OPTIONS'])
def workerSGX():
    ret={}
    if request.method == 'GET':
        intJobId=util.stringGenerator()
        result=orc.wc.queueFunc('sgx raw data', orc.runSGXFull, (intJobId), intJobId)
        ret={
            'answer':result}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp
    
@app.route('/hkexWorker', methods=['GET', 'OPTIONS'])
def workerHKEX():
    ret={}
    if request.method == 'GET':
        intJobId=util.stringGenerator()
        result=orc.wc.queueFunc('hkex raw data', orc.runHKEXFull, None , intJobId)
        ret={
            'answer':result}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp

@app.route('/rawdata', methods=['GET', 'OPTIONS'])
def rawdata():
    ret={}
    if request.method == 'GET':
        result=analysis.extractFileFromDB()
        print(result)
            
        resp = make_response(result.to_csv(header=True, index=False))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp

@app.route('/workerResult', methods=['GET', 'OPTIONS'])
def workerResult():
    ret={}
    if request.method == 'GET':
        jobid = request.args.get("jobId" ,type = str, default="")
        ret=orc.wc.getResult(jobid)
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp

if __name__ == '__main__':
     app.run(debug=True, port=80)
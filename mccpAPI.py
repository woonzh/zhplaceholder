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
        pw = request.args.get("pw", default='')
        if pw=='Keppel2017':
            result=orc.wc.queueFunc('sgx update', orc.runSGXUpdate, (dragIndex, sumTries), intJobId)
            ret={
                'answer':result}
        else:
            ret={
                'answer':'pw error'}
            
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
    
@app.route('/hkexUpdateDetails', methods=['GET', 'OPTIONS'])
def workerHKEXUpdateDetails():
    ret={}
    if request.method == 'GET':
        intJobId=util.stringGenerator()
        result=orc.wc.queueFunc('hkex update details', orc.runHKEXUpdateDetails, None , intJobId)
        ret={
            'answer':result}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp

@app.route('/hkexUpdateBasic', methods=['GET', 'OPTIONS'])
def workerHKEXUpdateBasic():
    ret={}
    if request.method == 'GET':
        quandlBool = request.args.get("quandl", default=0)
        pw = request.args.get("pw", default='')
        print('api-%s'%(quandlBool))
        
        intJobId=util.stringGenerator()
        
        if pw=='Keppel2017':
            result=orc.wc.queueFunc('hkex update basic', orc.runHKEXUpdateBasic, (quandlBool) , intJobId)
            ret={
                'answer':result}
        else:
            ret={
                'answer':'pw error'}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp
    
@app.route('/nasdaqfull', methods=['GET', 'OPTIONS'])
def workerNasdaqFull():
    ret={}
    if request.method == 'GET':
        intJobId=util.stringGenerator()
        userAgentNum = request.args.get("useragent", default=0)
        print('api -%s'%(str(userAgentNum)))
        result=orc.wc.queueFunc('nasdaq full', orc.runNasdaqFull, (userAgentNum) , intJobId)
        ret={
            'answer':result}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp
    
@app.route('/nasdaqupdatedetails', methods=['GET', 'OPTIONS'])
def workerNasdaqUpdateDetails():
    ret={}
    if request.method == 'GET':
        intJobId=util.stringGenerator()
        userAgentNum = request.args.get("useragent", default=0)
        print('api -%s'%(str(userAgentNum)))
        result=orc.wc.queueFunc('nasdaq update details', orc.runNasdaqDetailsUpdate, (userAgentNum) , intJobId)
        ret={
            'answer':result}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp
    
@app.route('/nasdaqupdatebasic', methods=['GET', 'OPTIONS'])
def workerNasdaqUpdateBasic():
    ret={}
    if request.method == 'GET':
        intJobId=util.stringGenerator()
        userAgentNum = request.args.get("useragent", default=0)
        print('api -%s'%(str(userAgentNum)))
        result=orc.wc.queueFunc('nasdaq update basic', orc.runNasdaqBasicUpdate, (userAgentNum) , intJobId)
        ret={
            'answer':result}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp

@app.route('/iexupdatedetails', methods=['GET', 'OPTIONS'])
def workeriexupdatedetails():
    ret={}
    if request.method == 'GET':
        intJobId=util.stringGenerator()
        start = request.args.get("start", default=0)
        end = request.args.get("end", default=0)
        print('api -%s-%s'%(str(start),str(end)))
        result=orc.wc.queueFunc('iex update details', orc.runIEXDetails, (start,end) , intJobId)
        ret={
            'answer':result}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp
    
@app.route('/iexupdatebasics', methods=['GET', 'OPTIONS'])
def workeriexupdatebasics():
    ret={}
    if request.method == 'GET':
        intJobId=util.stringGenerator()
        start = request.args.get("start", default=0)
        end = request.args.get("end", default=0)
        print('api -%s-%s'%(str(start),str(end)))
        result=orc.wc.queueFunc('iex update details', orc.runIEXBasics, (start,end) , intJobId)
        ret={
            'answer':result}
            
        resp = flask.Response(json.dumps(ret))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp
    
@app.route('/getAnalytics', methods=['GET', 'OPTIONS'])
def getAnalytics():
    if request.method == 'GET':
        country = request.args.get("country", default='sg')
        pw = request.args.get("pw", default='')
        clean= request.args.get("clean", default=False)
        clean=(clean=='true')
        print('api analytics -%s-%s-%s'%(country, pw, clean))
        df=orc.runAnalytics(country,pw, clean)
            
        resp = make_response(df.to_csv(header=True, index=False))
        resp.headers["Content-Disposition"] = "attachment; filename=error_reports.csv"
        resp.headers["Content-Type"] = "text/csv"
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
        return resp

@app.route('/cancelworker', methods=['GET', 'OPTIONS'])
def workerCancel():
    ret={}
    if request.method == 'GET':
        jobid = request.args.get("jobId" ,type = str, default="")
        ret=orc.wc.cancelJob(jobid)
        resp = flask.Response(json.dumps(ret))
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
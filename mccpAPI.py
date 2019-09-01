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
import testscraper as ts
import orchestrator as orc

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

@app.route('/testWorker', methods=['GET', 'OPTIONS'])
def workerCheck():
    ret={}
    if request.method == 'GET':
        params = request.args.get("params" ,type = str, default="")
        result=orc.wc.queueFunc(orc.testFunc, params)
        ret={
            'answer':result}
            
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

#@app.route('/passwordcheck', methods=['GET', 'OPTIONS'])
#def passwordcheck():
#    ret={}
#    if request.method == 'GET':
#        password = request.args.get("password" ,type = str, default="")
#        df=db.getPassword()
#        if df==password:
#            ret['result']='success'
#        else:
#            ret['result']='fail'
#            
#        resp = flask.Response(json.dumps(ret))
#        resp.headers['Access-Control-Allow-Origin'] = '*'
#        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
#        resp.headers['Access-Control-Allow-Credentials'] = 'true'
#        return resp
#    
#@app.route('/geturl', methods=['GET', 'OPTIONS'])
#def geturl():
#    if request.method=='GET':
#        ret={
#            'result':db.getUrl()
#            }
#            
#        resp = flask.Response(json.dumps(ret))
#        resp.headers['Access-Control-Allow-Origin'] = '*'
#        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
#        resp.headers['Access-Control-Allow-Credentials'] = 'true'
#        return resp
#    
#@app.route('/seturl', methods=['POST', 'OPTIONS'])
#def seturl():
#    if request.method=='POST':
#        url = request.form['url']
#        print("url: "+str(url))
#        ret={}
#        ret["result"]=str(db.setUrl(url))
#        
#        resp = flask.Response(json.dumps(ret))
#        resp.headers['Access-Control-Allow-Origin'] = '*'
#        resp.headers['Access-Control-Allow-Credentials'] = 'true'
#        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
#        return resp
#    
#@app.route('/login', methods=['GET', 'OPTIONS'])
#def login():
#    if request.method=='GET':
#        email = request.args.get("email" ,type = str, default="")
#        password = request.args.get("password" ,type = str, default="")
#        print("email: %s"%(email))
#        print("password: %s"%(password))
#        
#        success, msg=loginapp.loginmain(email, password)
#        
#        print(success)
#        print(msg)
#        
#        ret={
#            'result':success,
#            'msg':msg
#            }
#        
#        resp = flask.Response(json.dumps(ret))
#        resp.headers['Access-Control-Allow-Origin'] = '*'
#        resp.headers['Access-Control-Allow-Credentials'] = 'true'
#        resp.headers['Access-Control-Allow-Methods']= 'GET,PUT,POST,DELETE,OPTIONS'
#        print("returning")
#        return resp

if __name__ == '__main__':
     app.run(debug=True, port=80)
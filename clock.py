# -*- coding: utf-8 -*-
"""
Created on Sun Nov 26 18:28:55 2017

@author: ASUS
"""

from apscheduler.schedulers.blocking import BlockingScheduler
import orchestrator as orc
import dbConnector as db
import util

sched = BlockingScheduler()

#@sched.scheduled_job('interval', days=7)
#def timed_job():
#    print('This job is run every week.')

@sched.scheduled_job('cron', day_of_week='mon-fri', hour=12)
def scheduled_job():
    intJobId=util.stringGenerator()
    result=orc.wc.queueFunc('sgx update', orc.runSGXUpdate, (intJobId), intJobId)
    print('success')
    
#@sched.scheduled_job('interval', minute=5)
#def scheduled_job():
#    msg=master.updateSGXPriceBackground()
#    
#    print('success')

sched.start()
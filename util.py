# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 11:21:59 2019

@author: ASUS
"""

import datetime

def currentDate():
    now=datetime.datetime.now()
    return now.strftime("%d %b %Y")

#print(currentDate())
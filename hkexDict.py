companyTag={
    'bank':['ICBC', 'CCB', 'BANK OF CHINA', 'BANK OF GUIZHOU','BANK OF TIANJIN', 'BANKOFJINZHOU', \
            'BANKOFJIUJIANG', 'BANKOFZHENGZHOU', 'ABC', 'BANK OF GANSU'],
    'oil':['CNOOC', 'PetroChina','kunlun energy','SINOPEC CORP','SINOPEC KANTONS','SINOPEC SEG',\
           'SINOPEC SSC', 'YANCHANG PETRO']   
        }

bankdict={
    'stanchart':{
        'rev': '16 mil',
        'geog breakdown':{
            'china':'43%',
            'asia':'28%',
            'europe': '13%',
            'africa':'13%'},
        'biz breakdown':{
            'corp': '48%',
            'retail':'30%',
            'com': '7%',
            'private': '3%'
                },
        'profit':'4 mil',
        'loanbook': ['274b','322 b', '35% collateral'],
        'cash':'52b',
        'investment':'143b',
        'baddebt':'1.1mil',
        'high risk': ['7.3b','53% collateral'],
        'assets':'720b'
            },
    'dbs':{
        'rev': '14.5b',
        'geog breakdown':{
            'china':'27%',
            'singapore':'63%',
            'rest': '10%'},
        'biz breakdown':{
            'corp': '',
            'retail':'',
            'com': '',
            'private': ''
                },
        'profit':'6.39b',
        'loanbook': ['','', ''],
        'cash':'',
        'investment':'',
        'baddebt':'',
        'high risk': ['',''],
        'assets':'572b'
            }
        }

for ind in companyTag:
    lst=companyTag[ind]
    lst=[x.lower() for x in lst]
    companyTag[ind]=lst
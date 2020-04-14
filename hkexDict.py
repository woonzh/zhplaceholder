companyTag={
    'bank':['ICBC', 'CCB', 'BANK OF CHINA', 'BANK OF GUIZHOU','BANK OF TIANJIN', 'BANKOFJINZHOU', \
            'BANKOFJIUJIANG', 'BANKOFZHENGZHOU', 'ABC', 'BANK OF GANSU'],
    'oil':['CNOOC', 'PetroChina','kunlun energy','SINOPEC CORP','SINOPEC KANTONS','SINOPEC SEG',\
           'SINOPEC SSC', 'YANCHANG PETRO']   
        }

for ind in companyTag:
    lst=companyTag[ind]
    lst=[x.lower() for x in lst]
    companyTag[ind]=lst
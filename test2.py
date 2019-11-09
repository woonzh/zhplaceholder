from ortools.sat.python import cp_model
import pandas as pd
import time
import dbConnector as db

nurses=None
model=None
dates=None
shifts=None
shiftAlloc={}
fname='results.csv'
solver=cp_model.CpSolver()

def convertDfToDict(df, keyCol):
    store={}
    indexes=list(df.index)
    for i in indexes:
        tem=df.loc[i]
        tem2={}
        for j in list(tem.index):
            tem2[j]=tem[j]
        store[tem[keyCol]]=tem2
    return store

def dateVal():
    global dates
    df=pd.read_csv('dates.csv')
    dates=convertDfToDict(df, 'Date')

def defNurses():
    global nurses
    df=pd.read_csv('nurses.csv')
    nurses=convertDfToDict(df, 'name')
        
def defShifts():
    global shifts
    df=pd.read_csv("shifts.csv")
    shifts=convertDfToDict(df, 'Shift Code')
        
def initModel():
    global model, dates, test
    model=cp_model.CpModel()
            
def defShiftAlloc(mode=1):
    global shiftAlloc, model
    for i in dates:
        for j in nurses:
            for k in shifts:
                shiftAlloc[(i, j, k)]=model.NewBoolVar(i+"-"+str(j)+"-"+str(k))
            model.Add(sum(shiftAlloc[(i, j, p)] for p in shifts)==1)
    
    if mode==2:
        print ("mode 2")
    #    no afternoon shift after day off
    #    dateList=list(dates)
    #    dateList1=list(dates)[1:-1]
    ##    dateList2=list(dates)[1:]
    #    aftSess=['A1','A2']
    #    allSess=['A1','A2','DO']
    #    model.Maximize(sum(shiftAlloc[(i, j, k)]*2 for i in dateList1 for j in nurses for k in allSess)+\
    #                   sum(shiftAlloc[(dateList[0], j, 'DO')] for j in nurses)+\
    #                   sum(shiftAlloc[(dateList[len(dateList)-2], j, k)] for j in nurses for k in aftSess))
        
        #no consecutive 3 Aft shift
    #    dateList3=list(dates)[:-2]
    #    dateList4=list(dates)[1:-2]
    #    dateList5=list(dates)[2:]
    #    
    #    model.Minimize(sum(shiftAlloc[(i, j, 'DO')] for i in dateList1 for j in nurses)+\
    #                   sum(shiftAlloc[(i, j, k)] for i in dateList2 for j in nurses for k in aftSess)-\
    #                   len(dateList1)+\
    #                   sum(shiftAlloc[(i, j, k)] for i in dateList3 for j in nurses for k in aftSess)+\
    #                   sum(shiftAlloc[(i, j, k)] for i in dateList4 for j in nurses for k in aftSess)+\
    #                   sum(shiftAlloc[(i, j, k)] for i in dateList5 for j in nurses for k in aftSess)-\
    #                   (2*len(dateList3)))
        aftAfterOffLst, aftAfterOffCount=noAftAfterOff(2)
    #    model.Minimize(sum(aftAfterOffLst)-aftAfterOffCount)
        consecAftLst, consecAftCount=noConsecutiveAftShit(purpose=2)
        model.Minimize(sum(aftAfterOffLst)-aftAfterOffCount+sum(consecAftLst)-consecAftCount)
    
    if mode==1:
        print("mode 1")
#        model.Maximize(shiftAlloc[('2019-12-31', 'Adam', 'DO')] * shiftAlloc[('2019-12-31', 'Adam', 'DO')])
        model.Maximize(sum(shiftAlloc[(i, j, k)] for i in dates for j in nurses for k in shifts))

def checkDate(dateVal, purpose=1):
    #check for holiday
    if purpose==1:
        return dates[dateVal]['Hols']==1
    
    #check for min nurse:
    if purpose==2:
        if dates[dateVal]['Hols']==1 or dates[dateVal]['Day']>5:
            return 3,3
        else:
            return 4,3

def checkNurse(name, purpose=1):
    #check if can work on holiday
    if purpose==1:
        return nurses[name]['work on hols']==1
    #check for half day per week
    if purpose==2:
        return nurses[name]['1 half / week']==1
    
def classifyNurse():
    nurseClass={
        'Senior':[],
        'Mid':[],
        'Junior':[]
        }
    for i in nurses:
        nurseClass[nurses[i]['level']].append(i)
    
    return nurseClass

def getMinCount(dateVal, level):
    if level=='Junior' and dates[dateVal]['Day']<6:
        return 1
    else:
        return 1

def publicHol():
    for i in dates:
        if checkDate(i)==True:
            for j in nurses:
                if checkNurse(j)==False:
                    for k in shifts:
                        if k=='DO':
                            model.Add(shiftAlloc[(i,j,k)]==1)
#                            model.AddAllowedAssignments(shiftAlloc[(i,j,k)],1)
                        else:
                            model.Add(shiftAlloc[(i,j,k)]==0)
#                            model.AddAllowedAssignments(shiftAlloc[(i,j,k)],0)
#                            model.

def halfday():
    dateCol=list(dates)
    for k in nurses:
        if checkNurse(k,2)==True:
            for p in range(4):
                curDates=dateCol[p*7:p*7+7]
                model.Add(sum(shiftAlloc[(i, k, 'M3')] for i in curDates)>=1)

def calHours(dateVal, shift):
    if shift=='DO':
        if checkDate(dateVal)==True:
            return 8
        else:
            return 0
    else:
        return shifts[shift]['Work hours']

def hoursPerWeek(mode=1):
    for j in nurses:
        for p in range(4):
            curDates=list(dates)[p*7:p*7+7]
#            store=[calHours(x,y) for x in curDates for y in shifts]
            if mode==1:
                model.Add(sum(shiftAlloc[(i,j,k)]*calHours(i,k) for i in curDates for k in shifts)>=44)

            if mode==2:
                model.Add(sum(shiftAlloc[(i,j,k)]*calHours(i,k) for i in curDates for k in shifts)==44)

def dayOff():
    for j in nurses:
        for p in range(4):
            curDates=list(dates)[p*7:p*7+7]
            #changed to ==1 to speed up. Should be >=
            model.Add(sum(shiftAlloc[(i,j,'DO')] for i in curDates)>=1)
            
def aftSeniority():
    seniority=classifyNurse()
    aftSess=['A1','A2']
    for i in dates:
        for level in seniority:
            tem=seniority[level]
            model.Add(sum(shiftAlloc[(i,j,k)] for j in tem for k in aftSess)>=getMinCount(i,level))
        
def minNurse():
    mornSess=['M1','M2','M3']
    aftSess=['A1','A2']
    for i in dates:
        mornMin, aftMin=checkDate(i, 2)
        model.Add(sum(shiftAlloc[(i,j,k)] for j in nurses for k in mornSess)>=mornMin)
        model.Add(sum(shiftAlloc[(i,j,k)] for j in nurses for k in aftSess)>=aftMin)
        
def sameShift(mode=1):
    #mode1 = weekly, mode2=whole month
    mornSess=['M1','M2','M3']
    aftSess=['A1','A2']
    nurseList=['Adam', 'Fae']
    
#    allSess=mornSess+aftSess
    for j in nurseList:
        if mode==1:
            for p in range(4):
                curDates=list(dates)[p*7:p*7+7]
                for count, val in enumerate(curDates):
                    if count<len(curDates)-1:
                        model.Add(sum(shiftAlloc[val,j,k] for k in mornSess)==\
                                  sum(shiftAlloc[curDates[count+1],j,k] for k in mornSess))
                        model.Add(sum(shiftAlloc[val,j,k] for k in aftSess)==\
                                  sum(shiftAlloc[curDates[count+1],j,k] for k in aftSess))
        if mode==2:
            curDates=list(dates)
            for count,val in enumerate(curDates):
                if count<len(curDates)-1:
                    model.Add(sum(shiftAlloc[val,j,k] for k in mornSess)==\
                        sum(shiftAlloc[curDates[count+1],j,k] for k in mornSess))
                    model.Add(sum(shiftAlloc[val,j,k] for k in aftSess)==\
                        sum(shiftAlloc[curDates[count+1],j,k] for k in aftSess))

def noAftAfterOff(purpose=1):
    aftSess=['A1','A2']
    dateList=list(dates)
    lst=[]
    count=0
    
    for j in nurses:
        for count,val in enumerate(dateList):
            if count<len(dateList)-1:
                nextDay=dateList[count+1]
                if purpose==1:
                    model.Add(shiftAlloc[(val,j,'DO')]+sum(shiftAlloc[(nextDay,j,k)] for k in aftSess)<=1)
                if purpose==2:
                    count+=1
                    lst.append(shiftAlloc[(val,j,'DO')])
                    lst+=[shiftAlloc[(nextDay,j,k)] for k in aftSess]
    
    if purpose==2:
        return lst, count

def noConsecutiveAftShit(days=3, purpose=1):
    aftSess=['A1','A2']
    dateList=list(dates)
    lst=[]
    count=0
    for j in nurses:
        for count,val in enumerate(dates):
            if count<len(dates)-days:
                dateCheck=[dateList[x] for x in range(count,count+days)]
                
                if purpose==1:
                    model.Add(sum(shiftAlloc[(i,j,k)] for i in dateCheck for k in aftSess)<days)
                
                if purpose==2:
                    for d in dateCheck:
                        lst+=[shiftAlloc[(d,j,k)] for k in aftSess]
                    count=count+days-1
    
    if purpose==2:
        return lst, count
    
def writeAnsToDB():
    colLst=['n1','n2','n3','n4','n5','n6','n7','n8','n9']
    df=pd.DataFrame(index=list(dates))
    for count,j in enumerate(nurses):
        lst=[]
        for i in dates:
            for k in shifts:
                if(solver.Value(shiftAlloc[(i,j,k)])==1):
                    lst.append(k)
        df[colLst[count]]=lst
        
#    return df
    
    db.rewriteTable('friar', df)
    
def returnAns():
    df=pd.DataFrame(index=list(nurses))
    for i in dates:
        lst=[]
        for j in nurses:
            for k in shifts:
                if(solver.Value(shiftAlloc[(i,j,k)])==1):
                    lst.append(k)
        df[i]=lst
    
    df.to_csv(fname, index=True)
    return df

def updateJobDone(jobId, elapsed):
    cols=['duration']
    ans=[str(elapsed)]
    db.editRow('jobs',cols,ans,'intjobid',jobId)
    
    
def runProg(mode=1, jobId=''):
    start=time.time()
    
    initModel()
    dateVal()
    defNurses()
    defShifts()
    
    defShiftAlloc(mode)
    #publicHol()
    halfday()
    hoursPerWeek(mode)
    dayOff()
    aftSeniority()
    minNurse()
    
    #sameShift(1)
    #noAftAfterOff()
    #noConsecutiveAftShit()
    
    print("ready to model at %s" %(str((time.time()-start)/60)))
    status=solver.Solve(model)
    elapsed=str((time.time()-start)/60)
    print("done at %s" %(str(elapsed)))
    
    if status==cp_model.INFEASIBLE:
        print("infeasible")
    if status==cp_model.FEASIBLE:
        print("feasible")
        a=returnAns()
        writeAnsToDB()
    if status==cp_model.MODEL_INVALID:
        print("Invalid")
    if status==cp_model.OPTIMAL:
        print("Optimal")
        a=returnAns()
        writeAnsToDB()
    
    if jobId!='':
        updateJobDone(jobId, elapsed)

if __name__ == "__main__":
    runProg()
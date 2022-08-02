
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

class PassException(Exception): pass

# returns true or false if containing any empty values (np.nan)
def isEmpty(dfChunk,index,nRows,searchTarget):
    return pd.isna(dfChunk.iloc[index - nRows,dfChunk.columns.get_loc(searchTarget)])

def isEngland(row):
    if row["country"] == "E":
        return True
    return False

def sortArr(arr):
    return sorted(arr, key=lambda x: x[2], reverse=True)

def takeMonth(newDate):
    return (pd.Timestamp(newDate) + pd.DateOffset(months=-1)).strftime('%Y-%m-%d')

def addMonth(newDate):
    return (pd.Timestamp(newDate) + pd.DateOffset(months=+1)).strftime('%Y-%m-%d')

def finalCreateStats():
    # csv with 15271850 chunks / rows
    f1 = "C:/Users/Ilya/Documents/LandHawk/lyr_planning_applications_temp_202206171319_countries.csv"
    f2 = "C:/Users/Ilya/Documents/LandHawk/monthly_authority_stats.csv"
    f2templ = "C:/Users/Ilya/Documents/LandHawk/monthly_authority_stats_template.csv"
    acc1 = "C:/Users/Ilya/Documents/LandHawk/authority_country_codes.csv"

    cz = 10000  # recommended chunksize 10000
    accLen = 489  # number of unique authorities in the database
    nRows,masRows = 0,0  # row count
    arrStore,arrStore2,arrStore3 = [],[],[]  # misc array storages
    years5,years10 = [],[]
    authid,searchTarget = "authority_id","authority_id"
    appstate,searchTarget2 = "app_state","app_state"
    CurrentDate = datetime.datetime.now()
    acA,acB,acC = 0,0,0  # array counts
    header = True
    newDate = "2021-07-01"  # current or next month

    for monthOffset in range(1):
        newDate = takeMonth(newDate)

        masWork = pd.read_csv(f2templ)
        masWork = masWork.assign(date=newDate)

        for dfChunk in pd.read_csv(f1,chunksize=cz):
            nRows = nRows + len(dfChunk)
            # if nRows > cz:
            #     break
            print(f"~~~ processing chunk {nRows - cz:,} - {nRows:,} ~~~")

            for index,row in dfChunk.iterrows():
                # print(row["decided_date"]) #str type

                # UK / ENGLAND
                # if not isEngland(row):
                #     continue

                try:  # try to catch TypeError when DateObj is nan - when no decided_date is given
                    DateObj = datetime.datetime.strptime(row["decided_date"],"%Y-%m-%d")

                    # DATE CONTROL

                    if DateObj >= (datetime.datetime.strptime(newDate,"%Y-%m-%d")):
                        if DateObj < (datetime.datetime.strptime(addMonth(newDate),"%Y-%m-%d")):

                            # COLLECTING STATS

                            for masindex,masrow in masWork.iterrows():
                                if row[authid] == masrow[authid]:
                                    if row[appstate] == "Permitted":
                                        masWork.at[masindex,"permitted"] += 1
                                    elif row[appstate] == "Rejected":
                                        masWork.at[masindex,"rejected"] += 1
                                    elif row[appstate] == "Withdrawn":
                                        masWork.at[masindex,"withdrawn"] += 1
                                    masWork.at[masindex,"total"] += 1
                                    break

                except TypeError:
                    continue

        header = False
        masWork.to_csv(f2,header=header,mode='a',index=False)




def singleBarChart(arr):
    arr1 = sortArr(arr)

    labels, ys = zip(*arr1)
    xs = np.arange(len(labels))

    plt.bar(xs, ys, align='center')

    plt.show()

def doubleBarChart(arr1,arr2):
    arr1 = sortArr(arr1)
    arr2 = sortArr(arr2)

    width = 0.35
    opacity = 0.4

    labels, ys = zip(*arr1)
    xs = np.arange(len(labels))
    rects5 = plt.bar(xs, ys, width,
                     alpha=opacity,
                     color='b',
                     label='5 Year')

    labels, ys = zip(*arr2)
    xs = np.arange(len(labels))
    rects10 = plt.bar(xs + width, ys, width,
                      alpha=opacity,
                      color='orange',
                      label='10 Year')

    plt.ylabel('Frequency')
    plt.xticks(xs + width / 2, labels)  # Replace default x-ticks to fit multiline, then replace xs with labels
    # plt.yticks(ys)
    plt.legend()

    plt.show()
    # to continue code after showing graph 2 options - save as image or use threading:
    # plt.savefig('img.png')
    # https://python-forum.io/thread-20965.html

# original checkEngland inside main:
# try:
#     for accindex, accrow in acc.iterrows():
#         if accrow["id"] == row["authority_id"]:
#             if accrow["country"] == "E":
#                 break
#             raise PassException
# except PassException:
#     continue

# running:
# check510years(DateObj,CurrentDate,years5,years10,row,searchTarget)
def check510years(DateObj,CurrentDate,years5,years10,row,searchTarget):
    if DateObj > (CurrentDate + relativedelta(years=-5)):
        found = False
        for i in range(len(years5)):
            if years5[i][0] == row[searchTarget]:
                years5[i][1] += 1
                found = True
                break

        if not found:
            years5.append([row[searchTarget], 1])

    if DateObj > (CurrentDate + relativedelta(years=-10)):
        found = False
        for i in range(len(years10)):
            if years10[i][0] == row[searchTarget]:
                years10[i][1] += 1
                found = True
                break

        if not found:
            years10.append([row[searchTarget], 1])
import time
# import datetime
# from dateutil.relativedelta import relativedelta
# # import pandas as pd
# # import dask.dataframe as dd
# import numpy as np
# import matplotlib.pyplot as plt
from functions import *
import psycopg2
import psycopg2.extras
# import sys
# import pprint
import os
from dotenv import load_dotenv
import logging

def main():
    try:
        # START OF PROGRAM
        start = time.time()
        eDate = datetime.datetime.today().strftime('%Y-%m-01')

        # pull credentials out of env
        load_dotenv()
        LH_HOST = os.getenv('LH_HOST')
        LH_DATABASE = os.getenv('LH_DATABASE')
        LH_PORT = os.getenv('LH_PORT')
        LH_USERNAME = os.getenv('LH_USERNAME')
        LH_PASSWORD = os.getenv('LH_PASSWORD')

        # get a connection, if a connect cannot be made an exception will be raised here
        try:
            conn = psycopg2.connect("host='{h}' dbname='{d}' user='{u}' password='{p}' "
                                    .format(h=LH_HOST,d=LH_DATABASE,u=LH_USERNAME,p=LH_PASSWORD))
        except Exception as e:
            logging.exception("Connection could not be established")
            return

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        curs = conn.cursor()
        print("Connected!\n")
        logging.info('Connection established')

        for i in range(1):

            sDate = takeMonth(eDate)

            # execute our Query
            try:
                curs.execute("""select 
                            authority_id, 
                            authority_name, 
                            COUNT(app_state) filter (where app_state = 'Undecided') as undecided, 
                            COUNT(app_state) filter (where app_state = 'Permitted') as permitted, 
                            COUNT(app_state) filter (where app_state = 'Withdrawn') as withdrawn, 
                            COUNT(app_state) filter (where app_state = 'Conditions') as conditions, 
                            COUNT(app_state) filter (where app_state = 'Referred') as referred, 
                            COUNT(app_state) filter (where app_state = 'Rejected') as rejected, 
                            COUNT(app_state) filter (where app_state = 'Unresolved') as unresolved, 
                            COUNT(app_state) 
                            from 
                            _planning.lyr_planning_applications as apps 
                            left join _planning.planning_authorities_metadata meta on (apps.authority_id = meta.id) 
                            where start_date >= '{s}' and start_date < '{e}'
                            group by authority_id, authority_name 
                            order by authority_name """.format(s=sDate,e=eDate)
                )
            # lyr_planning_applications
            # tbl_monthly_authority_stats
            except Exception as e:
                logging.exception("SELECT query failed")
                return

            # retrieve the records from the database
            rows = curs.fetchall()

            # complete data computation
            for nRow in range(len(rows)):
                a = list(rows[nRow])
                b = str(rows[nRow][0]) + "/" + sDate
                a.insert(0, b) # uid pk
                a.insert(3, sDate) # date log
                rows[nRow] = tuple(a)

            # insert new data
            try:
                curs.executemany(
                    """INSERT INTO {t} ({d})
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(t="_planning.tbl_monthly_authority_stats",
                                                            d="uid,authority_id,authority_name,date,undecided,permitted,withdrawn,"
                                                              "conditions,referred,rejected,unresolved,total"),rows
                )
            except Exception as e:
                logging.exception("INSERT command failed")
                return

            eDate = takeMonth(eDate)

        # print out records using for loop
        # print("Rows: \n")
        # for row in rows:
        #     print("   ", row)

        # print out the records using pretty print
        # pprint.pprint(rows)

        # display all stats
        # curs.execute("""select * from _planning.tbl_monthly_authority_stats""")
        # rows = curs.fetchall()
        # for row in rows:
        #     print("   ", row)

        conn.commit()
        curs.close()
        conn.close()

        # TIMESTAMP
        end = time.time()
        print("Complete in: ",(end-start),"sec")
        logging.info(f'Complete in: {end-start}sec')

    except Exception as e:
        logging.exception("Error has occurred")
        return


if __name__ == "__main__":
    main()
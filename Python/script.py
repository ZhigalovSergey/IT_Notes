import pandas as pd
from google.oauth2 import service_account
import pyodbc
import argparse
import logging
import time
import os

def load(sql_conn, project_id, credentials, date_from, date_to, cnt_shard, number)
    sql = "select fl1, fl2 from src_tbl where mt_update_dt>='" + date_from + "' and mt_update_dt<'" + date_to + "' and lot_key%"+str(cnt_shard)+"="+str(number)
    logging.info("loading from src")
    df = pd.read_sql(con=sql_conn, sql=sql) # извлекаем данные для прогрузки
    logging.info("uploading to tgt")
    
    d10_sec = 10
    retry_insert_gbq = 1
    res = ''
    while res != 'success':
        try:
            df.to_gbq('tgt_tbl', project_id=project_id, chunksize=50000, if_exists='replace', credentials=credentials, table_schema=[{"name": "fl1", "type": "INTEGER"}, {"name": "fl2", "type": "STRING"}]) # выгружаем в GBQ
            res = 'success'
        except Exception as error:
            logging.info("type exception: " + type(error).__name__)
            logging.info("message exception: " + str(error))
            logging.info("sleep: " + str(d10_sec*retry_insert_gbq))
            time.sleep(d10_sec*retry_insert_gbq)
            if retry_insert_gbq > 3:
                raise error
            retry_insert_gbq += 1
    logging.info("uploading to tgt finished")
    
def main():
    logging.basicConfig(handlers=[logging.FileHandler(filename='load.log', encoding='utf-8', mode='w')],
                                format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                                datefmt='%H:%M:%S',
                                level=logging.INFO)
    logging.info("pid: " + str(os.getpid())) # логируем номер процесса, под которым запущен скрипт
    
    ap = argparse.ArgumentParser()
    ap.add_argument("json_credentials", type=str,
                    help="path for json file with credentials")
    args = ap.parse_args()
    json_credentials = args.json_credentials
    
    try:
        sql_conn = pyodbc.connect('''DRIVER={ODBC Driver 17 for SQL Server};
                                SERVER=****,1433;
                                DATABASE=MDWH;
                                Trusted_Connection=yes''')
                                
        credentials = service_account.Credentials.from_service_account_file(json_credentials)
        project_id = 'goods-161014'

        sql_tgt = "select ifnull(MAX(mt_update_dt), '1990-01-01') mt_update_dt from tgt_tbl"
        df_tgt = pd.read_gbq(sql_tgt, project_id=project_id, credentials=credentials, dialect='standard')
        max_dt_tgt = str(df_tgt['mt_update_dt'][0]).split('+')[0]
        load_from = pd.to_datetime(max_dt_tgt).date() - pd.DateOffset(days=0)

        sql_src = "select max(mt_update_dt) mt_update_dt from src_tbl"
        df_src = pd.read_sql(con=sql_conn, sql=sql_src)
        max_dt_src = str(df_src['mt_update_dt'][0]).split('+')[0]
        load_to = pd.to_datetime(max_dt_src).date() + pd.DateOffset(days=0)
        
        logging.info("loading from " + str(load_from) + " to " + str(load_to))
        list_dt = pd.date_range(load_from, load_to).to_pydatetime().tolist()
        
        for dt in list_dt:
            date_from = str(dt)
            date_to = str(dt + pd.DateOffset(days=1))
            sql_src = "select count(*) cnt from src_tbl where mt_update_dt >= '" + date_from + "' and mt_update_dt < '" + date_to + "'"
            df_count = pd.read_sql(con=sql_conn, sql=sql)
            cnt = str(df_count['cnt'][0]).split('+')[0]
            cnt_shard = int(cnt)//50000 + 1 # кол-во итераций, которое нужно для прогрузки. 50000 - максимальное кол-во строк в итерации
            number = 0 # значение функции шардирования
            while number < cnt_shard: # лучше использовать while, так как легче организовать retry в случае падения
                logging.info("iteration loading from " + date_from + " to " + date_to + ", shard " + str(number) " of " + str(cnt_shard))
                load(sql_conn, project_id, credentials, date_from, date_to, cnt_shard, number)
                number += 1
    except Exception as error:
        logging.info("type exception: " + type(error).__name__)
        logging.info("message exception: " + str(error))

if __name__ == '__main__':
    main()
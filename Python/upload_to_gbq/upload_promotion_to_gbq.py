import pandas as pd
from google.oauth2 import service_account
import pyodbc
import argparse
import logging
import time
import os

def load(sql_conn, project_id, credentials, date_from, date_to, cnt_shard, number):
    sql = '''select
                promotion_id
                ,name
                ,type_id
                ,merchant_id
                ,mt_update_dt
            from
                raw_mms_mss_merchantpromo.promotion''' + " where mt_update_dt>='" + date_from + "' and mt_update_dt<'" + date_to + "' and promotion_id%"+str(cnt_shard)+"="+str(number)
    logging.info("loading from raw_mms_mss_merchantpromo.promotion")
    df = pd.read_sql(con=sql_conn, sql=sql) # извлекаем данные для прогрузки
    logging.info("loading from raw_mms_mss_merchantpromo.promotion finished")

    logging.info("uploading to dwh_input.promotion_tmp")
    d10_sec = 10
    retry_insert_gbq = 1
    res = ''
    while res != 'success':
        try:
            df.to_gbq('dwh_input.promotion_tmp', project_id=project_id, chunksize=50000, if_exists='replace', credentials=credentials, 
                table_schema=[
                                {"name": "promotion_id", "type": "INTEGER"}, 
                                {"name": "name", "type": "STRING"},
                                {"name": "type_id", "type": "INTEGER"}, 
                                {"name": "merchant_id", "type": "INTEGER"}, 
                                {"name": "mt_update_dt", "type": "DATETIME"}
                                ]) # выгружаем в GBQ
            res = 'success'
        except Exception as error:
            logging.info("type exception: " + type(error).__name__)
            logging.info("message exception: " + str(error))
            logging.info("sleep: " + str(d10_sec*retry_insert_gbq))
            time.sleep(d10_sec*retry_insert_gbq)
            if retry_insert_gbq > 3:
                raise error
            retry_insert_gbq += 1
    logging.info("uploading to dwh_input.promotion_tmp finished")

    sql_insert = '''
        insert into dwh_input.promotion (
            promotion_id
            ,name
            ,type_id
            ,merchant_id
            ,mt_update_dt
            )
        select
            promotion_id
            ,name
            ,type_id
            ,merchant_id
            ,mt_update_dt
        from dwh_input.promotion_tmp as src
        where 
            not exists (
                        select 1
                        from dwh_input.promotion as tgt 
                        where src.promotion_id = tgt.promotion_id
                        )
    '''
    logging.info("inserting to dwh_input.promotion")
    pd.read_gbq(sql_insert, project_id=project_id, configuration = {"query": {"timeoutMs": 600000}}, credentials=credentials, dialect='standard')
    logging.info("inserting to dwh_input.promotion finished")

    sql_update = '''
        update dwh_input.promotion tgt
        set  tgt.name =                 src.name
            ,tgt.type_id =              src.type_id
            ,tgt.merchant_id =          src.merchant_id
            ,tgt.mt_update_dt =         src.mt_update_dt
        from dwh_input.promotion_tmp as src
        where src.promotion_id = tgt.promotion_id
            and not exists (
                select 
                    src.name
                    ,src.type_id
                    ,src.merchant_id
                    ,src.mt_update_dt
                intersect distinct
                select 
                    tgt.name
                    ,tgt.type_id
                    ,tgt.merchant_id
                    ,tgt.mt_update_dt
                )
    '''
    logging.info("updating dwh_input.promotion")
    pd.read_gbq(sql_update, project_id=project_id, configuration = {"query": {"timeoutMs": 600000}}, credentials=credentials, dialect='standard')
    logging.info("updating dwh_input.promotion finished")

def main():  
    ap = argparse.ArgumentParser()
    ap.add_argument("json_credentials", type=str,
                    help="path for json file with credentials")
    ap.add_argument("log_file", type=str,
                    help="path for log file")
    args = ap.parse_args()
    json_credentials = args.json_credentials
    log_file = args.log_file
    logging.basicConfig(handlers=[logging.FileHandler(filename=log_file, encoding='utf-8', mode='w')],
                                format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                                datefmt='%H:%M:%S',
                                level=logging.INFO)
    logging.info("pid: " + str(os.getpid())) # логируем номер процесса, под которым запущен скрипт
    
    try:
        sql_conn = pyodbc.connect('''DRIVER={ODBC Driver 17 for SQL Server};
                                SERVER=dwh.prod.lan,1433;
                                DATABASE=mdwh_raw;
                                Trusted_Connection=yes''')
                                
        credentials = service_account.Credentials.from_service_account_file(json_credentials)
        project_id = 'goods-161014'

        sql_tgt = "select ifnull(MAX(mt_update_dt), '1990-01-01') mt_update_dt from dwh_input.promotion"
        df_tgt = pd.read_gbq(sql_tgt, project_id=project_id, credentials=credentials, dialect='standard')
        max_dt_tgt = str(df_tgt['mt_update_dt'][0]).split('+')[0]
        load_from = pd.to_datetime(max_dt_tgt).date() - pd.DateOffset(days=0)

        sql_src = "select max(mt_update_dt) mt_update_dt from raw_mms_mss_merchantpromo.promotion"
        df_src = pd.read_sql(con=sql_conn, sql=sql_src)
        max_dt_src = str(df_src['mt_update_dt'][0]).split('+')[0]
        load_to = pd.to_datetime(max_dt_src).date() + pd.DateOffset(days=0)
        
        logging.info("loading from " + str(load_from) + " to " + str(load_to))
        list_dt = pd.date_range(load_from, load_to).to_pydatetime().tolist()
        
        for dt in list_dt:
            date_from = str(dt)
            date_to = str(dt + pd.DateOffset(days=1))
            sql_src = "select count(*) cnt from raw_mms_mss_merchantpromo.promotion where mt_update_dt >= '" + date_from + "' and mt_update_dt < '" + date_to + "'"
            df_count = pd.read_sql(con=sql_conn, sql=sql_src)
            cnt = str(df_count['cnt'][0]).split('+')[0]
            cnt_shard = int(cnt)//50000 + 1 # кол-во итераций, которое нужно для прогрузки. 50000 - максимальное кол-во строк в итерации
            number = 0 # значение функции шардирования
            while number < cnt_shard: # лучше использовать while, так как легче организовать retry в случае падения
                logging.info("iteration loading from " + date_from + " to " + date_to + ", shard " + str(number) + " of " + str(cnt_shard))
                load(sql_conn, project_id, credentials, date_from, date_to, cnt_shard, number)
                number += 1
    except Exception as error:
        logging.info("type exception: " + type(error).__name__)
        logging.info("message exception: " + str(error))

if __name__ == '__main__':
    main()
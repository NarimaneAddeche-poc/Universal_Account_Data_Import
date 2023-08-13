from pathlib import Path
import logging
import sqlalchemy
import pandas as pd
from IPython.display import display

class LoadDataFromSQL:
    queries = None
    mssql_handler = None
    clickhouse_handler = None
    bigquery_handler = None
    tedis_company_key = None
    logging_params = {"ETL_name": "sale_dashboard_tedis"}

    def __init__(self):
        self.database = 'poc-dwh'
        self.user = 'poc-admin'
        self.password = 'x6J^S444pSmy'
        self.host = 'pocvm1.eastasia.cloudapp.azure.com'


    def innit_mssql_connection_engine(self):
        engine = sqlalchemy.create_engine(
            'mssql+pymssql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(
                self.user, self.password, self.host, 1433, self.database))
        return engine
    
    def innit_postgresql_connection_engine(self):
        #engine = sqlalchemy.create_engine('postgresql://datateam:5ZpgMvoMzgmh_mD@10.148.0.10:5432/data_analytics')
        engine = sqlalchemy.create_engine('postgresql://postgres:myPassword@localhost:5432/DBAlch')
        return engine

    def get_df_from_sql_server(self, query):
        engine = self.innit_mssql_connection_engine()
        df_result = pd.DataFrame()
        try:
            df_result = pd.read_sql(query,engine)
            logging.info(f"------ Get {df_result.shape[0]} rows from SQL Server ------")
            return df_result
        except Exception as err:
            logging.error(f"Error when execute query on SQL Server, error: {err}")
            raise

    def extract_data_from_mssql_source(self, target_table_name):
        query = f'select * from {target_table_name}'
        print(query)
        input_df = self.get_df_from_sql_server(query)
        display(input_df)
        return input_df



    def load_data_to_postgresql(self, df, target_table_name):
        postgre_engine = self.innit_postgresql_connection_engine()
        df.to_sql(target_table_name, postgre_engine)


    def main(self, target_table_name):
        print('####### Start ETL: Loading Data from SQL: ######')
        print(f'####### Target Table Name: {target_table_name} ######')
        input_df = self.extract_data_from_mssql_source(target_table_name=target_table_name)
        print(input_df)
        self.load_data_to_postgresql(input_df, target_table_name = target_table_name)
        # self.load_data_to_bigquery(input_df, target_table_name = target_table_name)
        # self.mssql_handler.insert_etl_log_table(logging_params=self.logging_params)


if __name__ == '__main__':

    list_of_table_to_import=['DWH.CompanyPharmacy','DWH.Pharmacy','DWH.PharmacyAddress','DWH.Location','DWH.account','DWH.City','DWH.District']
    
    obj = LoadDataFromSQL()
    for element in list_of_table_to_import:
        obj.main(target_table_name=element)
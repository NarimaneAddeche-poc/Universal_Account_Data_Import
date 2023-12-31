
import pandas as pd
import time
from re import search
import numpy as np
import psycopg2
from pathlib import Path
from datetime import datetime

def db_connection():
    cnx = psycopg2.connect(database="DBAlch",
                            host="localhost",
                            user="postgres",
                            password="myPassword",
                            port="5432")
    return cnx

conn=db_connection()
cursor = conn.cursor()
  

def data_source_insertion(data_source_values):
    sql=""" INSERT INTO data_sources (name,country_id,created_at,updated_at) VALUES (%s,%s,%s,%s)"""
    cursor.executemany(sql,data_source_values)
    conn.commit()

def get_matched_data_source_id(matched_file):
    file_name=Path(matched_file).stem
    data=(file_name,)
    select_query="select id from data_sources where name = %s limit 1 "
    cursor.execute(select_query,data)
    matched_data_source_id=cursor.fetchone()[0]
    return (matched_data_source_id)

def matched_items_insertion(matched_list):
    sql=""" INSERT INTO data_source_items (data_source_id, data_source_code, name, address,city,district,created_at,updated_at) 
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(sql,matched_list)
    conn.commit()

def get_base_data_source_id(base_file):
    base_list=[]
    file_name=Path(base_file).stem
    data=(file_name,)
    select_query="select id from data_sources where name = %s limit 1 "
    cursor.execute(select_query,data)
    base_data_source_id=cursor.fetchone()[0]
    return (base_data_source_id)
    ############################################################

def base_data_items_source_insertion(base_list):
    sql=""" INSERT INTO data_source_items (data_source_id, data_source_code, name, address,city,district,created_at,updated_at) 
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(sql,base_list)
    conn.commit()

def get_base_items_id(data):
             select_query="select id from base_items where base_item_code = %s and matching_attempts_id= %s"
             cursor.execute(select_query,data)
             base_items_id=cursor.fetchone()[0]
             return(base_items_id)

def get_matching_type(type):
############## get matching type id ########################
    data=(type,)
    select_query="select id from matching_attempt_types where type = %s"
    cursor.execute(select_query,data)
    type_id=cursor.fetchone()[0]
    return(type_id)

def matching_attempts_insertion(matching):
    sql=""" INSERT INTO matching_attempts (matching_attempt_types_id, base_data_source_id, matched_data_source_id, number_of_items_in_base_list,number_of_items_in_matching_list,number_of_matches_found,number_of_not_matches,number_of_human_check_required,number_of_wrong_matching_decisions,created_at,updated_at) 
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cursor.execute(sql,matching)
    conn.commit()

def get_matching_attempts_id(base_data_source_id,matched_data_source_id):
    data=(base_data_source_id,matched_data_source_id,)
    select_query="select id from matching_attempts where base_data_source_id = %s and matched_data_source_id= %s"
    cursor.execute(select_query,data)
    matching_attempts_id=cursor.fetchone()[0]
    return(matching_attempts_id)

def base_items_insertion(data_values):
    sql=""" INSERT INTO base_items (matching_attempts_id,base_item_code,created_at,updated_at) VALUES (%s,%s,%s,%s)"""
    cursor.executemany(sql,data_values)
    #print(data_values)
    conn.commit()

def possible_matched_items_insertion(data_values_possible): 
    sql=""" INSERT INTO possible_matched_items(matched_item_code,base_items_id,matching_methods,similarity,result,matching_verification_method,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(sql,data_values_possible)
    #print(data_values_possible)
    conn.commit()

def universal_account_location_insertion(record_to_insert):
    sql=""" INSERT INTO universal_account_location (name,country_id,city_id,district_id,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s)"""
    cursor.execute(sql,record_to_insert)
    conn.commit()

def get_location_id():
    select_query="select id from universal_account_location ORDER BY ID DESC LIMIT 1"
    cursor.execute(select_query)
    universal_account_location_id=cursor.fetchone()[0]
    return(universal_account_location_id)

def universal_account_verification_insertion(record_to_insert):
    sql=""" INSERT INTO universal_account_verification (provided_address,provided_facility_image,provided_registration_no,verified_facility_image,verified_phone,verified_email,has_valid_document,has_delivered_orders,has_registred_users,physically_visited_by_poc_rep,users_accessed_poc,from_trusted_clients_ids,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cursor.execute(sql,record_to_insert)
    conn.commit()

def get_universal_account_verification_id():
    select_query="select id from universal_account_verification ORDER BY ID DESC LIMIT 1"
    cursor.execute(select_query)
    universal_account_verification_id=cursor.fetchone()[0]
    return(universal_account_verification_id)

def universal_account_insertion(record_to_insert):
    sql=""" INSERT INTO universal_accounts (universal_account_code,universal_account_name,universal_location_id,account_verification_id,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s)"""
    cursor.execute(sql,record_to_insert)
    conn.commit()

def get_universal_account_id(code):
    data=(code,)
    select_query="select id from universal_accounts where universal_account_code = %s"
    cursor.execute(select_query,data)
    universal_account_id=cursor.fetchone()[0]
    return(universal_account_id)

def universal_account_code_mapping_insertion(record_to_insert):
    sql=""" INSERT INTO universal_account_code_mapping (universal_account_id,data_source_id,matched_item_code,created_at,updated_at) VALUES (%s,%s,%s,%s,%s)"""
    cursor.execute(sql,record_to_insert)
    conn.commit()

def get_city_id(country_id,city_name):    
    data=(country_id,city_name,)
    select_query=('select "Id" from "DWH.city" where "CountryId"= %s and "Name"=%s')
    cursor.execute(select_query,data)
    result=cursor.fetchone()
    return(result)

def get_district_id(country_id,city_name,district_name):    
    data=(country_id,city_name,district_name,)
    select_query=('select "Id" from "DWH.district" where "CountryId"= %s and "CityId"=%s and "Name"=%s')
    cursor.execute(select_query,data)
    result=cursor.fetchone()
    return(result)


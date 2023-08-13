import pandas as pd
import time
from re import search
import numpy as np
import psycopg2
from pathlib import Path
from datetime import datetime
import db_insertion_functions



def traitement_befor_insertion(type, country_id, base_file, matched_file, result_file):
    dfBase = pd.read_excel(base_file,'Sheet3')
    dfMatched = pd.read_excel(matched_file,'Sheet3') 
    dfResult = pd.read_excel(result_file,'Sheet3')

    ####### data-sources#####
    data_source_values=([(Path(base_file).stem,country_id,current_dateTime,current_dateTime,),
                        (Path(matched_file).stem,country_id,current_dateTime,current_dateTime,)])
    #db_insertion_functions.data_source_insertion(data_source_values)


    ###### matched items insertion
    matched_data_source_id=db_insertion_functions.get_matched_data_source_id(matched_file)
    matched_list=[]
    for index, row in dfMatched.iterrows():
        code=str(row['ID'])
        name=str(row['Name']).replace("'","'"+"'")
        address=str(row['Address']).replace("'","'"+"'")
        city=str(row['City']).replace("'","'"+"'")
        district=str(row['District']).replace("'","'"+"'")
        value=(matched_data_source_id,code,name,address,city,district,current_dateTime,current_dateTime)
        matched_list.append(value)
    #db_insertion_functions.matched_items_insertion(matched_list)

    ######## base items insertion 
    base_data_source_id=db_insertion_functions.get_base_data_source_id(base_file)
    base_list=[]
    list_of_codes=[]
    for index, row in dfBase.iterrows():
        code=str(row['ID'])
        name=str(row['Name']).replace("'","'"+"'")
        address=str(row['Address']).replace("'","'"+"'")
        city=str(row['City']).replace("'","'"+"'")
        district=str(row['District']).replace("'","'"+"'")
        value=(base_data_source_id,code,name,address,city,district,current_dateTime,current_dateTime)
        base_list.append(value)
        list_of_codes.append(value[1])
    #db_insertion_functions.base_data_items_source_insertion(base_list)

    ######### matching_attempts insertion
    type_id=db_insertion_functions.get_matching_type(type)
    
    try:
        number_of_items_in_base_list=int(len(dfBase.axes[0]))
    except:
        number_of_items_in_base_list=0
    try:
        number_of_items_in_matched_list=int(len(dfMatched.axes[0]))
    except:
        number_of_items_in_matched_list=0
    try:
        number_matches_found=int(dfResult['matching_results'].value_counts()['Surely Matched'])
    except:
        number_matches_found=0
    try:
        number_of_not_matches=int(dfResult['matching_results'].value_counts()['Surely No Similar Found'])
    except:
        number_of_not_matches=0
    try:
        number_of_manual_check_required=int(dfResult['matching_results'].value_counts()['Unsure likely Matched, manual check required'] + dfResult['matching_results'].value_counts()['Unsure unlikely Matched, manual check required'])
    except:
        number_of_manual_check_required=0
    number_of_wrong_matching_decisions=0
    matching=(type_id,base_data_source_id,matched_data_source_id,number_of_items_in_base_list,number_of_items_in_matched_list,number_matches_found,number_of_not_matches,number_of_manual_check_required,number_of_wrong_matching_decisions,current_dateTime,current_dateTime,)
    #db_insertion_functions.matching_attempts_insertion(matching)

    matching_attempts_id=db_insertion_functions.get_matching_attempts_id(base_data_source_id,matched_data_source_id)

    ####### base_items insertion
    data_values=[]
    for element in list_of_codes:
        list=(matching_attempts_id,element,current_dateTime,current_dateTime)
        data_values.append(list)
    #db_insertion_functions.base_items_insertion(data_values)

    ###### possible_matched items
    data_values_possible=[]
    for index, row in dfResult.iterrows():
        ####### if not null do that else insert without matching_code and put 0 in similarity ########
        if pd.isnull(dfResult.loc[index, 'matching_id']):
            
            matched_item_code='Null'
            matching_method='Null'
            similarity=0
            result_string=str(row['matching_results'])
            if result_string=='Surely Matched':
                result=1
            elif result_string=='Surely No Similar Found':
                result=2
            elif result_string=='Unsure likely Matched, manual check required':
                result=3
            else:
                result=4
            base_item_code=str(row['base_id'])
            data=(base_item_code,matching_attempts_id,)
            base_items_id=db_insertion_functions.get_base_items_id(data)
            
            matching_verification_method=1
            list_possible=(matched_item_code,base_items_id,matching_method,similarity,result,matching_verification_method,current_dateTime,current_dateTime)
            data_values_possible.append(list_possible)
        else:
            matched_item_code=int(row['matching_id'])
            matched_item_code=str(matched_item_code)
            matching_method=row['matching_type']
            similarity=row['similarity']
            result_string=str(row['matching_results'])
            if result_string=='Surely Matched':
                result=1
            elif result_string=='Surely No Similar Found':
                result=2
            elif result_string=='Unsure likely Matched, manual check required':
                result=3
            else:
                result=4
            base_item_code=str(row['base_id'])
            data=(base_item_code,matching_attempts_id,)
            base_items_id=db_insertion_functions.get_base_items_id(data)
            matching_verification_method=1
            list_possible=(matched_item_code,base_items_id,matching_method,similarity,result,matching_verification_method,current_dateTime,current_dateTime)
            data_values_possible.append(list_possible)
    #db_insertion_functions.possible_matched_items_insertion(data_values_possible)

    if type=='monthly':

        for index,row in dfResult.iterrows():
            matching_result=str(row['matching_results'])
            if matching_result=='Surely No Similar Found':
                name=str(row['base_address'])
                city_name=str(row['base_city'])
                district_name=str(row['base_district'])
                city_id=db_insertion_functions.get_city_id(country_id,city_name)
                
                print (city_id)
                district_id=db_insertion_functions.get_district_id(country_id,city_id,district_name)
                list_location=(name,country_id,city_id,district_id,current_dateTime,current_dateTime)
                db_insertion_functions.universal_account_location_insertion(list_location)
                provided_address=provided_facility_image=provided_registarion_no=verified_facility_image=verified_phone=verified_email=has_valid_document=has_delivered_orders=has_registred_users=physically_visited=user_accessed_poc= False
                from_trusted=[]
                list_verification=(provided_address,provided_facility_image,provided_registarion_no,verified_facility_image,verified_phone,verified_email,has_valid_document,has_delivered_orders,has_registred_users,physically_visited,user_accessed_poc,from_trusted,current_dateTime,current_dateTime)
                db_insertion_functions.universal_account_verification_insertion(list_verification)
                universal_account_code=str(row['base_id'])
                universal_account_name=str(row['base_name'])
                universal_location_id=db_insertion_functions.get_location_id()
                universal_verification_id=db_insertion_functions.get_universal_account_verification_id()
                list_UA=(universal_account_code,universal_account_name,universal_location_id,universal_verification_id,current_dateTime,current_dateTime)
                db_insertion_functions.universal_account_insertion(list_UA)
            if matching_result=='Surely Matched':
                universal_account_code=str(row['base_id'])
                print(universal_account_code)
                universal_account_id=db_insertion_functions.get_universal_account_id(universal_account_code)
                data_source_id=int(matched_data_source_id)
                matched_item_code=int(row['matching_id'])
                liste_UACM=(universal_account_id,data_source_id,matched_item_code,current_dateTime,current_dateTime)
                db_insertion_functions.universal_account_code_mapping_insertion(liste_UACM)
    

if __name__ == "__main__":

  
    start= time.time()
    current_dateTime = datetime.now()

    ##################################################
    ################# to modify ######################
    ################################################## 

    base_file="Sanisphere_example.xlsx"
    
    matched_file="UA_example.xlsx"

    result_file="results_sanisphere.xlsx"

    country_id=1

    type='monthly' #type of matching
    ##################################################



    ############# Connection to postgre DB ###########
   
    traitement_befor_insertion(type,country_id,base_file,matched_file,result_file)









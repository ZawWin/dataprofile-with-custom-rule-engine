# -*- coding: utf-8 -*-

"""
Created on Wed Feb  3 15:28:42 2021

@author: zwin
"""

import sqlalchemy as sa
from pathlib import Path
import pandas as pd
import datetime as dt
import configparser
from pandas_profiling import ProfileReport
import shutil
import gc

def get_all_sites(usr,pwd):
    """
    Collect all partner sites. Setup connection strings for each site.
    """
    print (f'\nEstablishing connection to the partner databases.')
    shields = 'lxdbcmnp:5454/shrxprod'
    conn_string = 'postgresql://{0}:{1}@{2}'.format(usr,pwd,shields)
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    location_query = 'select distinct locationid, lower(location) as location, location_name_tableau as reportname, region as Region from shrx_location_info a left join ba.location_region b on a.locationid = b.location_id order by location;'
    location_df = pd.read_sql(location_query,conn)
    location_df.loc[location_df['locationid']==106551,['location']]='baystate'
    location_df.loc[location_df['locationid']==201602,['location']]='cne'
    location_df.loc[location_df['locationid']==201501,['location']]='university'
    location_df.loc[location_df['locationid']==201909,['location']]='uhs'
    site_conn_string = 'postgresql://{}:{}@lxdb{}p:5454/{}prod'
    location_df['conn_string']=location_df.apply(lambda x:site_conn_string.format(usr,pwd,x['location'],x['location']),axis=1)
    location_df['conn_status']=''
    
    for index,row in location_df.iterrows():
        try:
            conn = sa.create_engine(row['conn_string']).connect()
            location_df.loc[index,'conn_status'] = 'Connected'
        except Exception:
            location_df.loc[index,'conn_status'] = 'Failed'
            print('Fail to connect to '+row['location']+'. Please review the connection string for the site '+ row['location']+"\n")
            
    site_lists = location_df.loc[location_df['conn_status']=='Connected']['locationid'].tolist()
    site_locations = location_df.loc[location_df['conn_status']=='Connected']['location'].tolist()
    
    print ('These are all sites that will be running. \n'+str(site_locations))
    
    return location_df,site_lists, site_locations


def exec_sql(usr, pwd,l, location_df, f, rule_path):
    """
    Read the input sql file.
    param : usrname, password, location_id, location_df, input_file
    """
    
    site = location_df[location_df['locationid']==l]['location'].item()
    print(site + ' Started.')
    
    conn_str = location_df[location_df['locationid']==l]['conn_string'].item()
    engine = sa.create_engine(conn_str)
    conn= engine.connect()
    conn.execution_options(isolation_level='AUTOCOMMIT')
    
    df = pd.DataFrame()
    rule_str = ''
    rule_folder = rule_path
    rule_df = pd.DataFrame()
    if (rule_folder!=''):
        rule_df = pd.read_excel(str(Path(Path(rule_folder)/'Custom Rule Engine.xlsx')))
        rule_str = rule_df['Business Rule'].str.cat().upper()
        
    file = open(Path(f))    
    file_str = file.read().upper()
    new_str = file_str[:file_str.find('FROM')]+rule_str+file_str[file_str.find('FROM')-1:]
    file.close()
    s = sa.text(new_str)
    
    for chunk in pd.read_sql(s,conn,chunksize = 10000):
        print ('Reading in chunk: '+str(len(chunk))+' rows.')
        df = df.append(chunk)
        
    #result = conn.execute(s)
    #name = result.keys()
    
    #df = pd.DataFrame(result.fetchall(), columns = name)
    print(site + ' Completed.')
    return df, rule_df
    
    
def read_config(input_f):
    """
    If there is a config file, read from a config file
    """
    today = str(dt.date.today())
    config = configparser.ConfigParser()
    config.read(str(Path(input_f)))
    usr_input = config['Input']
    usr = usr_input['user']
    pwd = usr_input['password']
    f = usr_input['input_sql']
    output_folder = usr_input['out_folder']
    output_file = usr_input['out_file']+'-'+today
    all_sites = usr_input['all_sites']
    individual = usr_input['individual']
    write_out= ''
    sites = usr_input['site_lists'].split()
    rule_folder = usr_input['rule_folder']
    
    return usr, pwd, f, write_out, output_folder, output_file, all_sites, individual,sites, rule_folder
    
        
def gather_input():
    """
    

    Returns
    -------
    usr : str
        Username
    pwd : str
        Password
    f : str
        Input File 
    write_out : str
        Y/N to whether the data should be written out to a file
    file_type : str
        File type of the output file (.excel or .csv only)
    output_folder : str
        The file path of the output file. If empty, output will be written to the same folder as the python file.
    output_file : str
        The name of the output file
    all_sites : str
        Y/N answer to whether all sites need to be run or not.

    """
    today = str(dt.date.today())
    usr = input(f'\nPlease enter your username: ')
    pwd = input(f'\nPlease enter your password: ')
    f = input (f'\nPlease enter the complete path of the SQL file you want to run, including the file name with extension(.sql): ')
    write_out = input(f'\nDo you need to write the result out to a file? (Enter Y/N only): ')
    output_folder=''
    output_file = ''
    individual = ''
    rule_folder = ''
    sites = []
    if write_out=='Y':
        output_folder = input(f'\nEnter the folder you would like to write the output to: ')
        output_file = input (f'\nEnter your output file name: ')
        output_file = output_file+'-'+today
        individual = input(f'\nDo you want to create an individual report for each site. For large datasets, it is highly recommended. (Enter Y/N only): ')
        
    all_sites= input(f'\nAre you running this for all sites? (Enter Y/N only): ')
    if (all_sites=='N'):
        site_check = input(f'\nDo you know the location id of the site(s) you want to run this SQL for? (Y/N only): ')
        if (site_check == 'Y'):
            sites = input(f'\nPlease enter site ID separated by space. You can enter more than one site.: ')
            sites = sites.split()
            
    custom = input(f'\nDo you have a custom rule engine you would like to use? (Enter Y/N only): ')
    if custom == 'Y':
        rule_folder = input('\nPlease enter the folder where your custom rules are saved at and make sure "Custom Rule Engine.xlsx" file is located in that folder: ')
    
    return  usr, pwd, f, write_out, output_folder, output_file, all_sites, individual,sites, rule_folder

def convert_date(df,cols):
    for col in cols:
        if (len(df[col].drop_duplicates())>1):
            df[col]=pd.to_datetime(df[col],errors='coerce')
        else:
            df[col]=df[col].astype(str)
        
    return 'Conversion Completed.'

def check_column(df, rule_df, collist):
    """
    

    Parameters
    ----------
    df : DataFrame. The dataset to check pass/fail value of the actual columns
    rule_df : Dataframe. The rule dataframe with pass/fail threshold value
    col : Name of the column to check

    Returns
    -------
    df_outcome : Dataframe. The outcome result of the checked field.

    """
    total = len(df)
    final_df = pd.DataFrame()
    for col in collist:
        pass_total = len(df.loc[df[col]=='PASS',col])
        fail_total = len(df.loc[df[col]=='FAIL',col])
        if fail_total!=0:    
            true_pass_rate = (pass_total/total)*100
        else:
            true_pass_rate = 100
        rule_df['New Col Name']= rule_df['Col Name'].apply(lambda x: x.strip("\""))
        pass_threshold = (rule_df.loc[rule_df['New Col Name']==col,'Threshold to Pass'].tolist()[0])*100
        check = 'FAIL'
        if (true_pass_rate >= pass_threshold):
            check = 'PASS'
        d = {
            'DataField':[col],
            'Quality Check Result':[check],
            'Number of rows that Pass':[pass_total],
            'Number of rows that Fail':[fail_total],
            'Pass Rate':[true_pass_rate],
            'Pass Threshold':[pass_threshold]
            }
        df_outcome=pd.DataFrame(data = d)
        final_df = final_df.append(df_outcome)
        
    return final_df



def exec_sql_multiple_sites():
    empty_df = pd.DataFrame(data = {'': ['No data available.']})  
    error_df = pd.DataFrame()
    config_flag = input (f'\nDo you have a config file you want to use? Please enter Y/N only:')
    sites=[]
    if config_flag == 'Y':
        config_path = input (f'\nPlease enter the full path to your config file: ')
        usr, pwd,input_f, write_out, output_folder, output_file, all_sites, individual, sites, rule_folder = read_config(config_path)
    else:       
        confirm= 'N'
        while (confirm != 'Y'):
            usr, pwd,input_f, write_out, output_folder, output_file, all_sites, individual, sites, rule_folder = gather_input()
            confirm = input (f'\nPlease confirm all your inputs are correct. (Enter Y/N only): ')
    
    if (all_sites=='Y' and sites):
        print ('You can\'t answer \'Y\' to run all sites and enter site lists at the same time. Conflicting values. Please review your config file and run again.')
        quit()
        
    location_df, site_lists, site_locations = get_all_sites(usr, pwd)
    output_folder = Path(''.join([output_folder,'/',dt.datetime.now().strftime("%y-%m-%d %H%M%S"),'/']))
    output_folder.mkdir(parents=True, exist_ok = True)
    
    final_df=pd.DataFrame()
    outcome_final_df = pd.DataFrame()
    if (all_sites == 'Y'): sites = site_lists
    if (all_sites == 'N' and not sites):
        for s in site_lists:
            s_name = location_df[location_df['locationid']==s]['reportname'].item()
            region = location_df[location_df['locationid']==s]['region'].item()
            confirm_site = input('Do you want to run for '+s_name+
                                 '? (Y/N only): ')
            if (confirm_site=='Y'): sites.append(s)
            else: print ('Skipping '+s_name)
            
    for s in sites:
        s= int(s)
        s_name = location_df[location_df['locationid']==s]['reportname'].item()
        region = location_df[location_df['locationid']==s]['region'].item()
        outcome_df = pd.DataFrame()
        try:
            df, rule_df = exec_sql(usr, pwd, s, location_df, input_f,rule_folder)                
            convert_date(df,['lot_expiration','rx_date_received','order_date','date_entered','expire_date','date_last_adjudicated','fillcalendardate','fill_date','date_verified','delivered'] )
            collist = [col for col in df.columns if "_CHECK" in col]
            if len(rule_df)>0:
                outcome_df  = check_column(df, rule_df, collist)
                outcome_df['site']=s_name
                outcome_df['region']=region
                outcome_final_df = outcome_final_df.append(outcome_df)
            else:
                outcome_df['site']=s_name
                outcome_df['region']=region
                outcome_df['']='No rule engine defined'
                outcome_final_df = outcome_final_df.append(outcome_df)

            for col in collist:
                df.drop(columns = col, inplace=True)
                
            if (len(df)==0):
                df = empty_df
            df['site'] = s_name
            df['region'] = region
            if (individual == 'N'):
                final_df = final_df.append(df)
            else:
                individual_file = s_name+'-'+output_file
                out_file = str(output_folder)+'\\'+ individual_file+'.html'
                ProfileReport(df,minimal=True,samples=None, reject_variables=False, correlations=None, missing_diagrams=None, duplicates=None, interactions=None).to_file(output_file = out_file)

        except Exception as e:
            print(f'\nError with '+s_name+'. Please check the error log at the end of the run.')
            err = str(e)
            err_df = pd.DataFrame(data={'Locaton':[s_name], 'ErrorMsg':[err]})
            error_df = error_df.append(err_df)
    
    
    if (individual == 'N'):
         ProfileReport(final_df,minimal=True,samples=None, reject_variables=False, correlations=None, missing_diagrams=None, duplicates=None, interactions=None).to_file(output_file = out_file)
         
    outcome_final_df.to_excel(str(Path(output_folder/'Quality Check Outcome-All Sites-'))+str(dt.date.today())+'.xlsx', index=False)
        
    if (len(error_df)>0):
        error_df.to_csv(str(Path(output_folder/'ErrorLog-'))+str(dt.date.today())+'.csv', sep='|', index=False)
        
    

    
    shutil.copy(src = input_f, dst = str(output_folder))
    if config_flag == 'Y':
        shutil.copy(src = config_path, dst = str(output_folder))
        
    #if rule_folder != '':
     #   shutil.copy(src = str(Path(Path(rule_folder)/'Custom Rule Engine.xlsx')), dst = str(output_folder))
        
    
    del df
    del final_df
    gc.collect()

    return print('The run completed.')


def main():
    exec_sql_multiple_sites()
    
if __name__ == '__main__':
    main()

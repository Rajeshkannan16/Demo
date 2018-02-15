# -*- coding: utf-8 -*-
"""
Created on Fri Aug 18 14:35:41 2017

@author: kinsingh
"""

from collections import defaultdict, OrderedDict 
import pandas as pd
import xml.etree.ElementTree as ET
import numpy as np
import os
import zipfile
import gzip
import datetime
import re
import time
import subprocess
from os.path import isfile, join
from datetime import timedelta
#import uuid


from pyspark.sql.functions import broadcast
from cassandra.cluster import Cluster	
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SQLContext
from pyspark import SparkContext


### Used default parameters ###
date_time=datetime.date.today() #- datetime.timedelta(days=1)
#date_time = datetime.date(2017,11,5)
date = datetime.datetime.strftime(date_time, "%Y%m%d")#"20170907" 
tech = '3G'
cno_file = r"/home/cnonoida/cno_data_input/CNO-Data_input_29_05_17.xlsx"
#cno_file = r'D:\CNO Data\cno_66\CNO-Data_input_29_05_17.xlsx'
###############################


#for MAY month
KEYSPACE = 'cno_bharti_africa'
PMFMTable = 'kpipmfm_3g'
#CMTable = 'cm_data_3g'
CMTable = 'cm_3g'
CMTable4G = 'cm_data_4g'

## Spark connector
sc = SparkContext().getOrCreate()
sqlContext = SQLContext(sc)

cluster = Cluster(['93.183.28.71'])		
session = cluster.connect()	

dir_date=datetime.datetime.strftime(date_time, "%d.%m.%Y")
dir_path=r"/mnt/cno_66/BHA/Kenya/"
#dir_date="07.09.2017"
#pm_dir=dir_path+dir_date+'/PM/'
dir_fm=os.path.join(dir_path,dir_date)+"/FM/"
cm_file_dir=os.path.join(dir_path,dir_date)+"/CM/"
pmfile_dir=r'/mnt/cno_66/BHA/Kenya_Netact/'
#pm_dir=pmfile_dir++str(dir_date)+'/'
pm_dir = os.path.join(pmfile_dir, dir_date)+"/"
print(pm_dir,dir_fm,cm_file_dir)


class GetData(object):
    
    def __init__(self):
        self.all_dfs_list=[]
        self.all_dfs_cm_3g = list()        
        self.df_pm = pd.DataFrame()
        self.df_fm = pd.DataFrame()
        self.df_pmfm = pd.DataFrame()
        
    def fetchDirFiles(self, file_dir,date):
        dirpath = os.path.join(file_dir, date)
        return os.listdir(dirpath)    

    
    def xml2df(self,cm_dirpath,cm_file):
        '''
        cm = cm_file.split('.')
        cm_1 = cm[0].split('_')
        rnc_name = cm_1[1]
        '''
        
        print(cm_dirpath)
        if cm_dirpath.endswith('.gz'):
          f = gzip.open(cm_dirpath,'rb')
          tree = ET.parse(f)
          root = tree.getroot()
          f.close()
        else:
        
          tree = ET.parse(cm_dirpath)
          root = tree.getroot()
        df_list = []
        
        for i, child in enumerate(root): 
                
                for subchildren in child.findall('{raml20.xsd}header'):
                    for subchild in subchildren:
                        dateTime = subchild.get('dateTime')
                        print(dateTime)
                        date_= str(dateTime).split('+')
                        dateTime=date_[0]
                
                for subchildren in child.findall('{raml20.xsd}managedObject'):
                    match_found = 0
                    xml_class_name = subchildren.get('class')
                    df_dict = OrderedDict()                   
                    if (str(xml_class_name) in self.param_dict.keys()):                       
                        for subchild in subchildren:                
                            header = subchild.attrib.get('name')
                            if header=='name' and (subchild.text in self.df_wcel_names):
                                ## Class and wcell name both matched
                                wcel_name = subchild.text
                                match_found = 1
                        if match_found:
                            
                            for subchild in subchildren:
                                header = subchild.attrib.get('name')
                                if header in self.param_dict[xml_class_name]:
                                    df_dict[header]=subchild.text
                                    df_dict['dateTime'] = dateTime
                                    df_dict['CLASS_name'] = xml_class_name
                                    df_dict['RNC_name'] = self.wcel_rnc_dict[wcel_name]
                                    df_dict['name'] = wcel_name
                                    df_dict['last_created']=datetime.date.today()
                            df_list.append(df_dict)
                                               
                
        self.df_cm = pd.DataFrame(df_list)
        
        #self.df_date=str(self.df_cm['dateTime']).split('+')
        #self.df_cm['dateTime']=self.df_date[0]
        #print(self.df_cm['dateTime'])
        return (self.df_cm)

    def readFiles(self,date,pm_files):
        cno_data = pd.read_excel(cno_file,sheetname='CM Parameter',skiprows=1,parse_cols='C:E')        
        #pm_data = pd.read_excel(pm_file,sheetname='Data',parse_cols='C,F')
        #print(self.df_pm.shape)
        print("pm_files[0]")
        print(pm_files[0])
        print("pm_files[1]")
        print(pm_files[1])
        df_1=pd.read_csv(pm_files[0],sep=';')
        print(df_1.shape)
        df_2=pd.read_csv(pm_files[1],sep=';')
        print(df_2.shape)
        self.df_pm=pd.merge(df_2,df_1)
        #self.df_pm['WCEL']=self.df_pm['WCEL'].map(lambda x: x.lstrip('W_U_P_'))
        #self.df_pm=pd.read_csv(pm_files)
        pm_data=self.df_pm
        
        cols = pm_data.columns
        cols = cols.map(lambda x: x.replace(' ', '_') )
        pm_data.columns = cols
        pm_df = pd.DataFrame(pm_data.dropna(subset=['WCEL_name','RNC_name']))
        pm_df  = pm_df.query('WCEL_name != ["0"] & RNC_name!=["0"]')
        wcel_names=pm_df['WCEL_name'].values
        self.df_wcel_names = np.unique(wcel_names)
        if (self.df_wcel_names.size == 0):
         return
        self.wcel_rnc_dict = {}
        for wcel in self.df_wcel_names:
            p = pm_df[ pm_df['WCEL_name']== wcel]
            rnc_name = np.unique(p)[1]
            self.wcel_rnc_dict[wcel] = rnc_name
        rnc_names = pm_df['RNC_name'].values
        self.df_rnc_names = np.unique(rnc_names)
        cno_data = cno_data[ cno_data['Technology']=='3G']
        arr_of_classes = cno_data.Group.unique()
        self.param_dict={}
        for _class in arr_of_classes:
            cno_data_grp = cno_data[ cno_data['Group']== _class]
            params = cno_data_grp.loc[cno_data_grp['Group'] == _class, 'Parameter Name']
            l_params=[]
            for i,par in enumerate(params):
                l_params.append(str(params.iloc[i]))
                self.param_dict[_class]=l_params
        print("-------------Parameter Dictionary-----------------")                               
        print(self.param_dict)
        #files = self.fetchDirFiles(cm_file_dir,date)
        files = os.listdir(cm_file_dir)
        
        list_of_dfs = []
        for cm_file in files:           
            if cm_file in ['output_plmn_7.xml.gz','output_plmn_1.xml.gz','output_plmn_2.xml.gz','output_plmn_3.xml.gz','output_plmn_4.xml.gz','output_plmn_5.xml.gz','output_plmn_6.xml.gz','output_plmn_8.xml.gz','output_plmn_9.xml.gz']:
                cm_dirpath = os.path.join(cm_file_dir,cm_file)
                print(cm_dirpath)
                df = self.xml2df(cm_dirpath,cm_file)
                
                
                #print(df)
                list_of_dfs.append(df)
        #df1=self.xml2df(output_plmn_path,cm_file)
        #print(list_of_dfs)   
        self.df_cm_final = pd.concat(list_of_dfs)
        print(list(self.df_cm_final))
        self.df_cm_final['name']=self.df_cm_final['name'].map(lambda x: x.lstrip('W_U_OO2_ZZ1_'))
        with open('cm_kenya_new.csv', 'a') as f:       
           self.df_cm_final.to_csv(f,header=True)
        

    def getPM(self):               
        self.date = date
        self.tech = tech.upper()
        print("-"*80)
        print("Processing data for date %s and technology %s "%(self.date, self.tech))
        print("-"*80)
        
        onlyfiles = [f for f in os.listdir(pm_dir) if isfile(join(pm_dir, f))]
        #print('files in pmdir:',onlyfiles)
        pmfilepaths=[]
        for files in onlyfiles:
          ggg=str(files).split('_')
          if 'Monitoring' in ggg:
            pm_file_names=files
            pm_file_path=pm_dir+pm_file_names
            pmfilepaths.append(pm_file_path)
          if 'CNO' in ggg:
            pm_file_names=files
            pm_file_path=pm_dir+pm_file_names
            pmfilepaths.append(pm_file_path)
        print(pmfilepaths)
   
        self.readFiles(date, pmfilepaths)
        
        if self.df_cm_final.size != 0:
            print("Accumulate all dates CM data...")
            self.all_dfs_cm_3g.append(self.df_cm_final)
                                                            
		        
             
    def readparameters(self):
        # Read KPIs
        df_di1 = pd.read_excel(cno_file, sheetname='Monitoring KPI', skiprows= 1, parse_cols='B,C,F')
        df_di1_3g = pd.DataFrame(df_di1[df_di1['Technology']=='3G'])     
        
        self.df_di1_3g = df_di1_3g
        
        #Read Main KPIs
        df_di2 = pd.read_excel(cno_file, sheetname='Main KPI', skiprows= 1, parse_cols='B,C')
        self.df_di2_3g = pd.DataFrame(df_di2[df_di2['Technology']=='3G'])
                
        """
        creates a dictionary 
          kpis = {kpi_influencer : [kpi_formula1, kpi_formula2,....]}
        """
        self.kpis_3g = defaultdict(list)
                
        for i in range(len(df_di1_3g.values)):
            self.kpis_3g[df_di1_3g.iloc[i].get('KPI influencer')].append(str(df_di1_3g.iloc[i].get('Formula').upper()))
                
        self.main_kpis_3g = self.df_di2_3g.get("Formula").values.tolist()
        self.main_kpis_3g = [str(i).upper() for i in self.main_kpis_3g]      

    def pushData(self, df, table_name):
          
        print(".............................INSERTING DATA IN DB.........................")
        spark_df1 = sqlContext.createDataFrame(df)
        """
        try:
            print(self.gettablelastid(table_name))
        except:
            pass
        
        spark_df = spark_df1.select("*").withColumn("uid", monotonically_increasing_id())
        spark_df = spark_df.select(spark_df.columns[-1:] + spark_df.columns[:-1])
        spark_df.write.format("org.apache.spark.sql.cassandra").options(table=table_name , keyspace=KEYSPACE).save(mode="append")
        """
        #spark_df.printSchema()
        #print(df.columns)
        ts = time.time()
        spark_df1.write.format("org.apache.spark.sql.cassandra").options(table=table_name , keyspace=KEYSPACE).save(mode="append")
        print("Data inserted in %s mins!"%((time.time() - ts)/60))
    
    def brkdfs(self, df, n):
        l = len(df.values)
        x = 0
        temp_li=[]
        for i in range(int(l/n)):
            partition = '[%s : %s]'%(n*i, n*(i+1))
            temp_li.append(partition)
            x = n*(i+1)
        temp_li.append('[%s:]'%x)
        print(temp_li)

        return temp_li
    def gettablecols(self, keyspace_name, table_name):
        df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(keyspace=keyspace_name, table=table_name).load()
        return df.columns
            
    def checkTable(self, columns_list, table_name, datafor ):
        
        ln = len(columns_list)
        """
        create_table_qry="CREATE TABLE IF NOT EXISTS %s.%s(uid bigint PRIMARY KEY, "%(KEYSPACE, table_name)
        for i , col in enumerate(columns_list):
            if(i<ln-1):
                if col in ['datetime', 'period start time']:
                    create_table_qry += col + " timestamp,"
                else:
                    create_table_qry += col + " varchar,"
                    
            elif(i==ln-1):
                if col in ['datetime', 'period start time']:
                    create_table_qry += col + " timestamp"
                else:
                    create_table_qry += col + " varchar"
        create_table_qry +=");"
        """
        #print(create_table_qry)
        #print("Creating table %s"%table_name)	
	
        create_table_qry="CREATE TABLE IF NOT EXISTS %s.%s("%(KEYSPACE, table_name)
        for i , col in enumerate(columns_list):
            if(i<ln-1):
                if col in ['datetime', 'period_start_time']:
                    create_table_qry += col + " timestamp,"
                else:
                    create_table_qry += col + " varchar,"
                    
            elif(i==ln-1):
                if col in ['datetime', 'period_start_time']:
                    create_table_qry += col + " timestamp"
                else:
                    create_table_qry += col + " varchar"
        if datafor == 'CM':
            if self.tech =='3G':
                create_table_qry +=", PRIMARY KEY ((datetime, name)));"
            else:
                create_table_qry +=", PRIMARY KEY ((datetime, lncel_name)));"
        if datafor == 'KPIPMFM':
            create_table_qry +=", PRIMARY KEY ((kpi_influencer, period_start_time, rnc_name, wbts_id, wcel_id)));"
        print(create_table_qry)
        
        session.execute(create_table_qry)	
        
        tabcols = self.gettablecols(KEYSPACE, table_name)
        # add columns to table in not in table
        newcols = set(columns_list) - set(tabcols)
        print(set(columns_list))
        print(set(tabcols))
        print(newcols)
        if newcols:
            print("New columns added in table %s!! "%table_name)
            for col in newcols:
                query = "ALTER TABLE %s.%s ADD %s varchar;"%(KEYSPACE, table_name ,col)
                session.execute(query)
               	        
    def changeDFcols(self, df):
        
        columns_list=[]
        
        # modify column names for cassandra tables
        for colname in df.columns.values:
            # remove symbols(special characters) from column name
            expr = r'[^\w ]'
            colname = re.sub(expr, '', str(colname).lower())
            # replace space with underscore
            colname = re.sub('[ -]', '_', colname)
            # column name cannot start with underscore or space
            colname.lstrip('_')
            colname.strip()
            m = re.search('[a-zA-Z]\w*', colname)
            if m:
                colname = m.group(0)
            columns_list.append(colname)
            
        df.columns = columns_list
        
        #sqlContext = SQLContext(sc)
        
        # convert some data to string
        for col_name in df.columns.values:
            if col_name not in['period start time', 'datetime']:
                df[col_name] = df[col_name].astype(str)
      
        return df
    
    def pushDatainDB(self):
        
        if self.all_dfs_cm_3g:
            CMDF = pd.concat(self.all_dfs_cm_3g)
            
            CMDF = self.changeDFcols(CMDF)
            
            # creates table if does not exist
            self.checkTable(CMDF.columns.values, CMTable, 'CM')
            print("Table Created/Modified...... !!!!!")
            
            print("Inserting CM data for all dates and technology %s in Database...."%(self.tech))
            self.pushData(CMDF, CMTable)

    def writeoutputtoexcel(self):
        writer = pd.ExcelWriter(output_file)
        res = pd.concat(self.all_dfs_list)
        print(res.shape)
        res.to_excel(writer, "PM-CM")
        writer.save()
        print("Output Saved for PM-CM")
        
    def writeoutputtocsv(self):
        all_df = pd.DataFrame(self.all_dfs_list)
        self.rnc_RHNS01 = all_df.query('RNC_name==["RHNS01"]')
        self.rnc_RKNL01 = all_df.query('RNC_name==["RKNL01"]')
        self.rnc_RKNL02 = all_df.query('RNC_name==["RKNL02"]')
        print("Writing PM-FM-CM data to file...")
        
        self.rnc_RHNS01.to_csv('/home/cnonoida/Scripts/anjali/output/pmfmcm_RHNS01%s%s.csv'%(self.tech,self.date ))
        self.rnc_RKNL01.to_csv('/home/cnonoida/Scripts/anjali/output/pmfmcm_RKNL01%s%s.csv'%(self.tech,self.date ))
        self.rnc_RKNL02.to_csv('/home/cnonoida/Scripts/anjali/output/pmfmcm_RKNL02%s%s.csv'%(self.tech,self.date ))
        print("Output saved!!!")
        


if __name__ == '__main__':
    start_time = time.time()
    getData = GetData()
    
    getData.readparameters()
    getData.getPM()
    getData.pushDatainDB()
    ##getData.writeoutputtocsv()
    print("Complete!!!")
    print("--- %s mins ---" % ((time.time() - start_time)/60))
        
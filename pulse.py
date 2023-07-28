import os
import json
import pandas as pd
import plotly.express as px
# import streamlit as st
from sqlalchemy import create_engine
import psycopg2 as ps

rootdir = '/pulse/data'
datadict={}
# paths={}
pathtab={}
# pathtabinner2={}
# pathsinner2={}
# pathsinner1={}
dfs = []
rootcount=0
for file in os.listdir(rootdir):
#     if file=='top':
#         break
    dir=os.path.join(rootdir,file)

    pathtabinner2={}
    for file1 in os.listdir(dir):
        paths={}
        
        d = os.path.join(dir, file1)
        if file=='map':
            d=d+'/hover/country/india'

        else:
            d=d+'/country/india'


        for file2 in os.listdir(d):
            pathsinner1={}
            d2=os.path.join(d,file2)
            if file2=='state':
                
                for file3 in os.listdir(d2):
                    pathsinner2={}

                    d3=os.path.join(d2,file3)
                    for file4 in os.listdir(d3):
                        d4=os.path.join(d3,file4)
                        if os.path.isdir(d4):
                            pathsinner2[file4]=d4
                            rootcount+=1
                    pathsinner1[file3]=pathsinner2
                paths[file2]=pathsinner1
                    
            
            elif os.path.isdir(d2):
                paths[file2]=d2
                rootcount+=1
        pathtabinner2[file1]=paths
    
    pathtab[file]=pathtabinner2
    
    
def aggregateduser(path):
    temp=pd.read_json(path)
    df=pd.DataFrame(temp['data'][1])
    return df
def aggregatedtransaction(path):
    temp=pd.read_json(path)
    df=pd.DataFrame(temp['data'][2])
    instruments=[]
    for i in range(len(df)):
        instruments.append(df['paymentInstruments'][i][0])
    df[['type','count','amount']]=pd.DataFrame(instruments)
    df.drop('paymentInstruments',axis=1,inplace=True)
    return df
def mapfn(path):
    temp=pd.read_json(path)
    df=pd.DataFrame(temp['data'][0])
    metrics=[]
    for  i in range(len(df)):
        metrics.append(df['metric'][i][0])
    df[['type','count','amount']]=pd.DataFrame(metrics)
    df.drop(['metric','type'],axis=1,inplace=True)
    return df
def mapstateless(path):
    temp=pd.read_json(path)
    users=[]
    appopen=[]
    for i in temp['data'][0].keys():
        users.append(temp['data'][0][i]['registeredUsers'])
        appopen.append(temp['data'][0][i]['appOpens'])

    df=pd.DataFrame({'names':temp['data'][0].keys(),'users':users,'appopens':appopen})
    return df
def mapstatelesstransaction(path):
    temp=pd.read_json(path)

    df=pd.DataFrame(temp['data'][0])
    metric=[]
    for i in range(len(df)):

        metric.append(df['metric'][i][0])
    df[['type','count','amount']]=pd.DataFrame(metric)
    df.drop(['metric'],axis=1,inplace=True)
    return df
def topfn(path):
    
    with open(path,'r' ) as f:
        temp=json.load(f)
    valdict={}
    for scope in temp['data'].keys():
        if temp['data'][scope] is None:
            continue
            
        values=[]
        metrics=[]
        for i in range(len(temp['data'][scope])):
            values.append(temp['data'][scope][i]['entityName'])
            metrics.append(temp['data'][scope][i]['metric'])
        
        df=pd.DataFrame({'name':values})
        df[['type','count','amount']]=pd.DataFrame(metrics)
        valdict[scope]=df
    return valdict

def topuser(path):
    with open(path,'r' ) as f:
        temp=json.load(f)
    valdict={}
    for scope in temp['data'].keys():
        valdict[scope]=pd.DataFrame(temp['data'][scope])
    return valdict


for section in pathtab.keys():
    for subsection in pathtab[section].keys():

        for folder in pathtab[section][subsection].keys():
            if folder=='state':

                for state in pathtab[section][subsection]['state'].keys():
                    for year in pathtab[section][subsection][folder][state].keys():
                        dfs=[]
                        for file in os.listdir(pathtab[section][subsection][folder][state][year]):
                            path=os.path.join(pathtab[section][subsection][folder][state][year],file)
                            if section=='aggregated':
                                if subsection=='transaction':
                                    df=aggregatedtransaction(path)
                                elif subsection=='user':
                                    df=aggregateduser(path)
                            elif section=='map':
                                if subsection=='user':
                                    df=mapstateless(path)
                                elif subsection=='transaction':
                                    df=mapstatelesstransaction(path)
                            elif section=='top':
                                if subsection=='transaction':
                                    df=topfn(path)
                                elif subsection=='user':
                                    df=topuser(path)
                                    
                                    
                            
                            dfs.append(df)
                            key=section[0]+subsection[0]+folder+state+year+'Q'+file
                            if section =='top':
                                for i in df.keys():
                                    cust_key=key+i
                                    datadict[cust_key]=df[i]
                            else:

                                datadict[key]=df
                            

                            

                        if section!='top':
                            key=section[0]+subsection[0]+folder+state+year
                            datadict[key]=pd.concat(dfs)
            else:
                dfs=[]
                for file in os.listdir(pathtab[section][subsection][folder]):
                    path=os.path.join(pathtab[section][subsection][folder],file)
#                     print(path)
                    if section=='aggregated':
                        if subsection=='transaction':
                            df=aggregatedtransaction(path)
                        elif subsection=='user':
                            df=aggregateduser(path)
                    elif section=='map':
                        if subsection=='user':
                            df=mapstateless(path)
                        elif subsection=='transaction':
                            df=mapstatelesstransaction(path)
                      

                    elif section=='top':
                        if subsection=='transaction':
                            
                            df=topfn(path)
                        elif subsection=='user':
                            df=topuser(path)
                    dfs.append(df)
                    key=section[0]+subsection[0]+folder+'Q'+file



                    if section=='top':
                        for i in df.keys():
                            cust_key=key+i
                            datadict[cust_key]=df[i]
                    else:

                        datadict[key]=df


                if section!='top':
                    key=section[0]+subsection[0]+folder
                
                    datadict[key]=pd.concat(dfs)
                

def plot(section,subsection,folder,state='',year='',quarter='3',scope='districts'):

    if folder!='state':

        key=section+subsection+folder+'Q'+quarter+'.json'
    else:
        key=section+subsection+folder+state+year+'Q'+quarter+'.json'
    if section=='top':

        df=datadict[key][scope]
    else:
        df=datadict[key]
    return df
conn=ps.connect('dbname=pulse user=postgres password=rskjjnjkk')
engine=create_engine(

    url="postgresql://postgres:rskjjnjkk@localhost/pulse"
)
for key in datadict.keys():
    df=datadict[key]
    print(key)
    if isinstance(df,pd.DataFrame):        

        df.to_sql(key,engine,if_exists='replace')
    else:
        print(df)


#     codes=pd.read_csv('poptable.csv')
#     codes['name']=codes['State.Name'].apply(lambda x: x.lower)

#     lat,long=[],[]
#     for name in df['names']:

#         try:
#             name=name[0:-9]
#             indx=codes['name'].str.contains(name)
#         except:
#             print(name)
#             continue
#         ind=None
#         for i in range(len(indx)):
#             if indx[i]==True:
#                 ind=i
#                 break
#         if ind is not None:
#             lat.append(codes.at[ind,'latitude'])
#             long.append(codes.at[ind,'longitude'])
#         else:
#             lat.append(0)
#             long.append(0)
#     df['lat']=lat
#     df['long']=long
#     import plotly.express as px

#     px.set_mapbox_access_token('pk.eyJ1Ijoiam9zam9lIiwiYSI6ImNsamp3aG9mdDBkeXEzY3BkcndiZnR1aGcifQ.DKSMByvpW2cuoLRNEHgktQ')
#     fig = px.scatter_mapbox(df,
#                             lat='lat',
#                             lon='long',
#                             hover_name="names",
#     #                         color='',
#                             zoom=1)
#     fig.show()

# plot('map','user','2018')
# class pulse:
#     data=datadict
# # import streamlit as st
# # from pulse.pulse import data 
# section=st.selectbox('section',datadict.keys())
# # subsection=st.selectbox('subsection',datadict[section].keys())
# st.dataframe(datadict[section])
# print(len(datadict))

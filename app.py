import streamlit as st
import pandas as pd
from sqlalchemy import create_engine,text
import plotly.express as px
import requests
import seaborn as sns

key=None

section=st.selectbox('section',['map','top','agggregate'])
subsection=st.selectbox('subsection',['user','transaction'])
folder=st.selectbox('folder',['2018','2019','2020','2021','2022','state'])
if folder=='state':
    state=st.selectbox('',['andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh', 'assam', 'bihar', 'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'])
    year=st.selectbox('year',['2018','2019','2020','2021','2022'])
    year='state'+state+year
else:
    year=folder
quarter=st.selectbox('quarter',['1','2','3','4'])
if section=='top':
    if folder!='state':
        area=st.selectbox('area',['states','districts','pincodes'])
    else:
        area=st.selectbox('area',['districts','pincodes'])
    key=section[0]+subsection[0]+year+'Q'+quarter+'.json'+area
else:
    key=section[0]+subsection[0]+year+'Q'+quarter+'.json'
if key is not None:


    engine=create_engine(

        url="postgresql://postgres:rskjjnjkk@localhost/pulse"
    )
    
    df=pd.read_sql(key,engine)
st.dataframe(df )
if section[0] =='a':
    if subsection[0]=='t':
        fig1=px.bar(df,x='name',y='count')
        fig2=px.bar(df,x='name',y='amount')
        st.plotly_chart(fig1, use_container_width=True)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        fig1=px.bar(df,x='brand',y='count')
        fig2=px.bar(df,x='brand',y='percentage')
        st.plotly_chart(fig1, use_container_width=True)
        st.plotly_chart(fig2, use_container_width=True)
elif section[0]=='t':
        
    if subsection[0]=='u':
        fig1=px.bar(df,x='name',y='registeredUsers')
        st.plotly_chart(fig1, use_container_width=True)
    else:
        fig1=px.bar(df,x='name',y='count')
        fig2=px.bar(df,x='name',y='amount')
        st.plotly_chart(fig1, use_container_width=True)
        st.plotly_chart(fig2, use_container_width=True)
elif section[0]=='m':
    # table=pd.DataFrame()
    # table=pd.read_sql('codes',engine)
    lat=[]
    lon=[]
    if subsection=='t':
        k='name'
    else:
        k='names'
    for name in df[k]:
        if name[-8:]=='district':
            name=name[:-8]

        
        with engine.connect() as connection:
            result = connection.execute(text(f"select lat,lon from codes where codes.name='{name}'"))
        query=result.fetchall()
        if len(query)>0:
            if len(query[0])>1:
                lat.append(query[0][0])
                lon.append(query[0][1])
        else:
            url=f'http://api.openweathermap.org/geo/1.0/direct?q={name},,IN&limit=1&appid=9fc819c4ec9de0f0cf0e61d31f20cdb8'
            response=requests.get(url)
            data=response.json()
            if len(data)>0:
                lat.append(data[0]['lat'])
                lon.append(data[0]['lon'])
            else:
                lat.append(0)
                lon.append(0)
    df['lat']=lat
    df['lon']=lon
    # if subsection[0]=='u'
    st.map(df)
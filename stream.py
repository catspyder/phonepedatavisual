import streamlit as st
import pandas as pd
import mysql.connector
import mysql.connector
from mysql.connector.constants import ClientFlag
# creating the db
config = {
    'user': 'root',
    'password': 'rskjjnjkk',
    'host': 'localhost',
#     'client_flags': [ClientFlag.SSL],
#     'ssl_ca': 'ssl/server-ca.pem',
#     'ssl_cert': 'ssl/client-cert.pem',
#     'ssl_key': 'ssl/client-key.pem'
}

cnxn = mysql.connector.connect(**config)
cursor = cnxn.cursor()  # initialize connection cursor
cursor.execute('CREATE DATABASE phonepepulse')  # create a new 'testdb' database
cnxn.close()  
# district wise
import os
rootdir = 'pulse/data/map/transaction/hover/country/india'
paths=[]
for file in os.listdir(rootdir):
    
    d = os.path.join(rootdir, file)
    for file2 in os.listdir(d):
        d2=os.path.join(d,file2)
        
        if os.path.isdir(d2):
            paths.append(d2)
import os
import json
import pandas as pd

dfs = []

# Loop through all years  folders
for  path1 in paths:
    state=path1.split('\\')[-1] 
    dates=['2018','2019','2020','2021','2022']
    for date in dates:
        path = path1+ '/'+date
        for filename in os.listdir(path) :

            # Check if the file is a JSON file
            if filename.endswith('.json'):
                # Load the JSON data
                with open(os.path.join(path, filename)) as f:
                    data = json.load(f)
                # Normalize the data and create a dataframe
                df = pd.json_normalize(data['data']['hoverDataList'])
                df['Year'] = date
                df['state']=state
                # Append the dataframe to the list
                dfs.append(df)

# Concatenate all dataframes in the list into a single dataframe
result = pd.concat(dfs, ignore_index=True)
# state_wise=dfs
# state_wise[0]
state_wise=pd.DataFrame(result)
#normalising the metrics column
n=len(state_wise)
ind=[a for a in range(n)]
metric=[]
for i in state_wise['metric']:
    metric.append(i[0])
state_wise[['type','count', 'amount']]=pd.DataFrame(metric,index=ind)
new_state_wise=state_wise.drop(['metric','type'],axis=1)
new_state_wise['name']=new_state_wise['name'].str.slice(stop=-9)

#storing to sql
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://root:rskjjnjkk@localhost/phonepepulse')

new_state_wise.to_sql(name='new_district_wise', con=engine, if_exists='replace', index=True)
# new_state_wise['name']=new_state_wise['name'].str.slice(stop=-9)


state_wise_summed = new_state_wise.groupby(['state','Year']).agg({'count':'sum','amount':'sum'}).reset_index()
# storing data
#storing to sql
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://root:rskjjnjkk@localhost/phonepepulse')

state_wise_summed.to_sql(name='state_wise_summed', con=engine, if_exists='replace', index=True)

# district wise_aggregated
import os
rootdir = 'pulse/data/aggregated/transaction/country/india'
paths=[]
for file in os.listdir(rootdir):
    
    d = os.path.join(rootdir, file)
    for file2 in os.listdir(d):
        d2=os.path.join(d,file2)
        
        if os.path.isdir(d2):
            paths.append(d2)
import os
import json
import pandas as pd

dfs = []

# Loop through all years  folders
for  path1 in paths:
    state=path1.split('\\')[-1] 
    dates=['2018','2019','2020','2021','2022']
    for date in dates:
        path = path1+ '/'+date
        for filename in os.listdir(path) :

            # Check if the file is a JSON file
            if filename.endswith('.json'):
                # Load the JSON data
                with open(os.path.join(path, filename)) as f:
                    data = json.load(f)
                # Normalize the data and create a dataframe
                df = pd.json_normalize(data['data']['transactionData'])
                df['Year'] = date
                df['state']=state
                # Append the dataframe to the list
                dfs.append(df)

# Concatenate all dataframes in the list into a single dataframe
result = pd.concat(dfs, ignore_index=True)
# state_wise=dfs
# state_wise[0]
state_wise_aggregated=pd.DataFrame(result)
#normalising the metrics column
n=len(state_wise_aggregated)
ind=[a for a in range(n)]
metric_aggregated=[]
for i in state_wise_aggregated['paymentInstruments']:
    metric_aggregated.append(i[0])
state_wise_aggregated[['type','count', 'amount']]=pd.DataFrame(metric_aggregated,index=ind)
new_state_wise_aggregated=state_wise_aggregated.drop(['paymentInstruments','type'],axis=1)
#storing data

from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://root:rskjjnjkk@localhost/phonepepulse')

new_state_wise.to_sql(name='aggregated_state_wise', con=engine, if_exists='replace', index=True)
# district wise summed
state_wise_summed = new_state_wise.groupby(['name','Year']).agg({'count':'sum','amount':'sum'}).reset_index()
# storing data
#storing to sql
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://root:rskjjnjkk@localhost/phonepepulse')

state_wise_summed.to_sql(name='district_wise_summed', con=engine, if_exists='replace', index=True)
# district wise_aggregated
import os
rootdir = 'pulse/data/top/transaction/country/india'
paths=[]
for file in os.listdir(rootdir):
    
    d = os.path.join(rootdir, file)
    for file2 in os.listdir(d):
        d2=os.path.join(d,file2)
        
        if os.path.isdir(d2):
            paths.append(d2)
import os
import json
import pandas as pd

dfs = []

# Loop through all years  folders
for  path1 in paths:
    state=path1.split('\\')[-1] 
    dates=['2018','2019','2020','2021','2022']
    for date in dates:
        path = path1+ '/'+date
        for filename in os.listdir(path) :

            # Check if the file is a JSON file
            if filename.endswith('.json'):
                # Load the JSON data
                with open(os.path.join(path, filename)) as f:
                    data = json.load(f)
                # Normalize the data and create a dataframe
                df = pd.json_normalize(data['data']['districts'])
                df['Year'] = date
                df['state']=state
                # Append the dataframe to the list
                dfs.append(df)

# Concatenate all dataframes in the list into a single dataframe
result = pd.concat(dfs, ignore_index=True)
# state_wise=dfs
# state_wise[0]
state_wise_top=pd.DataFrame(result)
# storing the data
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://root:rskjjnjkk@localhost/phonepepulse')

new_state_wise.to_sql(name='top_state_wise', con=engine, if_exists='replace', index=True)

# finding and storing location data for districts
import opencage.geocoder
import pandas as pd
import mysql.connector

cnx = mysql.connector.connect(user='root',
                              password='rskjjnjkk',
                              host='localhost',
                              database='phonepepulse')

query = 'SELECT * FROM new_state_wise'

# Read query results into DataFrame
district_wise = pd.read_sql(query, cnx)
cnx.close()



# Set up OpenCage API credentials
api_key = '9c0b66db95184c068f6e4b8917263981'
geocoder = opencage.geocoder.OpenCageGeocode(api_key)
districts=list(district_wise['name'].unique())

# Define list of city names
cities = districts

# Initialize lists to store coordinates
latitudes = []
longitudes = []

# Loop through each city name and geocode it
def find_cord(city):
    result = geocoder.geocode(city)
    if result:
        # Extract latitude and longitude coordinates from geocoding result
        lat = result[0]['geometry']['lat']
        lng = result[0]['geometry']['lng']
        return lat,lng
    else:
        # Handle cases where geocoding result is not found
        return None,None
new_row = [find_cord(name) for name in districts]


ind=[]
for i in range(len(new_row)):
    ind.append(i)
district_codes= pd.DataFrame(columns=['latitude','longitude'],data=new_row,index=ind)
district_codes['name']= pd.Series(districts)
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://root:rskjjnjkk@localhost/phonepepulse')

district_codes.to_sql(name='district_codes', con=engine, if_exists='replace', index=True)
# finding and storing location data for states
import opencage.geocoder
import pandas as pd
import mysql.connector

cnx = mysql.connector.connect(user='root',
                              password='rskjjnjkk',
                              host='localhost',
                              database='phonepepulse')

query = 'SELECT * FROM new_state_wise'

# Read query results into DataFrame
district_wise = pd.read_sql(query, cnx)
cnx.close()



# Set up OpenCage API credentials
api_key = '9c0b66db95184c068f6e4b8917263981'
geocoder = opencage.geocoder.OpenCageGeocode(api_key)
states=list(district_wise['state'].unique())


# Initialize lists to store coordinates
latitudes = []
longitudes = []

# Loop through each city name and geocode it
def find_cord(city):
    result = geocoder.geocode(city)
    if result:
        # Extract latitude and longitude coordinates from geocoding result
        lat = result[0]['geometry']['lat']
        lng = result[0]['geometry']['lng']
        return lat,lng
    else:
        # Handle cases where geocoding result is not found
        return None,None
new_row = [find_cord(name) for name in states]


ind=[]
for i in range(len(new_row)):
    ind.append(i)
state_codes= pd.DataFrame(columns=['latitude','longitude'],data=new_row,index=ind)
state_codes['state']= pd.Series(states)
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://root:rskjjnjkk@localhost/phonepepulse')

state_codes.to_sql(name='state_codes', con=engine, if_exists='replace', index=True)



cnx=mysql.connector.connect(user='root',
                            password='rskjjnjkk',
                            host='localhost',
                            database='phonepepulse')
query='select * from district_codes'
district_codes = pd.read_sql(query, cnx)
district_codes.rename(columns = {'District':'name'}, inplace = True)

query='select *from state_codes'
state_codes=pd.read_sql(query,cnx)
query='select *from district_wise_summed'
district_wise=pd.read_sql(query,cnx)
query='select *from state_wise_summed'
state_wise =pd.read_sql(query,cnx)

cnx.close()
def create_map(daf,data):
    fig = go.Figure(go.Scattergeo(
    lon = df['longitude'],
    lat = df['latitude'],
    mode = 'markers',
    marker = dict(
        size = 10,
        color = df['data'],
        colorscale = 'Viridis',
        colorbar = dict(title = 'Data')
    ),
    text = df['data']
))

# Add text annotations to the map at each location
annotations = []
for i in range(len(df)):
    annotations.append(dict(
        x = df['longitude'][i],
        y = df['latitude'][i],
        text = df[data][i],
        showarrow = False
    ))
fig.update_layout(annotations=annotations)

# Set the layout of the map and display it
fig.update_layout(
    geo = dict(
        scope = 'world',
        projection_type = 'equirectangular',
        showland = True,
        landcolor = 'rgb(243, 243, 243)',
        countrycolor = 'rgb(204, 204, 204)',
        coastlinecolor = 'rgb(204, 204, 204)',
        lataxis = dict(range=[-90,90]),
        lonaxis = dict(range=[-180,180])
    )
)

fig.show()
def show_map(year,sord,data):
    if sord=='state':
        y=str(year)
        state_wise_new=state_wise[state_wise['Year']==y]
        state_data_codes=pd.merge(state_wise_new,state_codes,on='state')
        create_map(street_data_codes,data)
    elif sord=='district':
        y=str(year)
        district_wise_new=district_wise[district_wise['Year']==y]
        district_data_codes=pd.merge(district_wise_new,district_codes,on='state')
        create_map(district_data_codes,data)
        

#streamlit app 
year= st.selectbox('Year',(2019,2019,2020,2021,2022))
staordist=st.selectbox('detail',('state','district'))
data=st.selectbox('which',('count','amount'))
st.button('GO',on_click=show_map(year,staordist))
district_codes=district_codes.dropna(axis=0)
st.map(district_codes,zoom=5)


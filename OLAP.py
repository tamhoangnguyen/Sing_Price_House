
from google.cloud import bigquery
from google.auth import default
from random import randint
from datetime import datetime
import pandas as pd
import re
import datetime
import time
import os   


# Transform step
def convert_floorRange(x):
        month = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        if x == '-':
            floorRange_col = 0
        else:
            floorRange = str(x).strip()
            check  = floorRange.split('-')
            if check[0] in month:
                convert = datetime.datetime.strptime(floorRange,"%b-%d")
                floorRange_col = convert.strftime("%d - %m")
            elif check[1] in month:
                convert = datetime.datetime.strptime(floorRange,"%d-%b")
                floorRange_col = convert.strftime("%d - %m")
            else:
                a = re.sub('[a-zA-Z]','',check[0])
                b = re.sub('[a-zA-Z]','',check[1])
                floorRange_col = a + '-' + b
        return floorRange_col
def min_floor(x):
    if x == 0:
        min_floor = 0
    else:
        value = x.split('-')
        if int(value[0]) < int(value[1]):
            min_floor = value[0].strip()
        else: min_floor = value[1].strip()
    return int(min_floor)
def max_floor(x):
    if x == 0:
        max_floor = 0
    else:
        value = x.split('-')
        if int(value[0]) > int(value[1]):
            max_floor = value[0].strip()
        else: max_floor = value[1].strip()
    return int(max_floor)
def convert_tenure(x):
    if x == 'Freehold':
        return 0
    else: return 1
def convert_quarter(x):
    if x >= 10: return 'Q4'
    elif x >= 7: return 'Q3'
    elif x >= 4: return 'Q2'
    else: return 'Q1'
def type_tenure(x):
    if x == 1: return 'Freehold'
    else: return 'Leasehold'
def type_marketSegment(x):
    if x == 'RCR' : return 'Rest of Central Region'
    elif x == 'OCR': return 'Outside Central Region'
    elif x == 'CCR': return 'Core Central Region'
def transform(df):
    df['floorRange'] = df['floorRange'].apply(convert_floorRange)
    #df['contractDate'] = df['contractDate'].apply(lambda x: pd.to_datetime(x,format = '%m/%d/%Y'))


    price_dim = pd.DataFrame(df[['property_key','contractDate','price']])

    datetime_dim = pd.DataFrame(df['contractDate']).drop_duplicates().sort_values('contractDate',ascending = False).reset_index(drop = True)
    datetime_dim['contractDate'] = pd.to_datetime(datetime_dim['contractDate'],format = '%d/%m/%Y')
    datetime_dim['day'] = datetime_dim['contractDate'].dt.day
    datetime_dim['month'] = datetime_dim['contractDate'].dt.month
    datetime_dim['year'] = datetime_dim['contractDate'].dt.year
    datetime_dim['quarter'] = datetime_dim['year'].apply(lambda x: (str(x))) + '-' + datetime_dim['month'].apply(convert_quarter)


    properties_dim = df[['property_key','contractDate','area', 'floorRange',
       'propertyType', 'district', 'typeOfArea', 'tenure', 'project',
       'marketSegment']].drop_duplicates().reset_index(drop = True)
    properties_dim['type_of_tenure'] = properties_dim['tenure'].apply(convert_tenure)
    properties_dim['min_floor'] = properties_dim['floorRange'].apply(min_floor)
    properties_dim['max_floor'] = properties_dim['floorRange'].apply(max_floor)
    properties_dim['avg_floor'] = ((properties_dim['min_floor'] + properties_dim['max_floor']) / 2)
    properties_dim['note_tenure'] = properties_dim['tenure'].apply(type_tenure)
    properties_dim['name_of_marketSegment'] = properties_dim['marketSegment'].apply(type_marketSegment)
 

    deposits_dim = df[['contractDate','Deposits_3_years','Deposits_6_months','Deposits_12_Months','Savings_Deposits']]
    deposits_dim['contractDate'] = pd.to_datetime(deposits_dim['contractDate'],format = '%d/%m/%Y')
    deposits_dim['year'] = deposits_dim['contractDate'].dt.year
    deposits_dim['month'] = deposits_dim['contractDate'].dt.month
    deposits_dim = deposits_dim.groupby(['year','month']).agg({'Deposits_3_years': 'max','Deposits_6_months': 'max','Deposits_12_Months': 'max','Savings_Deposits': 'max'})
    deposits_dim.reset_index(inplace = True)

    location_dim = df[['district','lat','lng','num_schools_1km','num_supermarkets_500m','num_mrt_stations_500m']].drop_duplicates().sort_values('district')
    location_dim['district'] = location_dim['district'].apply(lambda x: int(x))
    location_dim.reset_index(inplace = True)
    location_dim = location_dim[['district','lat','lng','num_schools_1km','num_supermarkets_500m','num_mrt_stations_500m']]
    location_dim = location_dim.astype(str)
    
    vacant_dim = df[['contractDate','Available','Vacant']].drop_duplicates()
    vacant_dim['contractDate'] = pd.to_datetime(vacant_dim['contractDate'],format = '%d/%m/%Y')
    vacant_dim['month'] = vacant_dim['contractDate'].dt.month
    vacant_dim['year'] = vacant_dim['contractDate'].dt.year
    vacant_dim['quarter'] = vacant_dim['year'].apply(lambda x: (str(x))) + '-' + vacant_dim['month'].apply(convert_quarter)
    vacant_dim = vacant_dim[['quarter','Available','Vacant']].drop_duplicates().sort_values('quarter',ascending = False).reset_index(drop = True)
    
    cpi_dim = df[['contractDate','CPI']].drop_duplicates()
    cpi_dim['contractDate'] = pd.to_datetime(cpi_dim['contractDate'],format = '%d/%m/%Y')
    cpi_dim['month'] = cpi_dim['contractDate'].dt.month
    cpi_dim['year'] = cpi_dim['contractDate'].dt.year
    cpi_dim = cpi_dim[['year','month','CPI']].drop_duplicates().sort_values(['year','month'],ascending = False).reset_index(drop = True)

    rentIndex_dim = df[['contractDate','typeOfArea','marketSegment','index']]
    rentIndex_dim['contractDate'] = pd.to_datetime(rentIndex_dim['contractDate'],format = '%d/%m/%Y')
    rentIndex_dim['month'] = rentIndex_dim['contractDate'].dt.month
    rentIndex_dim['year'] = rentIndex_dim['contractDate'].dt.year
    rentIndex_dim['quarter'] = rentIndex_dim['year'].apply(lambda x: (str(x))) + '-' + rentIndex_dim['month'].apply(convert_quarter)
    rentIndex_dim = rentIndex_dim[['quarter','typeOfArea','marketSegment','index']]
    rentIndex_dim = rentIndex_dim.drop_duplicates().sort_values('quarter',ascending = False).reset_index(drop = True)
    for i in range(len(rentIndex_dim)):
        x  = rentIndex_dim['typeOfArea'][i].strip()
        if x == 'Land':
            rentIndex_dim['marketSegment'][i] = 'Whole Island'
        else:
            rentIndex_dim['typeOfArea'][i] = 'Non-Landed'
    rentIndex_dim = rentIndex_dim.drop_duplicates().reset_index(drop = True)
    
    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
            "properties_dim":properties_dim.to_dict(orient = "dict"),
            "deposits_dim":deposits_dim.to_dict(orient = "dict"),
            "location_dim":location_dim.to_dict(orient = "dict"),
            "vacant_dim":vacant_dim.to_dict(orient = "dict"),
            "cpi_dim":cpi_dim.to_dict(orient = "dict"),
            "rentIndex_dim":rentIndex_dim.to_dict(orient = "dict"),
            "fact_table":df.to_dict(orient = "dict"),
            'price_dim': price_dim.to_dict(orient="dict")}

if __name__ == '__main__':
    #Extract
    df = pd.read_csv("D:\\Sing Price House Predict\\data\\full_data.csv")

    #Transform
    data = transform(df)

    #Load into Bigquery
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\\Sing Price House Predict\\api.json" # Access API in GCP
    credentials, project_id = default()
    client = bigquery.Client(credentials=credentials,project=project_id)
    my_project = 'my-project-23-02-01'
    my_dataset = 'de_project'
    for key,value in data.items():
        table_id = "{}.{}.{}".format(my_project,my_dataset,key)
        my_df = pd.DataFrame(value)
        my_df.to_csv('D:\\Sing Price House Predict\data_2\{}.csv'.format(key),index = False)
        my_df = pd.read_csv("D:\\Sing Price House Predict\\data_2\\{}.csv".format(key))
        if 'contractDate' in my_df.columns:
            try: 
                my_df['contractDate'] = pd.to_datetime(my_df['contractDate'],format='%Y-%m-%d')
            except:
                my_df['contractDate'] = pd.to_datetime(my_df['contractDate'],format='%d/%m/%Y')
        table_id = "{}.{}.{}".format(my_project,my_dataset,key)
        my_df.to_gbq(
                    destination_table=table_id,
                    project_id= project_id,
                    if_exists="replace",
                )
        print(f"Table data {key} is upload Bigquery success !")
        time.sleep(randint(2,5))
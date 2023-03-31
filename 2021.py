import requests
import pandas as pd


# #https://www12.statcan.gc.ca/wds-sdw/2021profile-profil2021-eng.cfm
# # https://ws-entry-point/resource/flowRef/key/providerRef?queryStringParameters
# #The agencyID code to represent the Census Profile for Statistics Canada is STC_CP.
# #https://towardsdatascience.com/how-to-collect-data-from-statistics-canada-using-python-db8a81ce6475
# url = 'https://api.statcan.gc.ca/census-recensement/profile/sdmx/rest/data/STC_CP/all/latest'
# response = requests.get(url)
# response = response.json()
# values = list(response.values())[1]


#ionstructions: https://www12.statcan.gc.ca/wds-sdw/2021profile-profil2021-eng.cfm
# api.statcan.gc.ca/census-recensement/profile/sdmx/rest/resource/flowRef/key?parameters
#example: https://api.statcan.gc.ca/census-recensement/profile/sdmx/rest/data/STC_CP,DF_CMACA/A5.2021S0504501...
#df_dauid = pd.read_csv('98-401-X2021025_English_CSV_data.csv',encoding='cp1252',dtype=str)
df_dauid = pd.read_csv('98-401-X2021006_English_CSV_data_BritishColumbia.csv',encoding='cp1252',dtype=str)
head = df_dauid.head()

#the column with all DAUIDs is DGUID
#convert the weird series thing it did to list https://note.nkmk.me/en/python-pandas-list/

#get the list of DAUids you want
#use this page to build DGuid
df_dguid_wanted = pd.read_csv('sample_pccf_Data_dauid.csv',dtype=object)

#the column with dguids wanted is called DAuid
#dguid_list = df_dguid_wanted["DGUID"].tolist()


#test = df_dauid[df_dauid.ALT_GEO_CODE.isin(df_dguid_wanted.DAuid)]
test = df_dauid.merge(df_dguid_wanted,left_on='ALT_GEO_CODE',right_on='DAuid')

#df_final = df_dauid[df_dauid['DGUID'].isin(dguid_list)]
test.to_csv('alldata.csv')




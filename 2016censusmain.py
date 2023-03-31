import requests
import pandas as pd
import json
import time

#build list of the dauid you want
df_dauid = pd.read('sample_pccf_Data.csv')
DAuid_List = df_dauid ('Dauid').tolist()

#allbcdata?
df_alldata = pd.read('98-401-X2021025_English_CSV_data.csv')
print(df_alldata.head())

# creates a list (geo_uid) of all the Census Tracts in BC
# info about this URL available at https://www12.statcan.gc.ca/wds-sdw/cr2016geo-eng.cfm
# this worked when geos was DA, which was useless
# could also try FSA instead of CT - although this may not be detailed enoguh
response = requests.get("https://www12.statcan.gc.ca/rest/census-recensement/CR2021Geo.json?&geos=DA&cpt=59")
response = response.json()
values = list(response.values())[1]



# use this when pulling national data: all_geouids = list()

# this builds the list of all GEO UID - pulls whatever type of ID you called in the response variable (geos=x)
bc_geo_uids = []
for i in values:
    test = i[0]
    bc_geo_uids.append(i[0])

    # because the url we're using has cpt=59, all the CT data is for BC already, but this loop can check that or be used if we pull national data
    # for x in all_geouids[1]:
    #     if x[1] == '59':
    #         bc_geo_uids.append(x[0])
    # next - use the geo_uid data to generate a list of all the demographic data
    # use this format https://www12.statcan.gc.ca/rest/census-recensement/CPR2016.json?lang=E&dguid=2016S051259010123&topic=7&notes=0&stat=1
    # stat may have to equal 0?
    # loop through each uid, and for each uid loop through each of the 14 demographic profiles
    # parse the json you get back from the URLs to see if there's anything consistent you don't have to append to the dataframe
    # end up with a dataframe where each row is the uid and the columns keep getting appended from the URL

# now that we have the Census Tracts for BC, we use the Stats Can URL to get Topic Data
# instructions: https://www12.statcan.gc.ca/wds-sdw/cpr2016-eng.cfm
# base URL https://www12.statcan.gc.ca/rest/census-recensement/CPR2016{.type}?lang={lang}&dguid={dguid}&topic={topic}&notes={notes}&stat={stat}

# This example can help you build a list cycling through x data types, ideally you can just pull data type 0, which gets everything
# there are 766 Census Tracts in bc_geo_iuds
# there are 14 topics to cycle through
# build column names before getting data

# for x in range(1, 14):
#     url = f"https://www12.statcan.gc.ca/rest/census-recensement/CPR2016.json?lang=E&dguid={i}&topic={x}&notes=0&stat=0"
#     response2 = requests.get(url)
#     dict1 = response2.json()
#     values = list(dict1.values())
#     column_names.extend(values[0])
# # print(column_names)
# df_test_3 = pd.DataFrame(columns=column_names)
# df_test_3['bc_uid'] = bc_geo_uids
# print(df_test_3.head())
# for i in bc_geo_uids:
#     data = []
#     for x in range(1, 14):
#         url = f"https://www12.statcan.gc.ca/rest/census-recensement/CPR2016.json?lang=E&dguid={i}&topic={x}&notes=0&stat=1"
#         response2 = requests.get(url)
#         dict1 = response2.json()
#         values = list(dict1.values())
#         for datalist in values[1]:
#             for datapoint in datalist:
#                 data.extend(datapoint)

# pull the data from Stats Can, into a dataframe, save it as csv
# cycle through each uid
all_census_data_dict = {}
column_names = []
for i in bc_geo_uids:
    url = f"https://www12.statcan.gc.ca/rest/census-recensement/CPR2016.json?lang=E&dguid={i}&topic={0}&notes=0&stat=0"
    response2 = requests.get(url)
    response2 = response2.json()
    data = list(response2.values())[1]
    for f in data:
        #deletes columns 'PROV_TERR_ID', 'PROV_TERR_NAME_NOM', 'GEO_UID', 'GEO_ID', 'GEO_NAME_NOM', 'GEO_TYPE',
        del f[0:5]
    all_census_data_dict[i] = data
    #all_census_data_list.extend(data)

df3 = pd.DataFrame.from_dict(all_census_data_dict)

#


# df_test = pd.DataFrame(i,index=bc_geo_uids,columns=[column_names])
# df_test_2 = df_test["GEO_UID"]
# df1["BC_GEO_UID"] = bc_geo_uids
# df1.columns = column_names
# row1 = df_test.iloc[0]
# print(row1)
# print(df_test.size)


# a = 0
# for i in bc_geo_uids:
#     column_names = list()
#     data = []
#     unique_id = 0
#     labels = list()
#     for x in range(1, 14):
#         url = f"https://www12.statcan.gc.ca/rest/census-recensement/CPR2016.json?lang=E&dguid={i}&topic={x}&notes=0&stat=1"
#         response2 = requests.get(url)
#         dict1 = response2.json()
#         values = list(dict1.values())
#         if a == 0:
#             labels = values[0]
#             column_names = column_names.extend(labels)
#         else:
#             for value in values:
#                 # value is now each demographic data pulled in on this topic
#                 unique_id = value[2]
#                 # df_fail = pd.DataFrame.from_dict(dict1)
#     a = a + 1

# df2 = pd.DataFrame(list(dict1.items()),columns=)

# df_temp = pd.DataFrame(values2[0])
# list2 = list()
# print(values2)
#this takes the voters list, of all voters in BC, and turns it into a list where each poll is assigned a postal code based on the most frequent postal code in that poll.
#if you take the data generated from this, and turn it into dissem areas, we can start cherry picking demographic data

import glob
import pandas as pd
import os
columns = ["RidingAbbr","PollNum","PostalCode","Combined_RidingPoll","CombinedRidingPollPC","FSA"]

def buildvoterslist():
    #this creates a file of all voter information
    #the following takes the voters list and creates a file with riding name, pull number and postal code
    path = r'C:\Users\rober\Dropbox\BCPoli\Voters_List\GP Nov2021'
    filenames = glob.glob(os.path.join(path +"/*.csv"))
    df_final = pd.DataFrame()
    for f in filenames:
        df_1 = pd.read_csv(f,encoding='cp1252',dtype=object,usecols=[0,1,17],header=None)
        df_1["Combined_Riding_Poll"] = df_1[0].astype(str)+df_1[1].astype(str)
        df_1["CombinedRidingPollPC"] = df_1[0].astype(str)+df_1[1].astype(str)+df_1[17].astype(str)
        df_1["FSA"] = df_1[17].str[:3]
        df_final = df_final.append(df_1)
    df_final.columns = columns
    df_final = df_final[df_final['PostalCode'].astype(str).str.startswith('V')]
    df_final.to_csv('allvoters.csv')
    return

def buildpcpolllist():
    print("eachpoll with postal codes all provinces started")
    #this reads the voter file created in buildvoterslist and turns it into a list where each poll is assigned the postal code that is most frequent for that poll# based on voters list
    df = pd.read_csv('allvoters.csv')
    df_unique_polls = df.groupby('Combined_RidingPoll').first().reset_index()
    ridingpoll_list = df_unique_polls['Combined_RidingPoll'].unique().tolist()
    pollname = []
    postalcode = []
    print("for loop started")
    for i in ridingpoll_list:
        df_pc = df[df['Combined_RidingPoll'].str.contains(i)]
        pc = df_pc.PostalCode.mode()[0]
        pollname.append(i)
        postalcode.append(pc)
    df_multiple_pc = pd.DataFrame(list(zip(pollname,postalcode)),columns=['Combined_RidingPoll','PostalCode'])
    df_multiple_pc['CombinedRidingPollPC'] = df_multiple_pc['Combined_RidingPoll'] + df_multiple_pc['PostalCode']
    print("eachpoll with postal codes all provinces done")
    return df_multiple_pc

#modify this function to be able to read just the Dissem areas we want to analyze and not the whole province
# def getDAuid():
#     #looks at stats can data and PCCF to see which GUIDS are attached to which postal codes
#     print("all data csv started")
#     # instructions: https://www12.statcan.gc.ca/wds-sdw/2021profile-profil2021-eng.cfm
#     #opens the statscan file for BC
#     df_dauid = pd.read_csv('98-401-X2021006_English_CSV_data_BritishColumbia.csv', encoding='cp1252', dtype=str)
#     #the column with all DAUIDs is DGUID
#     #get the list of DAUids you want
#     #use this page to build DGuid
#     df_dguid_wanted = pd.read_csv('sample_pccf_Data_dauid.csv',dtype=object)
#
#     #the column with dguids wanted is called DAuid
#     df = df_dauid.merge(df_dguid_wanted,left_on='ALT_GEO_CODE',right_on='DAuid')
#     df.to_csv('alldata.csv')
#     print("all data csv done")
#     return df

def makeStatsFile(inputdf):
#this takes the pccf file and turns it into a dataframe with postal code, poll number and DA from the other functions
    print("postal code, poll, dissem area csv started")
    PC_List = inputdf['PostalCode'].tolist()
    PostalCodeList = []
    DAuidList = []
    CSDuidList = []
    SACList = []
    Dissem_block_List = []
    columnnames = ['PostalCode', 'FSA', 'PR', 'CDuid', 'CSDuid', 'CSDName', 'CSDType', 'CCSCode', 'SAC', 'SACType',
                   'CTName', 'ER', 'DPL', 'FED13uid', 'POP_CNTR_RA', 'POP_CNTR_RA_type', 'DAuid', 'DisseminationBlock',
                   'Rep_Pt_Type', 'LAT', 'LONG', 'SLI', 'PCtype', 'Comm_Name', 'DMT', 'H_DMT', 'Birth_Date', 'Ret_Date',
                   'PO', 'QI', 'Source', 'POP_CNTR_RA_SIZE_CLASS']

    pccf_df = pd.read_csv(r'C:\Users\rober\Dropbox\BCPoli\PCCF\RawData\PCCF_FCCP_V2212_2021.txt', encoding='cp1252', sep='\t', names=columnnames, header=None)
    data_list = pccf_df[pccf_df.columns[0]].tolist()
    for i in data_list:
        PostalCodeList.append(i[0:6])
        DAuidList.append(i[125:133])
        CSDuidList.append(i[15:22])
        SACList.append(i[98:101])
        Dissem_block_List.append(i[133:136])

    df3 = pd.DataFrame(zip(PostalCodeList, DAuidList),columns=['PostalCode', 'DAuid'])
    df2 = df3.groupby('PostalCode').first().reset_index()
    just_bc_pccf_df = df2.loc[df2['PostalCode'].isin(PC_List)]
    final_df = just_bc_pccf_df.merge(inputdf, left_on='PostalCode', right_on='PostalCode')
    final_df.to_csv('riding_poll_pc_dguid.csv')
    print("postal code, poll, dissem area csv done")
    return final_df

if __name__ == "__main__":
    ##gets only the needed info from voters list
    #buildvoterslist()

    ##this can be modified and debugged to eventually only get the dissem areas one wants instead of building the whole province
    #getDAuid()

    ##takes the voters list and turns it into a list of poll#s, postal codes and a combination of both
    #builds a list of polls where each poll has a postal code based on the most frequent postal code appearing in that poll
    BC_PC_Poll_df = buildpcpolllist()

    #takes the stats can data, voter list data, and gives a file that has all postal codes, DAguids
    makeStatsFile(BC_PC_Poll_df)

    #needs a function where you can input the statscan data you want for the riding you want





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
def getDAuid(df_dguid_wanted):
    #looks at stats can data and PCCF to see which GUIDS are attached to which postal codes
    print("all data csv started")
    # instructions: https://www12.statcan.gc.ca/wds-sdw/2021profile-profil2021-eng.cfm
    #opens the statscan file for BC
    df_dauid = pd.read_csv('98-401-X2021006_English_CSV_data_BritishColumbia.csv', encoding='cp1252', dtype=str)
    if df_dguid_wanted.size < 2:
        df_dguid_wanted = pd.read_csv('riding_poll_pc_dguid.csv', dtype=str)
    #the column with all DAUIDs is DGUID
    #get the list of DAUids you want
    #use this page to build DGuid
    #df_dguid_wanted = pd.read_csv('sample_pccf_Data_dauid.csv',dtype=object)

    #the column with dguids wanted is called DAuid
    df = df_dauid.merge(df_dguid_wanted[['PostalCode','Combined_RidingPoll','DAuid']],left_on='ALT_GEO_CODE',right_on='DAuid')
    df.to_csv('all_statscan_data_by_poll.csv')
    print("all data csv done")
    return df

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
        #CSDuidList.append(i[15:22])
        #SACList.append(i[98:101])
        #Dissem_block_List.append(i[133:136])

    df3 = pd.DataFrame(zip(PostalCodeList, DAuidList),columns=['PostalCode', 'DAuid'])
    df2 = df3.groupby('PostalCode').first().reset_index()
    just_bc_pccf_df = df2.loc[df2['PostalCode'].isin(PC_List)]
    final_df = just_bc_pccf_df.merge(inputdf, left_on='PostalCode', right_on='PostalCode')
    final_df.to_csv('riding_poll_pc_dguid.csv')
    print("postal code, poll, dissem area csv done")
    return final_df

def process_data_by_poll():
    #ignore all rows starting with Total -
    df = pd.read_csv(r'C:\Users\rober\Dropbox\Ryan_Data_Sharing\all_statscan_data_by_poll.csv', header=None,dtype=object)
    #df = pd.read_csv(r'OneDGUIDESampleStats.csv', header=None)
    agefile = [7,9,10,11,13,14,15,16,17,18,19,20,21,22,24,25,26,27,29,30,31,32,38,39]
    housingfile = [40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,56]
    HousingSuitabilityfile = [1468,1469,1470,1471,1472,1473,1474,1475,1476,1477]
    relationshipfile = [59,60,61,62,63,64,65,66,67,68,69,70,93,94,95]
    empincfile = [125,127,137,156,157,158,159,160,161,162,163,164,165,166,167,2222,2223,2226]
    langfile = [398,490,495,499,504,516,520,539,560,563,580,585,592,599,605,606,607,626,637,645,646,648,673,678,682,683,692,705,725]
    citizenshipfile = [1522,1525,1527,1528,1529,1530,1531,1532,1533,1534,1535,1665,1666,1667]
    diversityfile = [1683,1684,1685,1686,1687,1688,1689,1690,1691,1692,1693,1694,1695,1696]
    educationfile = [1997,1998,1999,2000,2007,2008,2009,2010,2011,2012]

    #df_names = ['agefiledf','housingfiledf','relationshipfiledf','empincfiledf','langfiledf','citizenshipfiledf','diversityfiledf','educationfiledf','HousingSuitabilityfiledf']
    standard_cols = ['StatDescr','Count','Count_Men','Count_Women','Poll_Num','DGUID']
    #listnames = ['agefile', 'housingfile', 'relationshipfile', 'empincfile', 'langfile', 'citizenshipfile','diversityfile', 'educationfile', 'HousingSuitabilityfile']
    agefiledf = pd.DataFrame(columns=standard_cols)
    housingfiledf= pd.DataFrame(columns=standard_cols)
    HousingSuitabilityfiledf = pd.DataFrame(columns=standard_cols)
    relationshipfiledf = pd.DataFrame(columns=standard_cols)
    empincfiledf = pd.DataFrame(columns=standard_cols)
    langfiledf = pd.DataFrame(columns=standard_cols)
    citizenshipfiledf = pd.DataFrame(columns=standard_cols)
    diversityfiledf = pd.DataFrame(columns=standard_cols)
    educationfiledf = pd.DataFrame(columns=standard_cols)
    Stat = df[10].tolist()
    Count_Total = df[12].tolist()
    Count_Men = df[14].tolist()
    Count_Women = df[16].tolist()
    Poll_Num = df[25].tolist()
    DGUID = df[3].tolist()
    for i in range (len(Stat)):
        i= i-1
        newrow = {'Poll_Num':Poll_Num[i],'DGUID':DGUID[i],'StatDescr':Stat[i],'Count':Count_Total[i],'Count_Men':Count_Men[i],'Count_Women':Count_Women[i]}
        new_df = pd.DataFrame(newrow,index=[0])
        if i in agefile:
            agefiledf = pd.concat([new_df,agefiledf.loc[:]]).reset_index(drop=True)
        if i in housingfile:
            housingfiledf = pd.concat([new_df, housingfiledf.loc[:]]).reset_index(drop=True)
        if i in HousingSuitabilityfile:
            HousingSuitabilityfiledf = pd.concat([new_df, HousingSuitabilityfiledf.loc[:]]).reset_index(drop=True)
        if i in relationshipfile:
            relationshipfiledf = pd.concat([new_df, relationshipfiledf.loc[:]]).reset_index(drop=True)
        if i in empincfile:
            empincfiledf = pd.concat([new_df, empincfiledf.loc[:]]).reset_index(drop=True)
        if i in langfile:
            langfiledf = pd.concat([new_df, langfiledf.loc[:]]).reset_index(drop=True)
        if i in citizenshipfile:
            citizenshipfiledf = pd.concat([new_df, citizenshipfiledf.loc[:]]).reset_index(drop=True)
        if i in diversityfile:
            diversityfiledf = pd.concat([new_df, diversityfiledf.loc[:]]).reset_index(drop=True)
        if i in educationfile:
            educationfiledf = pd.concat([new_df, educationfiledf.loc[:]]).reset_index(drop=True)
        new_df.drop(new_df.index, inplace=True)
    # #transpose the file to make it easier for mapping? if it doesn't work, delete it
    # agefiledf2 = agefiledf.set_index('Poll_Num').T
    # housingfiledf2 = housingfiledf.transpose()
    # HousingSuitabilityfiledf2 = HousingSuitabilityfiledf.transpose()
    # relationshipfiledf2 = relationshipfiledf.transpose()
    # empincfiledf2 = empincfiledf.transpose()
    # langfiledf2 = langfiledf.transpose()
    # citizenshipfiledf2 = citizenshipfiledf.transpose()
    # diversityfiledf2 = diversityfiledf.transpose()
    # educationfiledf2 = educationfiledf.transpose()

    #export to csv
    # agefiledf2.to_csv('age_stats.csv')
    # housingfiledf2.to_csv('housing_stats.csv')
    # HousingSuitabilityfiledf2.to_csv('housing_stats.csv')
    # relationshipfiledf2.to_csv('relationship.csv')
    # empincfiledf2.to_csv('employmentandincome.csv')
    # langfiledf2.to_csv('languages.csv',index=False)
    # citizenshipfiledf2.to_csv('citizenship.csv',index=False)
    # diversityfiledf2.to_csv('ethnicity.csv',index=False)
    # educationfiledf2.to_csv('education.csv',index=False)

    agefiledf.to_csv('age_stats.csv',index=False)
    housingfiledf.to_csv('housing_stats.csv',index=False)
    HousingSuitabilityfiledf.to_csv('housing_stats.csv',index=False)
    relationshipfiledf.to_csv('relationship.csv',index=False)
    empincfiledf.to_csv('employmentandincome.csv',index=False)
    langfiledf.to_csv('languages.csv',index=False)
    citizenshipfiledf.to_csv('citizenship.csv',index=False)
    diversityfiledf.to_csv('ethnicity.csv',index=False)
    educationfiledf.to_csv('education.csv',index=False)

    print(agefiledf.head())
    print(agefiledf.size)









    return


if __name__ == "__main__":
    ##gets only the needed info from voters list
    #buildvoterslist()



    # ##takes the voters list and turns it into a list of poll#s, postal codes and a combination of both
    # #builds a list of polls where each poll has a postal code based on the most frequent postal code appearing in that poll
    # BC_PC_Poll_df = buildpcpolllist()
    #
    # #takes the stats can data, voter list data, and gives a file that has all postal codes, DAguids
    # df = makeStatsFile(BC_PC_Poll_df)

    #this can be modified and debugged to eventually only get the dissem areas one wants instead of building the whole province
    #if running full scrip and can pass dataframe
    # getDAuid(df)
    #if just running creation of stats can file
    # df = pd.DataFrame()
    # getDAuid(df)
    process_data_by_poll()




from data_functions import EIA_Series
import pandas as pd

master = {}
regions = ['US','TX','CA','NY','FL','IA','PA','IL']
sources = ['ALL','WND','TSN','HYC','NG','NUC','COW']

import pandas as pd
region_frame = pd.DataFrame()
wind_abs = pd.DataFrame()
wind_perc = pd.DataFrame()
solar_abs = pd.DataFrame()
solar_perc = pd.DataFrame()



for region in regions:

    eia_classes = {}

    for source in sources:
        endpoint = 'ELEC.GEN.{}-{}-99.A'.format(source,region)
        eia_classes[source] = EIA_Series(endpoint,name = region,date_format='%Y')

        try:
            region_frame.loc[region,source] = eia_classes[source].frame.loc['2020-01-01',region]
        except:
            region_frame.loc[region,source] = 0
        
    print("Data for {} acquired".format(region))

    wind_abs[region] = eia_classes['WND'].frame[region]
    wind_perc[region] = (eia_classes['WND'].frame[region]/eia_classes['ALL'].frame[region])*100
    solar_abs[region] = eia_classes['TSN'].frame[region]
    solar_perc[region] = (eia_classes['TSN'].frame[region]/eia_classes['ALL'].frame[region])*100

regions_perc = region_frame.copy()

def get_perc(row):
    sources = ['WND','TSN','NG','NUC','COW','HYC']

    known_sources = 0
    for source in sources:
        row[source] = round((row[source]/row['ALL'])*100,2)
        known_sources += row[source]

    row['known_sources'] = known_sources
    row['Other'] = 100 - known_sources

    return row

col_dict = {'WND':'Wind','TSN':'Solar','NG':'Gas','NUC':'Nuclear','COW':'Coal','HYC':'Hydro'}

regions_perc = regions_perc.apply(lambda row:get_perc(row),axis=1).drop(['ALL','known_sources'],axis=1)
regions_perc.rename(columns = col_dict,inplace=True)
region_frame.rename(columns = col_dict,inplace=True)
regions_perc

regions_perc.to_csv('data/regions_perc.csv')
region_frame.to_csv('data/regions_abs.csv')
wind_abs.to_csv('data/wind_abs.csv')
wind_perc.to_csv('data/wind_perc.csv')
solar_abs.to_csv('data/solar_abs.csv')
solar_perc.to_csv('data/solar_perc.csv')

print("All Data succesfully acquired")
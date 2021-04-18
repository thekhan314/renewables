import pandas as pd
regions = pd.DataFrame()
wind_abs = pd.DataFrame()
wind_perc = pd.DataFrame()
solar_abs = pd.DataFrame()
solar_perc = pd.DataFrame()
natgas_perc = pd.DataFrame()

for key,item in all_series.items():

    all = EIA_Series(item['all'],name = key,date_format='%Y')
    wind = EIA_Series(item['wind'],name = key, date_format='%Y')
    solar = EIA_Series(item['solar'],name = key,date_format='%Y')
    natgas = EIA_Series(item['natgas'],name = key,date_format='%Y')
    nuclear = EIA_Series(item['nuclear'],name = key,date_format='%Y')

    regions.loc[key,]

    wind_abs[key] = wind.frame[key]
    wind_perc[key] = (wind.frame[key]/all.frame[key])*100
    solar_abs[key] = solar.frame[key]
    solar_perc[key] = (solar.frame[key]/all.frame[key])*100
    natgas_perc[key] = (natgas.frame[key]/all.frame[key])*100

    all_series = {
    'us':{
        'all':'ELEC.GEN.ALL-US-99.A ',
        'wind':'ELEC.GEN.WND-US-99.A',
        'solar':'ELEC.GEN.TSN-US-99.A',
        'natgas':'ELEC.GEN.NG-US-99.A',
        'nuclear':'ELEC.GEN.NUC-US-99.A ',
        'coal':'ELEC.GEN.COW-US-99.A'
    },
    'tx':{
        'all':'ELEC.GEN.ALL-TX-99.A ',
        'wind':'ELEC.GEN.WND-TX-99.A',
        'solar':'ELEC.GEN.TSN-TX-99.A',
        'natgas':'ELEC.GEN.NG-TX-99.A',
        'nuclear':'ELEC.GEN.NUC-TX-99.A',
        'coal':'ELEC.GEN.COW-TX-99.A'
    },
    'ca':{
        'all':'ELEC.GEN.ALL-CA-99.A',
        'wind':'ELEC.GEN.WND-CA-99.A',
        'solar':'ELEC.GEN.TSN-CA-99.A',
        'natgas':'ELEC.GEN.NG-CA-99.A',
        'nuclear':'ELEC.GEN.NUC-CA-99.A',
        'coal':'ELEC.GEN.COW-CA-99.A'

    },
    'ny':{
        'all':'ELEC.GEN.ALL-NY-99.A',
        'wind':'ELEC.GEN.WND-NY-99.A',
        'solar':'ELEC.GEN.TSN-NY-99.A',
        'natgas':'ELEC.GEN.NG-NY-99.A ',
        'nuclear':'ELEC.GEN.NUC-NY-99.A',
        'coal':'ELEC.GEN.COW-NY-99.A'
    },
    'fl':{
        'all':'ELEC.GEN.ALL-FL-99.A',
        'wind':'ELEC.GEN.WND-FL-99.A',
        'solar':'ELEC.GEN.TSN-FL-99.A',
        'natgas':'ELEC.GEN.NG-FL-99.A',
        'nuclear':'ELEC.GEN.NUC-FL-99.A',
        'coal':'ELEC.GEN.COW-FL-99.A'

    },
    'iw':{
        'all':'ELEC.GEN.ALL-IA-99.A',
        'wind':'ELEC.GEN.WND-IA-99.A',
        'solar':'ELEC.GEN.TSN-IA-99.A',
        'natgas':'ELEC.GEN.NG-IA-99.A',
        'nuclear':'ELEC.GEN.NUC-IA-99.A',
        'coal':'ELEC.GEN.COW-IA-99.A'

    }

import pandas as pd
import datetime as dt
from address_book import addresses as adds
import email_func_reaper as email_func
import schedule_methods
from gsheets import Sheets
import filepaths
import smtplib 
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
import numpy as np
from email import encoders 
import os
from os import listdir
from os.path import isfile, join

def get_cw():
    url='https://docs.google.com/spreadsheets/d/1beoDjrKwv8_oXkfC2VhDorAHXeEkQ9GkFccI0poeVAk/edit?usp=sharing'
    sheets=Sheets.from_files(filepaths.gsheets)
    contentdf=sheets.get(url)
    contentdf=contentdf.find('Content Warehouse').to_frame(dtype={'Reporting Content ID':str})
    contentdf['Reporting Content ID']=contentdf['Reporting Content ID'].str[:7]
    contentdf['Content Pitch']=np.where(contentdf['Type (Pitch)'].isna(),contentdf['Pitch Resolution'],contentdf['Type (Pitch)'])
    return contentdf 
cw = get_cw()
with pd.ExcelWriter(f'/Users/leonarie/Desktop/Python/reaper/cw.xlsx') as writer:
     cw.to_excel(writer,sheet_name="14-day & Over -1K Opp Cost",index=False,
                    # float_format=currency_format, columns=['Revenue', 'Opportunity Cost', 'eCPM']
                   )

#load files
cw = pd.read_excel("/Users/leonarie/Desktop/Python/reaper/cw.xlsx")
lexi_files = [f for f in listdir(filepaths.downloadpath) if isfile(join(filepaths.downloadpath, f)) & ('Lexi 3.0 - 1 Year (' in f)]
lexi_versions = [int(num.split('(')[1].split(')')[0]) for num in lexi_files]
most_recent_lexi = max(lexi_versions)
df = pd.read_excel("{}Lexi 3.0 - 1 Year ({}).xlsx".format(filepaths.downloadpath,most_recent_lexi))
max_date = df["Date"].max()
max_date_str = max_date.strftime('%D').replace('/', '-')
sdf = df[df["Date"]>=(max(df["Date"])-dt.timedelta(14))]
ldf = df[df["Date"]>=(max(df["Date"])-dt.timedelta(30))]
offers = schedule_methods.get_smartsheet("offers")
emit = schedule_methods.get_smartsheet("emit")
offers = offers[offers["Operational Status"]!="Canceled"]
emit = emit.loc[(emit["Status"]=="PRIMARY")]
emit["Dataset"] = emit["Revenue Pub ID"].astype(str).str.split('.').str[0] + "_" +emit['DP.DS or DP.DV if multiple sources using samePubID']
cobra = schedule_methods.get_cobra()


vertical_eCPM = df.groupby(["Vertical"], as_index=False).agg({
          "Clicks":"sum",
          "Delivered":"sum",
          })
vertical_eCPM['Vertical CTR'] = (vertical_eCPM["Clicks"]/vertical_eCPM["Delivered"])*100
vertical_eCPM = vertical_eCPM.drop(columns=['Clicks', 'Delivered'], inplace=False)

def harvest(df, vertical_eCPM):
     harvest = df.groupby(["Hitpath Offer ID", "Creative Type"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Opportunity Cost":"sum",
          "Clicks": "sum",
          "Opens": "sum"
          })
     harvest['Creative Type Revenue'] = (harvest["Revenue"])
     harvest['Offer Creative Type CTR'] = (harvest["Clicks"]/harvest["Delivered"])*100
     harvest['Offer Creative Type eCPM'] = harvest["Revenue"]*1000/harvest["Delivered"]
     harvest['Drops'] = df.groupby(["Hitpath Offer ID", "Creative Type"]).size().reset_index(name='Drops')['Drops']

     cols = ["Hitpath Offer ID","Offer Name","RX Rep","Payout Type","Vertical", "Advertiser Name","Delayed Reporting","Operational Status"]
     
     market = harvest.merge(offers[cols],how='left', on='Hitpath Offer ID')
     market = market.merge(vertical_eCPM, how='left', on='Vertical')
     return market
    
scyth = harvest(sdf, vertical_eCPM)
hood = harvest(ldf, vertical_eCPM)

grim = scyth.loc[(scyth["Payout Type"]!="CPM") & (scyth["Offer Creative Type eCPM"]<1) & (scyth["Opportunity Cost"]<-1000)]
reaper = hood.loc[(hood["Payout Type"]!="CPM") & (hood["Offer Creative Type eCPM"]<1) & (hood["Opportunity Cost"]<-2500)]

grim = grim[["Hitpath Offer ID", "Creative Type Revenue","Offer Name", "Creative Type", "Offer Creative Type eCPM", "Offer Creative Type CTR", "Vertical CTR", "Delivered", "Opportunity Cost", "Drops", "Vertical", "RX Rep","Payout Type", "Advertiser Name","Delayed Reporting","Operational Status"]
     ]
reaper = reaper[["Hitpath Offer ID","Creative Type Revenue", "Offer Name", "Creative Type", "Offer Creative Type eCPM", "Offer Creative Type CTR", "Vertical CTR", "Delivered", "Opportunity Cost", "Drops", "Vertical", "RX Rep","Payout Type", "Advertiser Name","Delayed Reporting","Operational Status"]
     ]

#offer_stats includes Production and DKIM
offer_stats_p = sdf.groupby(["Hitpath Offer ID"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Clicks":"sum"
          })
offer_stats_p['Offer Revenue'] = (offer_stats_p["Revenue"])
offer_stats_p['Offer eCPM'] = (offer_stats_p["Revenue"]*1000/offer_stats_p["Delivered"])
offer_stats_p['Offer CTR'] = (offer_stats_p["Clicks"]*100/offer_stats_p["Delivered"])
offer_stats_p = offer_stats_p.drop(columns=['Delivered', 'Revenue', 'Clicks'], inplace=False)

offer_stats_p2 = ldf.groupby(["Hitpath Offer ID"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Clicks":"sum"
          })
offer_stats_p2['Offer Revenue'] = (offer_stats_p2["Revenue"])
offer_stats_p2['Offer eCPM'] = (offer_stats_p2["Revenue"]*1000/offer_stats_p2["Delivered"])
offer_stats_p2['Offer CTR'] = (offer_stats_p2["Clicks"]*100/offer_stats_p2["Delivered"])
offer_stats_p2 = offer_stats_p2.drop(columns=['Delivered', 'Revenue', 'Clicks'], inplace=False)


#offer_stats includes DKIM only
dkim = sdf[sdf['Message'].str.contains('DKIM', case=False, na=False)]
dkim2 = ldf[ldf['Message'].str.contains('DKIM', case=False, na=False)]

offer_stats_dkim = dkim.groupby(["Hitpath Offer ID"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Clicks":"sum"
          })
offer_stats_dkim['Dkim eCPM'] = (offer_stats_dkim["Revenue"]*1000/offer_stats_dkim["Delivered"])
offer_stats_dkim['Dkim CTR'] = (offer_stats_dkim["Clicks"]/offer_stats_dkim["Delivered"])*100
offer_stats_dkim= offer_stats_dkim.drop(columns=['Delivered', 'Revenue', 'Clicks'], inplace=False)

offer_stats_dkim2 = dkim2.groupby(["Hitpath Offer ID"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Clicks":"sum"
          })
offer_stats_dkim2['Dkim eCPM'] = (offer_stats_dkim2["Revenue"]*1000/offer_stats_dkim2["Delivered"])
offer_stats_dkim2['Dkim CTR'] = (offer_stats_dkim2["Clicks"]/offer_stats_dkim2["Delivered"])*100
offer_stats_dkim2 = offer_stats_dkim2.drop(columns=['Delivered', 'Revenue', 'Clicks'], inplace=False)

#offer_stats includes production only
not_dkim = sdf[~sdf['Message'].str.contains('DKIM', case=False, na=False)]
not_dkim2 = ldf[~ldf['Message'].str.contains('DKIM', case=False, na=False)]

offer_stats_not_dkim = not_dkim.groupby(["Hitpath Offer ID"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Clicks":"sum"
          })
offer_stats_not_dkim['Non-Dkim eCPM'] = (offer_stats_not_dkim["Revenue"]*1000/offer_stats_not_dkim["Delivered"])
offer_stats_not_dkim['Non-Dkim CTR'] = (offer_stats_not_dkim["Clicks"]/offer_stats_not_dkim["Delivered"])*100
offer_stats_not_dkim= offer_stats_not_dkim.drop(columns=['Delivered', 'Revenue', 'Clicks'], inplace=False)

offer_stats_not_dkim2 = not_dkim2.groupby(["Hitpath Offer ID"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Clicks":"sum"
          })
offer_stats_not_dkim2['Non-Dkim eCPM'] = (offer_stats_not_dkim2["Revenue"]*1000/offer_stats_not_dkim2["Delivered"])
offer_stats_not_dkim2['Non-Dkim CTR'] = (offer_stats_not_dkim2["Clicks"]/offer_stats_not_dkim2["Delivered"])*100
offer_stats_not_dkim2 = offer_stats_not_dkim2.drop(columns=['Delivered', 'Revenue', 'Clicks'], inplace=False)

#content_stats includes DKIM only
content_stats_dkim = dkim.groupby(["Hitpath Offer ID", "Creative Type"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Clicks":"sum"
          })
content_stats_dkim['Dkim Offer Creative Type eCPM'] = (content_stats_dkim["Revenue"]*1000/content_stats_dkim["Delivered"])
content_stats_dkim['Dkim Offer Creative Type CTR'] = (content_stats_dkim["Clicks"]/content_stats_dkim["Delivered"])*100
content_stats_dkim= content_stats_dkim.drop(columns=['Delivered', 'Revenue', 'Clicks'], inplace=False)

content_stats_dkim2 = dkim2.groupby(["Hitpath Offer ID", "Creative Type"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Clicks":"sum"
          })
content_stats_dkim2['Dkim Offer Creative Type eCPM'] = (content_stats_dkim2["Revenue"]*1000/content_stats_dkim2["Delivered"])
content_stats_dkim2['Dkim Offer Creative Type CTR'] = (content_stats_dkim2["Clicks"]/content_stats_dkim2["Delivered"])*100
content_stats_dkim2 = content_stats_dkim2.drop(columns=['Delivered', 'Revenue', 'Clicks'], inplace=False)

#content_stats includes production only
content_stats_not_dkim = not_dkim.groupby(["Hitpath Offer ID", "Creative Type"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Clicks":"sum"
          })
content_stats_not_dkim['Non-Dkim Offer Creative Type eCPM'] = (content_stats_not_dkim["Revenue"]*1000/content_stats_not_dkim["Delivered"])
content_stats_not_dkim['Non-Dkim Offer Creative Type CTR'] = (content_stats_not_dkim["Clicks"]/content_stats_not_dkim["Delivered"])*100
content_stats_not_dkim= content_stats_not_dkim.drop(columns=['Delivered', 'Revenue', 'Clicks'], inplace=False)

content_stats_not_dkim2 = not_dkim2.groupby(["Hitpath Offer ID", "Creative Type"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Clicks":"sum"
          })
content_stats_not_dkim2['Non-Dkim Offer Creative Type eCPM'] = (content_stats_not_dkim2["Revenue"]*1000/content_stats_not_dkim2["Delivered"])
content_stats_not_dkim2['Non-Dkim Offer Creative Type CTR'] = (content_stats_not_dkim2["Clicks"]/content_stats_not_dkim2["Delivered"])*100
content_stats_not_dkim2 = content_stats_not_dkim2.drop(columns=['Delivered', 'Revenue', 'Clicks'], inplace=False)

# 1 year eCPM and CTR
year_1 = df[df["Date"]>=(max(df["Date"])-dt.timedelta(365))]
year_1_offer_stats_p = year_1.groupby(["Hitpath Offer ID"], as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Clicks":"sum"
          })
year_1_offer_stats_p['1 Year Offer eCPM'] = (year_1_offer_stats_p["Revenue"]*1000/year_1_offer_stats_p["Delivered"])
year_1_offer_stats_p['1 Year Offer CTR'] = (year_1_offer_stats_p["Clicks"]*100/year_1_offer_stats_p["Delivered"])
year_1_offer_stats_p = year_1_offer_stats_p.drop(columns=['Delivered', 'Revenue', 'Clicks'], inplace=False)

cw_stats = cw.groupby('OfferIDs').size().reset_index(name='Custom Content Inventory')
# Clean the OfferID column: replace periods with commas, then strip leading/trailing commas
cw_stats['Hitpath Offer ID'] = cw_stats['OfferIDs'].str.replace('.', ',').str.strip(',')

# Split the OfferID column into lists of OfferIDs
cw_stats['Hitpath Offer ID'] = cw_stats['Hitpath Offer ID'].str.split(',')

# Explode the lists into separate rows
cw_stats = cw_stats.explode('Hitpath Offer ID').reset_index(drop=True)
cw_stats = cw_stats[cw_stats['Hitpath Offer ID'].str.isnumeric()]
cw_stats['Hitpath Offer ID'] = cw_stats['Hitpath Offer ID'].astype(int)
cw_stats = cw_stats.drop(columns=['OfferIDs'], inplace=False)

#merge the extra columns to grim and reaper
final_grim_merged_df = pd.merge(grim, offer_stats_p, on='Hitpath Offer ID', how='left')
final_grim_merged_df = pd.merge(final_grim_merged_df, offer_stats_dkim, on='Hitpath Offer ID', how='left')
final_grim_merged_df = pd.merge(final_grim_merged_df, offer_stats_not_dkim, on='Hitpath Offer ID', how='left')
final_grim_merged_df = pd.merge(final_grim_merged_df, year_1_offer_stats_p, on='Hitpath Offer ID', how='left')
final_grim_merged_df = pd.merge(final_grim_merged_df, content_stats_dkim, on=['Hitpath Offer ID', 'Creative Type'], how='left')
final_grim_merged_df = pd.merge(final_grim_merged_df, content_stats_not_dkim, on=['Hitpath Offer ID', 'Creative Type'], how='left')
final_grim_merged_df = pd.merge(final_grim_merged_df, cw_stats, on=['Hitpath Offer ID'], how='left')
final_grim_merged_df['Content vs. Offer eCPM Difference'] = final_grim_merged_df['Offer Creative Type eCPM'] - final_grim_merged_df['Offer eCPM']
final_grim_merged_df['Content vs. Offer CTR Difference'] = final_grim_merged_df['Offer Creative Type CTR'] - final_grim_merged_df['Offer CTR']

final_grim_merged_df2 = pd.merge(reaper, offer_stats_p2, on='Hitpath Offer ID', how='left')
final_grim_merged_df2 = pd.merge(final_grim_merged_df2, offer_stats_dkim2, on='Hitpath Offer ID', how='left')
final_grim_merged_df2 = pd.merge(final_grim_merged_df2, offer_stats_not_dkim2, on='Hitpath Offer ID', how='left')
final_grim_merged_df2 = pd.merge(final_grim_merged_df2, year_1_offer_stats_p, on='Hitpath Offer ID', how='left')
final_grim_merged_df2 = pd.merge(final_grim_merged_df2, content_stats_dkim2, on=['Hitpath Offer ID', 'Creative Type'], how='left')
final_grim_merged_df2 = pd.merge(final_grim_merged_df2, content_stats_not_dkim2, on=['Hitpath Offer ID', 'Creative Type'], how='left')
final_grim_merged_df2 = pd.merge(final_grim_merged_df2, cw_stats, on=['Hitpath Offer ID'], how='left')
final_grim_merged_df2['Content vs. Offer eCPM Difference'] = final_grim_merged_df2['Offer Creative Type eCPM'] - final_grim_merged_df2['Offer eCPM']
final_grim_merged_df2['Content vs. Offer CTR Difference'] = final_grim_merged_df2['Offer Creative Type CTR'] - final_grim_merged_df2['Offer CTR']

#clean up
columns_to_round = [
    'Offer Creative Type eCPM', 'Offer Creative Type CTR', 'Vertical CTR', 'Opportunity Cost',
    'Offer eCPM', 'Offer CTR', 'Dkim eCPM', 'Dkim CTR', 'Non-Dkim eCPM', 'Non-Dkim CTR',
    '1 Year Offer eCPM', '1 Year Offer CTR', 'Dkim Offer Creative Type eCPM', 'Dkim Offer Creative Type CTR',
    'Non-Dkim Offer Creative Type eCPM', 'Non-Dkim Offer Creative Type CTR','Content vs. Offer eCPM Difference', "Content vs. Offer CTR Difference"
]

final_grim_merged_df[columns_to_round] = final_grim_merged_df[columns_to_round].round(2)
final_grim_merged_df['Offer Revenue'] = final_grim_merged_df['Offer Revenue'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Creative Type Revenue'] = final_grim_merged_df['Creative Type Revenue'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Offer eCPM'] = final_grim_merged_df['Offer eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Offer Creative Type eCPM'] = final_grim_merged_df['Offer Creative Type eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Opportunity Cost'] = final_grim_merged_df['Opportunity Cost'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Content vs. Offer eCPM Difference'] = final_grim_merged_df['Content vs. Offer eCPM Difference'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Non-Dkim Offer Creative Type eCPM'] = final_grim_merged_df['Non-Dkim Offer Creative Type eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Dkim eCPM'] = final_grim_merged_df['Dkim eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Non-Dkim eCPM'] = final_grim_merged_df['Non-Dkim eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['1 Year Offer eCPM'] = final_grim_merged_df['1 Year Offer eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Dkim Offer Creative Type eCPM'] = final_grim_merged_df['Dkim Offer Creative Type eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Offer CTR'] = final_grim_merged_df['Offer CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Offer Creative Type CTR'] = final_grim_merged_df['Offer Creative Type CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Content vs. Offer CTR Difference'] = final_grim_merged_df['Content vs. Offer CTR Difference'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Dkim Offer Creative Type CTR'] = final_grim_merged_df['Dkim Offer Creative Type CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Non-Dkim Offer Creative Type CTR'] = final_grim_merged_df['Non-Dkim Offer Creative Type CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Non-Dkim CTR'] = final_grim_merged_df['Non-Dkim CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['1 Year Offer CTR'] = final_grim_merged_df['1 Year Offer CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Vertical CTR'] = final_grim_merged_df['Vertical CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df['Dkim CTR'] = final_grim_merged_df['Dkim CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)


final_grim_merged_df2[columns_to_round] = final_grim_merged_df2[columns_to_round].round(2)
final_grim_merged_df2['Offer Revenue'] = final_grim_merged_df2['Offer Revenue'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Creative Type Revenue'] = final_grim_merged_df2['Creative Type Revenue'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Offer eCPM'] = final_grim_merged_df2['Offer eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Offer Creative Type eCPM'] = final_grim_merged_df2['Offer Creative Type eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Opportunity Cost'] = final_grim_merged_df2['Opportunity Cost'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Content vs. Offer eCPM Difference'] = final_grim_merged_df2['Content vs. Offer eCPM Difference'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Non-Dkim Offer Creative Type eCPM'] = final_grim_merged_df2['Non-Dkim Offer Creative Type eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Dkim eCPM'] = final_grim_merged_df2['Dkim eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Non-Dkim eCPM'] = final_grim_merged_df2['Non-Dkim eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['1 Year Offer eCPM'] = final_grim_merged_df2['1 Year Offer eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Dkim Offer Creative Type eCPM'] = final_grim_merged_df2['Dkim Offer Creative Type eCPM'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Offer CTR'] = final_grim_merged_df2['Offer CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Offer Creative Type CTR'] = final_grim_merged_df2['Offer Creative Type CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Content vs. Offer CTR Difference'] = final_grim_merged_df2['Content vs. Offer CTR Difference'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Dkim Offer Creative Type CTR'] = final_grim_merged_df2['Dkim Offer Creative Type CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Non-Dkim Offer Creative Type CTR'] = final_grim_merged_df2['Non-Dkim Offer Creative Type CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Non-Dkim CTR'] = final_grim_merged_df2['Non-Dkim CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['1 Year Offer CTR'] = final_grim_merged_df2['1 Year Offer CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Vertical CTR'] = final_grim_merged_df2['Vertical CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)
final_grim_merged_df2['Dkim CTR'] = final_grim_merged_df2['Dkim CTR'].apply(lambda x: '{:,.2f}%'.format(x) if pd.notnull(x) else np.nan)

final_grim_merged_df = final_grim_merged_df[["Hitpath Offer ID", "Offer Name", "Creative Type", "Creative Type Revenue", "Offer Revenue", "Offer eCPM", "Offer Creative Type eCPM", "Content vs. Offer eCPM Difference",
                                             "Dkim Offer Creative Type eCPM","Non-Dkim Offer Creative Type eCPM","Dkim eCPM", "Non-Dkim eCPM", "Offer CTR", "Offer Creative Type CTR", "Content vs. Offer CTR Difference",
                                             "Dkim Offer Creative Type CTR", "Non-Dkim Offer Creative Type CTR","Dkim CTR", "Non-Dkim CTR", "Custom Content Inventory", "1 Year Offer eCPM", "1 Year Offer CTR","Vertical CTR", "Delivered", "Opportunity Cost", "Drops", "Vertical", "RX Rep","Payout Type", "Advertiser Name","Delayed Reporting","Operational Status"
                                            ]]

final_grim_merged_df2 = final_grim_merged_df2[["Hitpath Offer ID", "Offer Name", "Creative Type", "Creative Type Revenue", "Offer Revenue", "Offer eCPM", "Offer Creative Type eCPM", "Content vs. Offer eCPM Difference",
                                             "Dkim Offer Creative Type eCPM","Non-Dkim Offer Creative Type eCPM","Dkim eCPM", "Non-Dkim eCPM", "Offer CTR", "Offer Creative Type CTR", "Content vs. Offer CTR Difference",
                                             "Dkim Offer Creative Type CTR", "Non-Dkim Offer Creative Type CTR","Dkim CTR", "Non-Dkim CTR", "Custom Content Inventory", "1 Year Offer eCPM", "1 Year Offer CTR","Vertical CTR", "Delivered", "Opportunity Cost", "Drops", "Vertical", "RX Rep","Payout Type", "Advertiser Name","Delayed Reporting","Operational Status"
                                            ]]


#Souls
ldf = df[df["Date"]>=(max(df["Date"])-dt.timedelta(120))]
worth = ldf.groupby(["DP&Pub","Hitpath Offer ID","Creative Type"]).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Opportunity Cost":"sum",
          "Clicks": "sum",
          "Opens": "sum"
          }).reset_index()
worth['Creative Type CTR'] = (worth["Clicks"]/worth["Opens"])*100
worth['Creative Type eCPM'] = worth["Revenue"]*1000/worth["Delivered"]

cols = ["Hitpath Offer ID","Vertical"]
worth = worth.merge(offers[cols], how='left', on='Hitpath Offer ID')
worth = worth.merge(vertical_eCPM, how='left', on='Vertical')

worth['Revenue'] = worth['Revenue'].map('${:,.2f}'.format)
worth['Creative Type eCPM'] = worth['Creative Type eCPM'].map('${:,.2f}'.format)
worth['Opportunity Cost'] = worth['Opportunity Cost'].map('${:,.2f}'.format)

souls = cobra[cobra["CC ID (mailers) / Reporting ID (Akshad)"].str.contains(
     '|'.join(list(set(grim["Creative Type"].unique()).union(
          set(reaper["Creative Type"].unique())))),
     regex=True) & (cobra["Date"] >= dt.datetime.today())]

souls.rename(columns = {'Creative Type':'Content Type','CC ID (mailers) / Reporting ID (Akshad)':'Creative Type'}, inplace = True)
souls = souls.merge(emit[["Dataset","Analyst","Emailer","DP&Pub"]],on='Dataset')
souls = souls.merge(worth, on=["DP&Pub","Hitpath Offer ID","Creative Type"],how="left").fillna("No drops in last 120 days.")
souls = souls[['Date', 'Dataset', 'Drop', 'Send Strategy','Campaign ID', 'Creative Type', 'Content Type', 'Revenue', 'Delivered', 'Creative Type eCPM', 'Opportunity Cost', 'Vertical CTR','Analyst', 'Emailer']]

souls["Offer ID"] = souls["Campaign ID"].apply(lambda x: x.split('-')[0])
offers_l = offers.dropna(subset=['Hitpath Offer ID'])
offers_l['Hitpath Offer ID'] = offers_l['Hitpath Offer ID'].astype(int)
offers_l['Hitpath Offer ID'] = offers_l['Hitpath Offer ID'].astype(str)
souls = souls.merge(offers_l, how='left', left_on = 'Offer ID', right_on = 'Hitpath Offer ID')
souls = souls[['Date', 'Dataset', 'Drop', 'Send Strategy','Campaign ID','Creative Type', 'Content Type', 'Revenue', 'Delivered', 'Creative Type eCPM', 'Opportunity Cost', 'Vertical CTR','Analyst', 'RX Rep','Emailer']]


with pd.ExcelWriter(f'/Users/leonarie/Desktop/Python/reaper/grimreaper_content_{max_date_str}.xlsx') as writer:
     final_grim_merged_df.to_excel(writer,sheet_name="14-day & Over -1K Opp Cost",index=False,
                    # float_format=currency_format, columns=['Revenue', 'Opportunity Cost', 'eCPM']
                   )
     final_grim_merged_df2.to_excel(writer,sheet_name="30-Day & Over -2.5K Opp Cost",index=False,
                      # float_format=currency_format, columns=['Revenue', 'Opportunity Cost', 'eCPM']
                     )
     souls.to_excel(writer,sheet_name="Upcoming Drops",index=False)

lexi_files = [f for f in listdir(filepaths.downloadpath) if isfile(join(filepaths.downloadpath, f)) & ('Lexi 3.0 - 1 Year (' in f)]
lexi_versions = [int(num.split('(')[1].split(')')[0]) for num in lexi_files]
most_recent_lexi = max(lexi_versions)
df = pd.read_excel("{}Lexi 3.0 - 1 Year ({}).xlsx".format(filepaths.downloadpath,most_recent_lexi))
max_date = df["Date"].max()
max_date_str = max_date.strftime('%D').replace('/', '-')
sdf = df[df["Date"]>=(max(df["Date"])-dt.timedelta(14))]
ldf = df[df["Date"]>=(max(df["Date"])-dt.timedelta(30))]
offers = schedule_methods.get_smartsheet("offers")
emit = schedule_methods.get_smartsheet("emit")
offers = offers[offers["Operational Status"]!="Canceled"]
emit = emit.loc[(emit["Status"]=="PRIMARY")]
emit["Dataset"] = emit["Revenue Pub ID"].astype(str).str.split('.').str[0] + "_" +emit['DP.DS or DP.DV if multiple sources using samePubID']
cobra = schedule_methods.get_cobra()


def harvest_2(df):
     harvest = df.groupby("Hitpath Offer ID", as_index=False).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Opportunity Cost":"sum",
          "Clicks": "sum",
          "Opens": "sum"
          })
     harvest['CTR'] = (harvest["Clicks"]/harvest["Opens"])*100
     harvest['eCPM'] = harvest["Revenue"]*1000/harvest["Delivered"]
     harvest['Drops'] = df.groupby(["Hitpath Offer ID"]).size().reset_index(name='Drops')['Drops']

     
     cols = ["Hitpath Offer ID","Offer Name","RX Rep","Payout Type","Vertical", "Advertiser Name","Delayed Reporting","Operational Status"]
     
     market = harvest.merge(offers[cols],how='left', on='Hitpath Offer ID')
     return market

scyth = harvest_2(sdf)
hood = harvest_2(ldf)

grim = scyth.loc[(scyth["Payout Type"]!="CPM") & (scyth["eCPM"]<1) & (scyth["Opportunity Cost"]<-1000)]
reaper = hood.loc[(hood["Payout Type"]!="CPM") & (hood["eCPM"]<1) & (hood["Opportunity Cost"]<-2500)]

grim['Revenue'] = grim['Revenue'].map('${:,.2f}'.format)
grim['eCPM'] = grim['eCPM'].map('${:,.2f}'.format)
grim['Opportunity Cost'] = grim['Opportunity Cost'].map('${:,.2f}'.format)

reaper['Revenue'] = reaper['Revenue'].map('${:,.2f}'.format)
reaper['eCPM'] = reaper['eCPM'].map('${:,.2f}'.format)
reaper['Opportunity Cost'] = reaper['Opportunity Cost'].map('${:,.2f}'.format)

grim = grim[["Hitpath Offer ID", "Offer Name", "Revenue", "eCPM","Delivered", "Opportunity Cost", "CTR", "Drops", "Vertical", "RX Rep","Payout Type", "Advertiser Name","Delayed Reporting","Operational Status"]]
reaper = reaper[["Hitpath Offer ID", "Offer Name", "Revenue", "eCPM","Delivered", "Opportunity Cost", "CTR", "Drops", "Vertical", "RX Rep","Payout Type", "Advertiser Name","Delayed Reporting","Operational Status"]]


worth = ldf.groupby(["DP&Pub","Hitpath Offer ID"]).agg({
          "Revenue":"sum",
          "Delivered":"sum",
          "Opportunity Cost":"sum",
          "Clicks": "sum",
          "Opens": "sum"
          }).reset_index()
worth['CTR'] = (worth["Clicks"]/worth["Opens"])*100
worth['eCPM'] = worth["Revenue"]*1000/worth["Delivered"]

cols = ["Hitpath Offer ID","Vertical"]
worth = worth.merge(offers[cols], how='left', on='Hitpath Offer ID')

worth['Revenue'] = worth['Revenue'].map('${:,.2f}'.format)
worth['eCPM'] = worth['eCPM'].map('${:,.2f}'.format)
worth['Opportunity Cost'] = worth['Opportunity Cost'].map('${:,.2f}'.format)


souls = cobra[cobra["Hitpath Offer ID"].isin(
     list(set(grim["Hitpath Offer ID"].unique()).union(
          set(reaper["Hitpath Offer ID"].unique()))))][cobra["Date"]>=dt.datetime.today()]

souls = souls.merge(emit[["Dataset","Analyst","Emailer","DP&Pub"]],on='Dataset')
souls = souls.merge(worth, on=["DP&Pub","Hitpath Offer ID"],how="left").fillna("No drops in last 30 days.")
souls = souls[['Date', 'Dataset', 'Drop', 'Send Strategy','Campaign ID', 
               'Revenue', 'eCPM', 'Delivered', 'Opportunity Cost', 'CTR','Analyst', 'Emailer']]

souls["Offer ID"] = souls["Campaign ID"].apply(lambda x: x.split('-')[0])
offers_l = offers.dropna(subset=['Hitpath Offer ID'])
offers_l['Hitpath Offer ID'] = offers_l['Hitpath Offer ID'].astype(int)
offers_l['Hitpath Offer ID'] = offers_l['Hitpath Offer ID'].astype(str)
souls = souls.merge(offers_l, how='left', left_on = 'Offer ID', right_on = 'Hitpath Offer ID')
souls = souls[['Date', 'Dataset', 'Drop', 'Send Strategy','Campaign ID', 
               'Revenue', 'eCPM', 'Delivered', 'Opportunity Cost', 'CTR','Analyst', 'RX Rep','Emailer']]

with pd.ExcelWriter(f'/Users/leonarie/Desktop/Python/reaper/grimreaper_{max_date_str}.xlsx') as writer:
     grim.to_excel(writer,sheet_name="14-day & Over -1K Opp Cost",index=False,
                    # float_format=currency_format, columns=['Revenue', 'Opportunity Cost', 'eCPM']
                   )
     reaper.to_excel(writer,sheet_name="30-Day & Over -2.5K Opp Cost",index=False,
                      # float_format=currency_format, columns=['Revenue', 'Opportunity Cost', 'eCPM']
                     )
     souls.to_excel(writer,sheet_name="Upcoming Drops",index=False)
     
     
folder_path = "/Users/leonarie/Desktop/Python/reaper"

fromaddr = "schedulealartsrxmg@gmail.com"
# creates SMTP session
s = smtplib.SMTP('smtp.gmail.com', 587)

# start TLS for security
s.starttls()

# Authentication
s.login(fromaddr, "yyjetejtjhjhzxxk")

# Get all files in the folder
all_files = os.listdir(folder_path)

# Filter files with the prefix 'grimreaper_' and sort them based on modification time
grimreaper_files = sorted([f for f in all_files if f.startswith('grimreaper_')], key=lambda x: os.path.getmtime(os.path.join(folder_path, x)), reverse=True)

# Select the newest files
newest_files = grimreaper_files[:2]  # Assuming you need the two newest files


to_addresses = ["leon@rxmg.com", "offernotices@rxmg.com","elliot@rxmg.com", "jennifer@rxmg.com", "a.miller@rxmg.com", "f.erickson@rxmg.com", "k.smith@rxmg.com"]
#to_addresses = ["leon@rxmg.com"]
# Create email message
msg = MIMEMultipart()
msg['From'] = fromaddr
msg['To'] = ', '.join(to_addresses)  # Join multiple email addresses with commas
msg['Subject'] = "Grim Reaper! Please look at content and offers."

# Email body
body = "Please look at content and offers on the attached Excel files.\n\nBest,\nLeon Arie\n"
msg.attach(MIMEText(body, 'plain'))

# Attach both Excel files to the email
for filename in newest_files:
    filepath = os.path.join(folder_path, filename)
    attachment = open(filepath, "rb")
    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f"attachment; filename= {filename}")
    msg.attach(part)

# Convert message to string and send email
text = msg.as_string()
s.sendmail(fromaddr, to_addresses, text)

# terminating the session
s.quit()
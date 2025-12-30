from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
import pandas as pd
import requests, os, logging
from sqlalchemy import URL, create_engine, types, text, bindparam

# LOGIC_APP_URL = os.getenv("LOGIC_APP_URL")  # Set in Azure Function App settings

yesterday = (datetime.now() - BDay(1)).strftime('%Y%m%d')
# yesterday = '20251028'  # For testing purposes only

# Get FuturesSummary{yesterday}.xlsx from FuturesSummary folder
futures_summary_path = f'FuturesSummary/FuturesSummary{yesterday}.xlsx'
futures_summary_df = pd.read_excel(futures_summary_path, sheet_name='TotalFutures')
logging.info(futures_summary_df.head())

# Get the Options summary file for yesterday
options_summary_df = pd.read_excel(futures_summary_path, sheet_name='TotalOptions')
logging.info(options_summary_df.head())

# Group by Commodity and Contract, sum up NetExp
grouped_futures = futures_summary_df.groupby(['BrokerName', 'Contract', 'CommodityName'], as_index=False)['NetExp'].sum()

grouped_options = options_summary_df.groupby(['BrokerName', 'Contract', 'OptType', 'Strike', 'CommodityName'], as_index=False)['NetExp'].sum()

# Drop net epxosure 0
grouped_futures = grouped_futures[grouped_futures['NetExp'] != 0]
grouped_options = grouped_options[grouped_options['NetExp'] != 0]


commodity_symbol_dict = {
    'KC Wheat': ('KW', 50),                     # 5,000 bu * $0.01 = $50/pt
    'Canola': ('RS', 20),                       # CAD $20/pt
    'ICE Raw Sugar': ('SB', 1120),              # 112,000 lbs * $0.01 = $1,120/pt
    'LDN Sugar #5': ('CW', 50),               # $27.50/pt (metric ton contract)
    'Cotton': ('CT', 500),                      # 50,000 lbs * $0.01 = $500/pt
    'CBOT Corn': ('C', 50),                     # 5,000 bu * $0.01 = $50/pt
    'Arabica Coffee': ('KC', 375),              # 37,500 lbs * $0.01 = $375/pt
    'CBOT Soybeans': ('S', 50),                 # 5,000 bu * $0.01 = $50/pt
    'EURO FX': ('6E', 125000),                  # €125,000 per contract
    'Matif Wheat': ('EBM', 50),                 # €50/pt (50 tons)
    'ECP Futures': ('ECP', 1000),                 # assume 10 multiplier
    'CME EMINI S&P 500': ('ES', 50),            # $50 per index point
    'COMX GOLD': ('GC', 100),                   # 100 troy oz * $1 = $100/pt
    'MPLS Wheat': ('MW', 50),                   # 5,000 bu * $0.01 = $50/pt
    'CME EMINI NSDQ': ('NQ', 20),               # $20 per index point
    'ROBUSTA COFFEE': ('RC', 10),               # $10/pt (ICE Europe)
    'CBT SOUTH AMERICAN SOYBEANS': ('S', 50), # 5,000 bu * $0.01 = $50/pt
    'Soybean Meal': ('SM', 100),                # $100/pt (100 tons)
    'Chicago SRW Wheat': ('W', 50),             # 5,000 bu * $0.01 = $50/pt
    'Soybean Oil': ('BO', 600)                  # 60,000 lbs * $0.01 = $600/pt
}

grouped_futures['TSSymbol'] = grouped_futures['CommodityName'].map(lambda x: commodity_symbol_dict[x][0])
grouped_futures['Multiplier'] = grouped_futures['CommodityName'].map(lambda x: commodity_symbol_dict[x][1])
grouped_futures['TSContract'] = grouped_futures['TSSymbol'] + grouped_futures['Contract'].str[-3:]
unique_ts_contracts = grouped_futures['TSContract'].unique().tolist()


# Get the last prices from SQL database
url_object = URL.create(
"postgresql+psycopg2",
username="AgrocorpAdmin",
password="LAY7z-gkpLwNM:4",  # plain (unescaped) text
host="agrocorppricedb.postgres.database.azure.com",
database="agrocorpproddb",
)

engine = create_engine(url_object)

# Get yesterday in the form YYYY-MM-DD using yesterday
sqldate = yesterday[:4] + '-' + yesterday[4:6] + '-' + yesterday[6:]
# Convert list to tuple for SQL IN clause
# Convert to SQL-safe tuple string
contracts_tuple = tuple(unique_ts_contracts)

sql = f"""
SELECT *
FROM daily_futures
WHERE contract IN {contracts_tuple}
"""
df = pd.read_sql(sql=text(sql), con=engine)

# Sort by date descending and drop duplicates to get the latest price for each contract
df = df.sort_values(by='date', ascending=False).drop_duplicates(subset=['contract'], keep='first')


# Merge last prices into grouped_futures
merged_futures = pd.merge(grouped_futures, df[['close', 'date', 'contract']], left_on='TSContract', right_on='contract', how='left')

# Filter out rows where missing close
merged_futures = merged_futures[merged_futures['close'].notna()] ### NEED TO EVENTUALLY FIX THIS

# Filter out if netexp is 0
merged_futures = merged_futures[merged_futures['NetExp'] != 0]

grouped_options['TSSymbol'] = grouped_options['CommodityName'].map(lambda x: commodity_symbol_dict[x][0])
grouped_options['Multiplier'] = grouped_options['CommodityName'].map(lambda x: commodity_symbol_dict[x][1])
grouped_options['TSContract'] = grouped_options['TSSymbol'] + grouped_options['Contract'].str[-3:]
unique_ts_contracts = grouped_options['TSContract'].unique().tolist()

# # Get the last prices from SQL database
# url_object = URL.create(
# "postgresql+psycopg2",
# username="AgrocorpAdmin",
# password="LAY7z-gkpLwNM:4",  # plain (unescaped) text
# host="agrocorppricedb.postgres.database.azure.com",
# database="agrocorpproddb",
# )

# engine = create_engine(url_object)

# Get yesterday in the form YYYY-MM-DD using yesterday
sqldate = yesterday[:4] + '-' + yesterday[4:6] + '-' + yesterday[6:]
# Convert list to tuple for SQL IN clause
# Convert to SQL-safe tuple string
contracts_tuple = tuple(unique_ts_contracts)

sql = f"""
SELECT *
FROM daily_futures
WHERE contract IN {contracts_tuple}
"""
df = pd.read_sql(sql=text(sql), con=engine)

# Sort by date descending and drop duplicates to get the latest price for each contract
df = df.sort_values(by='date', ascending=False).drop_duplicates(subset=['contract'], keep='first')
logging.info(df.head())

brokers = merged_futures['BrokerName'].unique().tolist()
results = []
for broker in brokers:
    adverse_move = 0.05
    # Calculate the adverse scenario profit up and adverse scenario profit down
    broker_df = merged_futures[merged_futures['BrokerName'] == broker].copy()
    broker_df['AdverseProfitUp'] = broker_df['NetExp'] * broker_df['Multiplier'] * (broker_df['close'] * (1 + adverse_move) - broker_df['close'])
    broker_df['AdverseProfitDown'] = broker_df['NetExp'] * broker_df['Multiplier'] * (broker_df['close'] * (1 - adverse_move) - broker_df['close'])
    
    # Group it by CommodityName, then sum up AdverseProfitUp and AdverseProfitDown
    grouped_broker_df = broker_df.groupby('CommodityName', as_index=False).agg({
        'AdverseProfitUp': 'sum',
        'AdverseProfitDown': 'sum',
        'NetExp': 'sum',
    })

    # For each commodity tke the worst case scenario, and write the column which scenario is taken, so use CommodityName + "adverse_move up" or "adverse_move down"
    # Replace the words
    grouped_broker_df['WorstCase'] = grouped_broker_df[['AdverseProfitUp', 'AdverseProfitDown']].min(axis=1)
    grouped_broker_df['Scenario'] = grouped_broker_df.apply(lambda row: f"{row['CommodityName']} {adverse_move * 100}% up" if row['AdverseProfitUp'] < row['AdverseProfitDown'] else f"{row['CommodityName']} {adverse_move * 100}% down", axis=1)
    grouped_broker_df['BrokerName'] = broker
    results.append(grouped_broker_df)

final_results_df = pd.concat(results, ignore_index=True)
# Drop AdverseProfitUp and AdverseProfitDown columns
final_results_df = final_results_df.drop(columns=['AdverseProfitUp', 'AdverseProfitDown'])

# Group by BrokerName and keep WorstCase
broker_consolidated = final_results_df.groupby('BrokerName', as_index=False).agg({
    'WorstCase': 'sum',
})

# Create another copy of final_results_df called broker_commodity_breakdown_df and sort columns into BrokerName, CommodityName, NetExp, WorstCase, Scenario
broker_commodity_breakdown_df = final_results_df[['BrokerName', 'CommodityName', 'NetExp', 'WorstCase', 'Scenario']]
print(broker_commodity_breakdown_df)

# def send_via_logic_app(html_body):
#     payload = {
#         "subject": "Adverse Scenario on Futures Margin Call",
#         "html": html_body,
#         "to": ["richard.goh@agrocorp.com.sg"]
#     }

#     r = requests.post(LOGIC_APP_URL, json=payload)

#     if r.status_code not in (200, 202):
#         raise Exception(f"Logic App failed: {r.text}")

#     return "Email sent via Logic App"

# df1 = broker_consolidated.copy()
# df2 = broker_commodity_breakdown_df.copy()

# # Round Worst Case to 2 decimal places and add commas
# df1['WorstCase'] = df1['WorstCase'].round(2).map('{:,.2f}'.format)
# df2['WorstCase'] = df2['WorstCase'].round(2).map('{:,.2f}'.format)

# # --- Formatting: convert DataFrames to styled HTML tables ---
# def df_to_html(df, title):
#     styled = (
#         df.style
#         .set_caption(title)
#         .set_table_styles([{
#             'selector': 'caption',
#             'props': [('font-size', '14px'),
#                       ('font-weight', 'bold'),
#                       ('text-align', 'left'),
#                       ('margin-bottom', '8px')]
#         }])
#         .hide(axis='index')
#     )
#     return styled.to_html()

# html_table_1 = df_to_html(df1, "Consolidated by Broker")
# html_table_2 = df_to_html(df2, "Consolidated by Broker and Commodity")

# # --- Combine the tables into one HTML body ---
# html_body = f"""
# <html>
# <head>
# <style>
# body {{
#     font-family: Calibri, sans-serif;
#     font-size: 12pt;
# }}
# table {{
#     border-collapse: collapse;
#     margin-bottom: 20px;
# }}
# th, td {{
#     border: 1px solid #999;
#     padding: 6px 10px;
#     text-align: right;
# }}
# th {{
#     background-color: #004080;
#     color: white;
# }}
# caption {{
#     text-align: left;
#     font-weight: bold;
#     font-size: 14px;
#     margin-bottom: 5px;
# }}

# </style>
# </head>
# <body style="font-family: Calibri, sans-serif; font-size: 12pt; color: #333;">
# <p>Hi team,</p>

# <p>
# Please find the latest summary below. The <strong>worst-case scenarios</strong> are calculated using our 
# <strong>futures positions only</strong> to estimate potential margin call movements, as 
# <strong>options PnL</strong> cannot be used to offset futures losses.
# </p>

# <p>
# For each contract, yesterday’s settlement price is taken as the base, and an 
# <strong>adverse 5% move up</strong> and <strong>5% move down</strong> is applied to compute 
# the resulting profit or loss. The total PnL by commodity is then grouped together, and the 
# <strong>worst-case scenario</strong> for that commodity is selected.
# </p>

# <p>
# In other words, we make the simplifying assumption that contracts within the same commodity 
# will move directionally together (i.e., not one +5% and another −5% simultaneously).
# </p>

# {html_table_1}
# {html_table_2}

# <p>Best regards,<br>Richard</p>
# </body>
# </html>

# """

# send_via_logic_app(html_body)




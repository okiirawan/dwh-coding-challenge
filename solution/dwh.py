#!/usr/bin/env python3

import pandas as pd
import os,json
import numpy as np

path_to_json = '/data/'
#json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
 # an empty list to store the data frames

#dfs = {}
frames = {}
#for index, js in enumerate(json_files):
for dirpath, dirs, files in os.walk(path_to_json):
  dfs = []
  subdir = os.path.basename(dirpath)
  for filename in files:
    if filename.endswith('.json'):
      with open(os.path.join(dirpath, filename)) as json_file:
        json_data = pd.json_normalize(json.loads(json_file.read()))\
        .rename(columns=lambda x: x.replace('data.', '').replace('set.', ''))
      dfs.append(json_data)
    df = pd.concat(dfs).sort_values('ts').reset_index(drop=True)
    frames[subdir] = df.fillna(method='ffill').replace('', np.nan)
#print(frames[dirpath])
for key in frames:
  print("Dataframe from: "+key)
  print (frames[key])
  print ("\n")

#and ('ts == df_account.max()['ts']')
df_account = frames['accounts']
df_account.to_csv(r'/result/account.txt', index=None, sep='\t', mode='a')

df_saving = frames['savings_accounts']
df_saving.to_csv(r'/result/saving.txt', index=None, sep='\t', mode='a')

df_card = frames['cards']
df_card.to_csv(r'/result/card.txt', index=None, sep='\t', mode='a')


pdList = [df_account[['ts','account_id','card_id','savings_account_id']], df_saving[['ts','savings_account_id']], df_card[['ts','card_id']]]
df_time = pd.concat(pdList).sort_values('ts').drop_duplicates(['ts']).reset_index(drop=True)

#result = frames['accounts'].groupby('account_id')[['card_id','address','email','account_id','name','phone_number','savings_account_id']]
result_acc = pd.merge_asof(df_time,df_account,
                 on='ts',
                 by=['ts','account_id','card_id','savings_account_id'])

result_acc_card = pd.merge_asof(result_acc,df_card,
                 on='ts',
                 by=['ts','card_id'])
result_acc_card_saving = pd.merge_asof(result_acc_card,df_saving,
                 on='ts',
                 by=['ts','savings_account_id'])
result_acc_card_saving['credit_used'] = result_acc_card_saving['credit_used'].fillna(0)

#print('\n', result_acc)
#print('\n', result_acc_card)
print('\n', result_acc_card_saving[['ts','account_id','card_id','savings_account_id','address','email','name','phone_number','credit_used','card_number','monthly_limit','status_x','interest_rate_percent','balance','status_y']].fillna(method='ffill').replace('', np.nan).rename({'status_x':'card_status', 'status_y':'saving_account_status'}, axis='columns'))

result_acc_card_saving[['ts','account_id','card_id','savings_account_id','address','email','name','phone_number','credit_used','card_number','monthly_limit','status_x','interest_rate_percent','balance','status_y']].fillna(method='ffill').replace('', np.nan).rename({'status_x':'card_status', 'status_y':'saving_account_status'}, axis='columns').to_csv(r'/result/denormalize.txt', index=None, sep='\t', mode='a')

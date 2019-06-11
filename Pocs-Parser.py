import sys
import pandas as pd
 
df=pd.read_csv('POCS_arin_db_2-20-2019_unparsed.txt',error_bad_lines=False, header = None)
df.columns = ['result']
df_out = df['result'].str.split(': ', expand=True)
df_out = df_out[[0,1]]
df_out = df_out.set_index([0,(df_out[0] == 'POCHandle').cumsum().rename('row')])
df_out = df_out.set_index(df_out.groupby([0,'row']).cumcount(), append=True)
df_out = df_out.reset_index('row')
df_out.index = df_out.index.map('{0[0]}_{0[1]}'.format)
df_out = df_out.set_index(['row'], append=True)[1].unstack(0)
df_out = df_out.rename(columns=lambda x: x.split('_0')[0]).reset_index()
df_out = df_out[['POCHandle','IsRole','LastName','FirstName','Street','City','State/Prov','Country','PostalCode','RegDate','Updated','OfficePhone','Mailbox','Source']]
df_out = df_out.applymap(lambda x: x.strip() if isinstance(x, str) else x)
df_out.to_csv('POCS_arin_db_2-20-2019_parsed.csv', header=True, index=False)

import re
import pymssql
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from bcpp_export.private_settings import LisCredentials
from tshepo.modification_reasons import modification_reasons
from tshepo.dose_status import dose_status

engine = create_engine('mssql+pymssql://{user}:{passwd}@{host}:{port}/{db}'.format(
    user=LisCredentials.user, passwd=LisCredentials.password,
    host=LisCredentials.host, port=LisCredentials.port,
    db=LisCredentials.name))


sql = 'select * from bhpdmc.dbo.PH100Response'
with engine.connect() as conn, conn.begin():
    df_meds = pd.read_sql_query(sql, conn)

sql = 'select * from bhp007.dbo.ts003Dict order by DICTTYPE, SHOWORDER'
with engine.connect() as conn, conn.begin():
    df_dict = pd.read_sql_query(sql, conn)

sql = 'select * from bhp007.dbo.ts003response as ts003 left join bhp007.dbo.TS003ResponseQ001X0 as ts003d on ts003d.QID1X0 = ts003.Q001X0'
with engine.connect() as conn, conn.begin():
    df_data = pd.read_sql_query(sql, conn)

df_dose_status = pd.DataFrame(dose_status.items(), columns=['code', 'status']).sort_values('code')


df_reasons = pd.DataFrame(modification_reasons.items(), columns=['code', 'reason'])

df_data = pd.merge(df_data, df_dose_status, left_on='Q001A2', right_on='code', how='left')
df_data = pd.merge(df_data, df_reasons, left_on='rx_modification_reason', right_on='code', how='left')

# on
df_on = df_data.query('Q002X0 == 1 and rx_dose_status == \'Initial dose\'')[['PID', 'VisitID', 'rx_code', 'rx_modification_date']]
gb_on = df_on[['PID', 'VisitID', 'rx_code', 'rx_modification_date']].groupby(['PID', 'rx_modification_date'])
rx_on = gb_on['rx_code'].unique()
df_on = pd.DataFrame({'rx_on': rx_on}).reset_index()

# held
df_held = df_data.query('Q002X0 == 1 and rx_dose_status == \'Temporarily held\'')[['PID', 'VisitID', 'rx_code', 'rx_modification_date']]
gb_held = df_held[['PID', 'VisitID', 'rx_code', 'rx_modification_date']].groupby(['PID', 'rx_modification_date'])
rx_held = gb_held['rx_code'].unique()
df_held = pd.DataFrame({'rx_held': rx_held}).reset_index()

# resumed
df_resumed = df_data[df_data['Q002X0'] == 1 & df_data['rx_dose_status'].str.contains('Resumed')][['PID', 'VisitID', 'rx_code', 'rx_modification_date']]
gb_resumed = df_resumed[['PID', 'VisitID', 'rx_code', 'rx_modification_date']].groupby(['PID', 'rx_modification_date'])
rx_resumed = gb_resumed['rx_code'].unique()
df_resumed = pd.DataFrame({'rx_resumed': rx_resumed}).reset_index()

# discontinued
df_discontinued = df_data.query('Q002X0 == 1 and rx_dose_status == \'Discontinued\' and rx_modification_reason != \'Completion of Study follow-up\'')[['PID', 'VisitID', 'rx_code', 'rx_modification_date']]
gb_discontinued = df_discontinued[['PID', 'VisitID', 'rx_code', 'rx_modification_date']].groupby(['PID', 'rx_modification_date'])
rx_discontinued = gb_resumed['rx_code'].unique()
df_discontinued = pd.DataFrame({'rx_discontinued': rx_discontinued}).reset_index()

# off
df_off = df_data.query('Q002X0 == 1 and rx_dose_status == \'Discontinued\' and rx_modification_reason == \'Completion of Study follow-up\'')[['PID', 'VisitID', 'rx_code', 'rx_modification_date']]
gb_off = df_off[['PID', 'VisitID', 'rx_code', 'rx_modification_date']].groupby(['PID', 'rx_modification_date'])
rx_off = gb_off['rx_code'].unique()
df_off = pd.DataFrame({'rx_off': rx_off}).reset_index()

# merge all
df = pd.merge(df_on, df_held, on=['PID', 'rx_modification_date'], how='outer')
df = pd.merge(df, df_discontinued, on=['PID', 'rx_modification_date'], how='outer')
df = pd.merge(df, df_resumed, on=['PID', 'rx_modification_date'], how='outer')
df = pd.merge(df, df_off, on=['PID', 'rx_modification_date'], how='outer')

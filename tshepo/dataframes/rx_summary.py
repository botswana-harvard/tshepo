import pandas as pd

from .ts003 import ts003 as df_data

# on treatment
df_on = df_data.query('Q002X0 == 1 and rx_dose_status == \'Initial dose\'')[['PID', 'VisitID', 'rx_code', 'rx_modification_date']]
gb_on = df_on[['PID', 'VisitID', 'rx_code', 'rx_modification_date']].groupby(['PID', 'rx_modification_date'])
rx_on = gb_on['rx_code'].unique()
df_on = pd.DataFrame({'rx_on': rx_on}).reset_index()

# held treatment
df_held = df_data.query('Q002X0 == 1 and rx_dose_status == \'Temporarily held\'')[['PID', 'VisitID', 'rx_code', 'rx_modification_date']]
gb_held = df_held[['PID', 'VisitID', 'rx_code', 'rx_modification_date']].groupby(['PID', 'rx_modification_date'])
rx_held = gb_held['rx_code'].unique()
df_held = pd.DataFrame({'rx_held': rx_held}).reset_index()

# resumed treatment
df_resumed = df_data[df_data['Q002X0'] == 1 & df_data['rx_dose_status'].str.contains('Resumed')][['PID', 'VisitID', 'rx_code', 'rx_modification_date']]
gb_resumed = df_resumed[['PID', 'VisitID', 'rx_code', 'rx_modification_date']].groupby(['PID', 'rx_modification_date'])
rx_resumed = gb_resumed['rx_code'].unique()
df_resumed = pd.DataFrame({'rx_resumed': rx_resumed}).reset_index()

# discontinued treatment
df_discontinued = df_data.query('Q002X0 == 1 and rx_dose_status == \'Discontinued\' and rx_modification_reason != \'Completion of Study follow-up\'')[['PID', 'VisitID', 'rx_code', 'rx_modification_date']]
gb_discontinued = df_discontinued[['PID', 'VisitID', 'rx_code', 'rx_modification_date']].groupby(['PID', 'rx_modification_date'])
rx_discontinued = gb_discontinued['rx_code'].unique()
df_discontinued = pd.DataFrame({'rx_discontinued': rx_discontinued}).reset_index()

# off treatment
df_off = df_data.query('Q002X0 == 1 and rx_dose_status == \'Discontinued\' and rx_modification_reason == \'Completion of Study follow-up\'')[['PID', 'VisitID', 'rx_code', 'rx_modification_date']]
gb_off = df_off[['PID', 'VisitID', 'rx_code', 'rx_modification_date']].groupby(['PID', 'rx_modification_date'])
rx_off = gb_off['rx_code'].unique()
df_off = pd.DataFrame({'rx_off': rx_off}).reset_index()

# merge all
df = pd.merge(df_on, df_held, on=['PID', 'rx_modification_date'], how='outer')
df = pd.merge(df, df_discontinued, on=['PID', 'rx_modification_date'], how='outer')
df = pd.merge(df, df_resumed, on=['PID', 'rx_modification_date'], how='outer')
df = pd.merge(df, df_off, on=['PID', 'rx_modification_date'], how='outer')

rx_summary = df

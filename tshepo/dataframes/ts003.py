import pymssql
import pandas as pd
from sqlalchemy import create_engine

from bcpp_export.private_settings import LisCredentials
from tshepo.modification_reasons import modification_reasons
from tshepo.dose_status import dose_status

engine = create_engine('mssql+pymssql://{user}:{passwd}@{host}:{port}/{db}'.format(
    user=LisCredentials.user, passwd=LisCredentials.password,
    host=LisCredentials.host, port=LisCredentials.port,
    db=LisCredentials.name))

sql = 'select * from bhp007.dbo.ts003response as ts003 left join bhp007.dbo.TS003ResponseQ001X0 as ts003d on ts003d.QID1X0 = ts003.Q001X0'
with engine.connect() as conn, conn.begin():
    df_data = pd.read_sql_query(sql, conn)

df_dose_status = pd.DataFrame(dose_status.items(), columns=['code', 'status']).sort_values('code')
df_reasons = pd.DataFrame(modification_reasons.items(), columns=['code', 'reason'])
df_data = pd.merge(df_data, df_dose_status, left_on='Q001A2', right_on='code', how='left')
df_data = pd.merge(df_data, df_reasons, left_on='rx_modification_reason', right_on='code', how='left')

ts003 = df_data

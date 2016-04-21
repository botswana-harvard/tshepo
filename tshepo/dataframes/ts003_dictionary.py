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


sql = 'select * from bhp007.dbo.ts003Dict order by DICTTYPE, SHOWORDER'
with engine.connect() as conn, conn.begin():
    df_dict = pd.read_sql_query(sql, conn)

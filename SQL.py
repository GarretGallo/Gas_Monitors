from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date, DECIMAL, text
from sqlalchemy.orm import declarative_base

from Gas_Monitors import monitor_locations, daily_df, weekly_df, monthly_df, df_percentages, combined_stats

Base = declarative_base()

#Database Credentials
USERNAME = 'root'
PASSWORD = 'GratCode1122'
HOST = '127.0.0.3:3306'
DATABASE = 'gas_monitor_data'

engine = create_engine(f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE}")

#Create SQL Tables
class Monitors(Base):
    __tablename__ = 'monitor_locations'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    Monitor = Column(String(50), nullable=False)
    Location = Column(String(50), nullable=False)

class Daily_Data(Base):
    __tablename__ = 'daily_h2s'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    Date = Column(Date, nullable=False)
    Monitor = Column(Integer, ForeignKey('monitor_locations.ID'))
    Average = Column(DECIMAL(10,2), nullable=False)
    Percentile_75th = Column(DECIMAL(10,2), nullable=False)
    Max = Column(DECIMAL(10,2), nullable=False)

class Weekly_Data(Base):
    __tablename__ = 'weekly_h2s'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    Date = Column(Date, nullable=False)
    Monitor = Column(Integer, ForeignKey('monitor_locations.ID'))
    Average = Column(DECIMAL(10,2), nullable=False)
    Percentile_75th = Column(DECIMAL(10,2), nullable=False)
    Max = Column(DECIMAL(10,2), nullable=False)

class Monthly_Data(Base):
    __tablename__ = 'monthly_h2s'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    Date = Column(Date, nullable=False)
    Monitor = Column(Integer, ForeignKey('monitor_locations.ID'))
    Average = Column(DECIMAL(10,2), nullable=False)
    Percentile_75th = Column(DECIMAL(10,2), nullable=False)
    Max = Column(DECIMAL(10,2), nullable=False)

class Data_Ranges(Base):
    __tablename__ = 'safety_ranges'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    Monitor = Column(Integer, ForeignKey('monitor_locations.ID'))
    Safe = Column(DECIMAL(10,2), nullable=False)
    Detectable = Column(DECIMAL(10,2), nullable=False)
    Hazardous = Column(DECIMAL(10,2), nullable=False)

class Overview(Base):
    __tablename__ = 'overview_stats'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    monitor = Column(String(50), nullable=False)
    count = Column(DECIMAL(10,2), nullable=False)
    mean = Column(DECIMAL(10,2), nullable=False)
    stand_dev = Column(DECIMAL(10,2), nullable=False)
    min = Column(DECIMAL(10,2), nullable=False)
    q1 = Column(DECIMAL(10,2), nullable=False)
    median = Column(DECIMAL(10,2), nullable=False)
    q3 = Column(DECIMAL(10,2), nullable=False)
    max = Column(DECIMAL(10,2), nullable=False)

#Upload Data
Base.metadata.create_all(engine)

with engine.begin() as conn:
    conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
    conn.execute(text("TRUNCATE TABLE daily_h2s"))
    conn.execute(text("TRUNCATE TABLE weekly_h2s"))
    conn.execute(text("TRUNCATE TABLE monthly_h2s"))
    conn.execute(text("TRUNCATE TABLE safety_ranges"))
    conn.execute(text("TRUNCATE TABLE overview_stats"))
    conn.execute(text("TRUNCATE TABLE monitor_locations"))
    conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))

monitor_locations.to_sql('monitor_locations', con=engine, index=False, if_exists='append')
daily_df.to_sql('daily_h2s', con=engine, index=False, if_exists='append')
weekly_df.to_sql('weekly_h2s', con=engine, index=False, if_exists='append')
monthly_df.to_sql('monthly_h2s', con=engine, index=False, if_exists='append')
df_percentages.to_sql('safety_ranges', con=engine, index=False, if_exists='append')
combined_stats.to_sql('overview_stats', con=engine, index=False, if_exists='append')
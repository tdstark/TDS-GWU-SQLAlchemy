#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import pandas as pd

from flask import Flask, jsonify


# In[2]:


# Database Setup
engine = create_engine("sqlite:///hawaii.sqlite")
# conn = engine.connect()

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the tables
measurement = Base.classes.measurement
station = Base.classes.station

#create session
session = Session(engine)

#Flask Setup
app = Flask(__name__)


# In[3]:


# Flask Routes
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end<br/>"
    )


# In[4]:


@app.route("/api/v1.0/precipitation")
def precipitation():
    """Convert the query results to a dictionary using `date` as the key and `prcp` as the value."""
    """Return the JSON representation of your dictionary."""
    
    precip_query = session.query(measurement.date, measurement.prcp).order_by(measurement.date).all()

    session.close()

    return jsonify(dict(precip_query))


# In[5]:


@app.route("/api/v1.0/stations")
def stations():
    
    """Return a JSON list of stations from the dataset."""

    station_results = list(session.query(station.id,
                                    station.station,
                                    station.name,
                                    station.latitude,
                                    station.latitude,
                                    station.longitude,
                                    station.elevation))
    station_results = pd.DataFrame(station_results)
    station_results = station_results.to_dict(orient='records')
    session.close()

    return jsonify(station_results)


# In[6]:


@app.route("/api/v1.0/tobs")
def tobs():
    """Query the dates and temperature observations of the most active station for the last year of data."""
    """Return a JSON list of temperature observations (TOBS) for the previous year."""

    active_stations = session.query(measurement.station, func.count(measurement.station)).\
                                   group_by(measurement.station).\
                                   order_by(func.count(measurement.station).desc())
    
    act_station_12mos = list(session.query(measurement.date, measurement.tobs).order_by(measurement.date).filter(measurement.station == active_stations[0][0]))
    act_station_12mos = pd.DataFrame(act_station_12mos)
    act_station_12mos['date'] = pd.to_datetime(act_station_12mos['date'])
    act_station_12mos = act_station_12mos.sort_values(['date'], ascending=False)
    act_station_12mos = act_station_12mos.dropna()
    max_date = act_station_12mos['date'].max()
    one_yr_prior = max_date - pd.offsets.DateOffset(years=1)
    act_station_12mos = act_station_12mos.loc[act_station_12mos['date'] >= one_yr_prior,]
    act_station_12mos['date'] = act_station_12mos['date'].astype(str)
    act_station_12mos = act_station_12mos.set_index('date')
    
    session.close()

    act_station_12mos = act_station_12mos.to_dict()

    return jsonify(act_station_12mos)


# In[7]:


@app.route("/api/v1.0/<start>")
def start(start):
    
    """Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range."""
    """When given the start only, calculate `TMIN`, `TAVG`, and `TMAX` for all dates greater than and equal to the start date."""
   
    all_query = list(session.query(measurement.date, measurement.tobs).order_by(measurement.date))
    query_df = pd.DataFrame(all_query)
    query_df['date'] = pd.to_datetime(query_df['date'])
    query_df = query_df.sort_values(['date'], ascending=False)
    query_df = query_df.dropna()
    min_date = start
    query_df = query_df.loc[query_df['date'] >= min_date,]
    query_df['date'] = query_df['date'].astype(str)
    query_df = query_df.groupby(['date'])
    tmin_df = query_df.min()
    tavg_df = query_df.mean()
    tmax_df = query_df.max()
    query_df = pd.merge(tmin_df, tavg_df, how='left', left_index=True, right_index=True)
    query_df = pd.merge(query_df, tmax_df, how='left', left_index=True, right_index=True)
    query_df = query_df.rename(columns={'tobs':'tobs_max', 
                                        'tobs_x': 'tobs_min',
                                        'tobs_y': 'tobs_avg'})
    
    session.close()
    
    query_df = query_df.to_dict()
    
    return jsonify([query_df])


# In[8]:


@app.route("/api/v1.0/<start>/<end>")
def startend(start, end):

    """Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range."""
    """When given the start and the end date, calculate the `TMIN`, `TAVG`, and `TMAX` for dates between the start and end date inclusive."""
    
    all_query = list(session.query(measurement.date, measurement.tobs).order_by(measurement.date))
    query_df = pd.DataFrame(all_query)
    query_df['date'] = pd.to_datetime(query_df['date'])
    query_df = query_df.sort_values(['date'], ascending=False)
    query_df = query_df.dropna()
    min_date = start
    max_date = end
    query_df = query_df.loc[(query_df['date'] >= min_date) & (query_df['date'] <= max_date),]
    query_df['date'] = query_df['date'].astype(str)
    query_df = query_df.groupby(['date'])
    tmin_df = query_df.min()
    tavg_df = query_df.mean()
    tmax_df = query_df.max()
    query_df = pd.merge(tmin_df, tavg_df, how='left', left_index=True, right_index=True)
    query_df = pd.merge(query_df, tmax_df, how='left', left_index=True, right_index=True)
    query_df = query_df.rename(columns={'tobs':'tobs_max', 
                                        'tobs_x': 'tobs_min',
                                        'tobs_y': 'tobs_avg'})
    
    session.close()
    
    query_df = query_df.to_dict()
    
    return jsonify([query_df])


# In[9]:


if __name__ == '__main__':
    app.run(debug=True)


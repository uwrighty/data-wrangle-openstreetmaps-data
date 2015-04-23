# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 16:55:33 2015

@author: Mark Wright

Connect to the mongo database to visualise some of the data
"""

import pandas as pd
from ggplot import *

def visualise(data, title, x, y):
    print data['result']
    df = pd.DataFrame(data['result'])
    print df.sort().plot(kind='bar', x='_id', y='count').set_title(title)
    #plot = ggplot(df, aes(x='_id', weight='count')) + geom_bar() + ggtitle(title) + scale_x_discrete(labels=None)
    #print plot 
    
    #print df

def get_place_of_worship(db):
    return db.osm.birmingham.aggregate([
        {"$match" : { "amenity" : "place_of_worship", "religion" : {"$exists" : 1}}},
        {"$group" : { "_id": "$religion", "count": { "$sum" : 1} }}])
        
def get_land_use_summary(db):
    return db.osm.birmingham.aggregate([
        {"$match" : { "land_sum" : {"$exists" : 1}}},
        {"$group" : { "_id": "$land_sum", "count": { "$sum" : 1} }}])
        
def get_user_summary(db):
    return db.osm.birmingham.aggregate([
        {'$group' : { "_id" : "$created.user", "count" : {"$sum" : 1}}},
        {"$sort" : {"count" : -1}},
        {"$limit" : 50}])        

def get_db():
    # For local use
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    # 'examples' here is the database name. It will be created if it does not exist.
    db = client.users
    return db
    

if __name__ == "__main__":
    # For local use
    db = get_db()
    #data = get_land_use_summary(db)
    #data = get_place_of_worship(db)
    data = get_user_summary(db)
    visualise(data, "Edits by user", "Username", "Count")
    #visualise(data, "Cultural Diversity based on place of worship", "Religion", "Count of places of worship")



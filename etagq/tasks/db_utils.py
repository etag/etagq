from celeryconfig import DB_USERNAME, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT
import celeryconfig

from datetime import datetime
import logging
import pytz
import pandas as pd

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, text
PG_DB = {
    'drivername': 'postgres',
    'username': DB_USERNAME,
    'password': DB_PASSWORD,
    'database': DB_NAME,
    'host': DB_HOST,
    'port': DB_PORT
    }

Base = automap_base()
engine = create_engine(URL(**PG_DB))
Base.prepare(engine, reflect=True)

AnimalHitReader = Base.classes.animal_hit_reader
Readers = Base.classes.readers
Tags = Base.classes.tags
Animals = Base.classes.animals
TagOwner = Base.classes.tag_owner
TagReads = Base.classes.tag_reads
TaggedAnimal = Base.classes.tagged_animal
Locations = Base.classes.locations
ReaderLocation = Base.classes.reader_location


def load_tagreads(df, user_id):
    df.TIMESTAMP = pd.to_datetime(df.TIMESTAMP)
    session = Session(engine)
    # Make sure the readers exist in the readers table - add if missing
    provided_reader_ids = set(df['UUID'].tolist())
    existing_readers = [r.reader_id for r in session.query(Readers).filter(Readers.reader_id.in_(provided_reader_ids)) if r]
    non_existing_readers = [r_id for r_id in provided_reader_ids if r_id not in existing_readers]
    for reader_id in non_existing_readers:
        session.add(Readers(reader_id=reader_id, user_id=user_id, description="System Added - please update description"))
    
    # Make sure the tags exist in the tags table - add if missing
    provided_tag_ids = set(df['TAG_ID'].tolist())
    existing_tags = [t.tag_id for t in session.query(Tags).filter(Tags.tag_id.in_(provided_tag_ids)) if t]
    non_existing_tags = [t_id for t_id in provided_tag_ids if t_id not in existing_tags]
    for tag_id in non_existing_tags:
        session.add(Tags(tag_id=tag_id, description="System Added - please update description"))
        session.add(TagOwner(tag_id=tag_id, user_id=user_id, start_time=datetime.now(pytz.utc)))
    
    #FIXME: Should tag reads be unique by reader_id, tag_id, and timestamp?
    #Currently does not prevent duplicates - may need to update database schema
    for record in df.to_dict(orient="record"):
        session.add(
           TagReads(
               tag_id=record['TAG_ID'],
               user_id=user_id,
               reader_id=record['UUID'],
               tag_read_time=record['TIMESTAMP'],
               public=False
           )
       )
    try:
        session.commit()
        loaded = True
        print("loaded {0}".format(len(df)))
    except SQLAlchemyError as e:
        print(e.message)
        loaded = False
        session.rollback()
    finally:
        session.close()
    return loaded


def load_locations(df, user_id):
    df.STARTDATE = pd.to_datetime(df.STARTDATE, utc=True)
    df.ENDDATE = pd.to_datetime(df.ENDDATE, utc=True)
    session = Session(engine)
    
    # Add and update readers
    provided_reader_ids = set(df['UUID'].tolist())
    existing_readers = session.query(Readers).filter(Readers.reader_id.in_(provided_reader_ids))
    existing_reader_ids = [r.reader_id for r in existing_readers]
    non_existing_reader_ids = [r_id for r_id in provided_reader_ids if r_id not in existing_reader_ids]
    for reader in existing_readers:
        # Only update if this reader is "owned" by this user
        if reader.user_id == user_id:
            reader.description = df[df['UUID'] == reader.reader_id].NAME.values[0]
    for reader_id in non_existing_reader_ids:
        session.add(Readers(reader_id=reader_id, user_id=user_id, description=df[df['UUID'] == reader_id].NAME.values[0]))
    
    # Add and update reader_locations
    existing_reader_locations = session.query(ReaderLocation).filter(ReaderLocation.reader_id.in_(provided_reader_ids))
    existing_reader_location_ids = [rl.reader_id for rl in existing_reader_locations]
    non_existing_reader_location_ids = [rl_r_id for rl_r_id in provided_reader_ids if rl_r_id not in existing_reader_location_ids]
    # Add new location and reader_location records
    for reader_id in non_existing_reader_location_ids:
        for record in df[df['UUID'] == reader_id].to_dict(orient="record"):
            existing_locations = session.query(Locations).filter(
                Locations.name == record['DESCRIPTION'],
                Locations.latitude == record['LATITUDE'],
                Locations.longitude == record['LONGITUDE']
            )
            if existing_locations.first():
                location = existing_locations.first()  # use first match
            else:
                location = Locations(name=record['DESCRIPTION'],
                                     latitude=record['LATITUDE'],
                                     longitude=record['LONGITUDE'],
                                     active=True)  # TODO: if ENDDATE set and it is in the past, set to false
                session.add(location)
                session.flush()
            reader_location = ReaderLocation(reader_id=reader_id,
                                             location_id=location.location_id,
                                             start_timestamp=record['STARTDATE'],
                                             end_timestamp=record['ENDDATE'])
            session.add(reader_location)
    
    try:
        session.commit()
        loaded = True
    except SQLAlchemyError as e:
        print(e.message)
        session.rollback()
        loaded = False
    finally:
        session.close()
    return loaded

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


def export_tagreads(location, user_id):
    """
    Exports tag read data to csv file
    """
    session = Session(engine)
    records = session.query(TagReads).filter(
        TagOwner.user_id == user_id,
        Tags.tag_id == TagOwner.tag_id,
        TagReads.tag_id == Tags.tag_id
    )

    df =  pd.read_sql(records.statement, session.bind)[
        ['reader_id', 'tag_id', 'tag_read_time']
    ]
    df.columns = ['uuid', 'tag_id', 'timestamp']
    try:
        df.to_csv(location, index=False)
        return True
    except IOError as e:
        print("Error creating CSV file for export")
        print(e)
        return False


def export_locations(location, user_id):
    """
    Exports location data to csv file
    """
    session = Session(engine)
    records = session.query(
        Readers.reader_id,
        Readers.description,
        ReaderLocation.start_timestamp,
        ReaderLocation.end_timestamp,
        Locations.latitude,
        Locations.longitude,
        Locations.name
    ).filter(
        Readers.reader_id == ReaderLocation.reader_id,
        ReaderLocation.location_id == Locations.location_id,
        Readers.user_id == user_id
    )

    df = pd.read_sql(records.statement, session.bind)[
        # These are the column names from the database
        ['reader_id', 'description', 'start_timestamp', 'end_timestamp', 'latitude', 'longitude', 'name']
    ]
    # Translate these back to the columns used by the ingester
    df.columns = ['uuid', 'name', 'startdate', 'enddate', 'latitude', 'longitude', 'description']
    try:
        df.to_csv(location, index=False)
        return True
    except IOError as e:
        print("Error creating CSV file for export")
        print(e)
        return False


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
    # update existing records    
    provided_reader_ids = set(df['UUID'].tolist())
    existing_records = session.query(Readers, ReaderLocation, Locations).filter(
        Readers.reader_id.in_(provided_reader_ids),
        Readers.reader_id == ReaderLocation.reader_id,
        ReaderLocation.location_id == Locations.location_id,
        Readers.user_id == user_id
    )
    for record in existing_records:
        df_record = df[df['UUID'] == record.readers.reader_id].to_dict(orient='record')[0]
        # FIXME: This always updates, modify to update on changes only - check session.dirty to confirm
        record.readers.description = df_record['NAME']
        record.locations.name = df_record['DESCRIPTION']
        record.locations.latitude = df_record['LATITUDE']
        record.locations.longitude = df_record['LONGITUDE']
        record.locations.active = True if not df_record['ENDDATE'] else df_record['ENDDATE'].tz_convert(None) > datetime.utcnow()
        record.reader_location.start_timestamp = df_record['STARTDATE'] if df_record['STARTDATE'] is not pd.NaT else None
        record.reader_location.end_timestamp = df_record['ENDDATE'] if df_record['ENDDATE'] is not pd.NaT else None
    # add new records
    try:
        existing_reader_ids = set([record.readers.reader_id for record in existing_records])
        # FIXME: confirm new_reader_ids are owned by this user
        new_reader_ids = provided_reader_ids - existing_reader_ids
        for record in df[df['UUID'].isin(new_reader_ids)].to_dict(orient='record'):
            reader = Readers(
                reader_id=record['UUID'],
                user_id=user_id,
                description=record['NAME']
            )
            # TODO: decide if 1:1 reader to location is okay - otherwise, change to remove duplicated locations
            location = Locations(
                name=record['DESCRIPTION'],
                latitude=record['LATITUDE'],
                longitude=record['LONGITUDE'],
                active=True if not record['ENDDATE'] else record['ENDDATE'].tz_convert(None) > datetime.utcnow()
            )
            readerlocation = ReaderLocation(
                start_timestamp=record['STARTDATE'] if record['STARTDATE'] is not pd.NaT else None,
                end_timestamp=record['ENDDATE'] if record['ENDDATE'] is not pd.NaT else None
            )
            readerlocation.readers = reader
            readerlocation.locations = location
            session.add(readerlocation)
            # FIXME: issue with duplicating first 3 location_ids after initial loading of db schema
            session.flush()
        session.commit()
        loaded = True
    except SQLAlchemyError as e:
        print(e.message)
        session.rollback()
        loaded = False
    finally:
        session.close()
    return loaded

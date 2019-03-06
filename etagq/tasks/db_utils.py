from celeryconfig import DB_USERNAME, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT
import celeryconfig

from datetime import datetime
import logging
import pytz

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
    except SQLAlchemyError as e:
        print(e.msg)
        loaded = False
    finally:
        session.close()
    return loaded

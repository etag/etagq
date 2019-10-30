from celeryconfig import DB_USERNAME, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT
import celeryconfig

from datetime import datetime
from numpy import nan
import logging
import pytz
import pandas as pd

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, text
from json import loads, dumps

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

    # ensure that a connection to the db is established
    global engine
    try:
        engine.execute("select 1")
    except OperationalError:
        engine = create_engine(URL(**PG_DB))

    reserved_fields = [
        "TAG_ID",
        "UUID",  # This is the reader_id
        "TIMESTAMP"
    ]

    errors = []
    # TODO: Should the timestamp be forced to UTC?
    df.TIMESTAMP = pd.to_datetime(df.TIMESTAMP)
    session = Session(engine)
    # Make sure the readers exist in the readers table - add if missing
    provided_reader_ids = set(df['UUID'].dropna().tolist())
    existing_readers = [
        r.reader_id for r in session.query(Readers).filter(Readers.reader_id.in_(provided_reader_ids)) if r
    ]
    non_existing_readers = [r_id for r_id in provided_reader_ids if r_id not in existing_readers]
    reader_id_max_length = Readers.reader_id.type.length
    for reader_id in non_existing_readers:
        if len(reader_id) > reader_id_max_length:
            errors.append("UUID exceeds max length: {0}".format(reader_id))
        else:
            session.add(
                Readers(reader_id=reader_id, user_id=user_id, description="System Added - please update description")
            )
    # Make sure the tags exist in the tags table - add if missing
    provided_tag_ids = set(df['TAG_ID'].dropna().tolist())
    existing_tags = [t.tag_id for t in session.query(Tags).filter(Tags.tag_id.in_(provided_tag_ids)) if t]
    non_existing_tags = [t_id for t_id in provided_tag_ids if t_id not in existing_tags]
    tag_id_max_length = Tags.tag_id.type.length
    for tag_id in non_existing_tags:
        if len(tag_id) > tag_id_max_length:
            errors.append("TAG_ID exceeds max length: {0}".format(tag_id))
        else:
            session.add(Tags(tag_id=tag_id, description="System Added - please update description"))
            session.add(TagOwner(tag_id=tag_id, user_id=user_id, start_time=datetime.now(pytz.utc)))

    # does not prevent duplicates, this is desired
    for index, record in df.iterrows():
        session.add(
            TagReads(
                tag_id=record['TAG_ID'],
                user_id=user_id,
                reader_id=record['UUID'],
                tag_read_time=record['TIMESTAMP'],
                accessory_data=record[df.columns.difference(reserved_fields)].to_json(),
                public=False
            )
        )
    try:
        logging.debug("new", len(session.new))
        #logging.debug("updated", updated)
        logging.debug("dirty", len(session.dirty))
        logging.debug("deleted", len(session.deleted))
        logging.debug(set(record.tag_id for record in session.dirty if record.__dict__.get("tag_id")))
        #logging.debug("nonowned", len(non_owned_tag_ids))
        logging.error(errors)
        session.commit()
        success = True
    except SQLAlchemyError as e:
        logging.error(e.message)
        logging.debug(session.info)
        session.rollback()
        success = False
    finally:
        session.close()
    return {"success": success} if success else {"success": success, "errors": errors}


def load_locations(df, user_id):

    # ensure that a connection to the db is established
    global engine
    try:
        engine.execute("select 1")
    except OperationalError:
        engine = create_engine(URL(**PG_DB))

    df.STARTDATE = pd.to_datetime(df.STARTDATE, utc=True)
    df.ENDDATE = pd.to_datetime(df.ENDDATE, utc=True)
    session = Session(engine)
    # update existing records    
    provided_reader_ids = set(df['UUID'].dropna().tolist())
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
            # FIXME: if database is initialized with test data, this will collide with existing test data
            session.flush()
        session.commit()
        success = True
    except SQLAlchemyError as e:
        logging.error(e.message)
        session.rollback()
        success = False
    finally:
        session.close()
    return {"success": success}  # TODO: add error details to output


def load_animals(df, user_id):

    # ensure that a connection to the db is established
    global engine
    try:
        engine.execute("select 1")
    except OperationalError:
        engine = create_engine(URL(**PG_DB))

    df.columns = map(str.upper, df.columns)
    df.TAG_STARTDATE = pd.to_datetime(df.TAG_STARTDATE, utc=True)
    df.TAG_ENDDATE = pd.to_datetime(df.TAG_ENDDATE, utc=True)

    session = Session(engine)

    provided_tag_ids = set(df['TAG_ID'].dropna().tolist())
    existing_tag_records = session.query(Tags, TagOwner).filter(
        Tags.tag_id.in_(provided_tag_ids),
        Tags.tag_id == TagOwner.tag_id
    )

    existing_tagged_animal_records = session.query(Animals, TaggedAnimal, TagOwner).filter(
        Animals.animal_id == TaggedAnimal.animal_id,
        TaggedAnimal.tag_id.in_(provided_tag_ids),
        TaggedAnimal.tag_id == TagOwner.tag_id
    )

    existing_tag_ids = set([record.tags.tag_id for record in existing_tag_records if record.tag_owner.user_id == user_id])
    non_owned_tag_ids = set([record.tags.tag_id for record in existing_tag_records if record.tag_owner.user_id != user_id])
    new_tag_ids = provided_tag_ids - existing_tag_ids - non_owned_tag_ids

    # The following data_fields and reserved_fields are used in calculating custom field_data
    data_fields = [
        'ANIMAL_IDENTIFYINGMARKERSTARTDATE',
        'ANIMAL_IDENTIFYINGMARKERENDDATE',
        'ANIMAL_ORIGINALMARKER',
        'ANIMAL_CURRENTMARKER'
    ]
    reserved_fields = data_fields + [
        'ANIMAL_SPECIES',
        'TAG_ID',
        'TAG_STARTDATE',
        'TAG_ENDDATE'
    ]

    # Update existing records
    updated = 0
    for record in existing_tagged_animal_records:
        # Match existing records using the following:
        df_record = df[
            (df['TAG_ID'] == record.tagged_animal.tag_id) &
            (df['TAG_STARTDATE'] == record.tagged_animal.start_time)
            ]
        df_record_count = len(df_record)
        if df_record_count == 1:
            # One single match
            changed = False
            # update animal.species
            if df_record['ANIMAL_SPECIES'].iloc[0] != record.animals.species:
                record.animals.species = df_record['ANIMAL_SPECIES'].iloc[0]
                changed = True
                logging.info("updated animals species")
            # Update taggedanimal.enddate
            if (df_record['TAG_ENDDATE'].iloc[0] != record.tagged_animal.end_time) and not pd.isna(
                    df_record['TAG_ENDDATE'].iloc[0]):
                record.tagged_animal.end_time = df_record['TAG_ENDDATE'].iloc[0]
                changed = True
                logging.info("updated taggedanimal enddate")
            elif pd.isna(df_record['TAG_ENDDATE'].iloc[0]) and (record.tagged_animal.end_time is not None):
                record.tagged_animal.end_time = None
                changed = True
                logging.info("cleared taggedanimal enddate")
            # Update animal.field_data
            animal_field_data = loads(record.animals.field_data)
            new_field_data = {}
            for field in data_fields:
                new_value = df_record[field].iloc[0] if not pd.isna(df_record[field].iloc[0]) else None
                existing_value = animal_field_data.get(field, None)
                if (new_value != existing_value) and not pd.isna(new_value):
                    logging.debug(new_value, existing_value)
                    changed = True
                    logging.debug("animal from update -> " + field.upper())
                    new_field_data[field.upper()] = new_value
                    logging.info("updated animals field_data")
            if new_field_data:
                # This overwrites previous field_data for this record
                record.animals.field_data = dumps(new_field_data)
            # Update taggedanimal.field_data
            ta_data_fields = [field for field in df.columns if field not in reserved_fields]
            tagged_animal_field_data = loads(record.tagged_animal.field_data)
            new_ta_field_data = df_record[ta_data_fields].iloc[0].fillna("").to_dict()
            changed_fields = {
                key: value for key, value in new_ta_field_data.items()
                if tagged_animal_field_data.get(key, None) != value
            }
            if changed_fields:
                changed = True
                # This overwrites previous field_data for this record FIXME: Should new field data be appended?
                tagged_animal_field_data.update(changed_fields)
                record.tagged_animal.field_data = dumps(tagged_animal_field_data)
                logging.info("updated tagged animal field data")

            if changed:
                updated += 1

        elif df_record_count > 1:
            # Multiple matches - handle and update
            # combine field data into one record and use the last record for updating non-field data values
            ta_data_fields = [field for field in df.columns if field not in reserved_fields]
            ta_combined_data_fields = df_record[ta_data_fields].fillna("").to_dict()
            df_last = df_record.iloc[-1]
            changed = False
            # update animal.species
            if df_last['ANIMAL_SPECIES'] != record.animals.species:
                record.animals.species = df_last['ANIMAL_SPECIES']
                changed = True
                logging.info("updated animals species")
            # Update taggedanimal.enddate
            if (df_last['TAG_ENDDATE'] != record.tagged_animal.end_time) and not pd.isna(df_last['TAG_ENDDATE']):
                record.tagged_animal.end_time = df_last['TAG_ENDDATE']
                changed = True
                logging.info("updated taggedanimal enddate")
            elif pd.isna(df_last['TAG_ENDDATE']) and (record.tagged_animal.end_time is not None):
                record.tagged_animal.end_time = None
                changed = True
                logging.info("cleared taggedanimal enddate")
            # Update animal.field_data
            # FIXME: The following line raises "ValueError: If using all scalar values, you must pass an index" in certain situations
            df_animal_field_data = pd.DataFrame(loads(record.animals.field_data))
            if set(df_animal_field_data) == set(df_record[data_fields]):
                # columns correspond, check for new data
                # FIXME: index dtypes are not matching, causing this to always flag as not equal
                if not df_animal_field_data.equals(df_record[df_animal_field_data.columns]):
                    # FIXME: The following overwrites existing data - get previous values and append to the update
                    record.animals.field_data = dumps(df_record[data_fields].fillna("").to_dict())
                    logging.info("updated animals with multiple field_data")
                    changed = True
            else:
                # field data columns are different
                # FIXME: The following overwrites existing data - get previous values and append to the update
                record.animals.field_data = dumps(df_record[data_fields].fillna("").to_dict())
                logging.info("updated animals with multiple field data with change in columns")
                changed = True

            # Update taggedanimal.field_data
            # TODO: add functionality to check for change in data
            df_tagged_animal_field_data = pd.DataFrame(loads(record.tagged_animal.field_data))
            if set(df_tagged_animal_field_data) == set(df_record[ta_data_fields]):
                if not df_tagged_animal_field_data.equals(df_record[ta_data_fields].fillna("")):
                    record.tagged_animal.field_data = dumps(df_record[ta_data_fields].fillna("").to_dict())
                    logging.info("updated tagged animals with multiple field data")
                    changed = True
            else:
                # field data columns are different
                # FIXME: The following overwrites existing data - get previous values and append to the update
                record.tagged_animal.field_data = dumps(df_record[ta_data_fields].fillna("").to_dict())
                logging.info("updated tagged animals with multiple field data with change in columns")
                changed = True
    
            if changed:
                updated += 1
    
        else:
            # If you reach this point, something wrong happened
            logging.error("Flagged animal record for update but no data found for update")

    # Add new records
    for tag_id in new_tag_ids:
        records = df[df['TAG_ID'] == tag_id]
        if len(records) > 1:  # flatten records with same tag_id
            logging.debug('multiple matches')
            ta_field_names = [field for field in df.columns if field not in reserved_fields]
            combined_data_fields = records[data_fields + ta_field_names].fillna("").to_dict()
            record = records.iloc[-1]  # default to keeping non-flattened attributes from the last record
            for field_name, field_value in combined_data_fields.items():
                record[field_name] = field_value
        elif len(records) == 1:
            logging.debug('single match')
            record = records.to_dict(orient='record')[0]
        else:
            logging.debug('no matches found')

        animal = Animals(
            species=record['ANIMAL_SPECIES'],
            field_data=dumps(
                {item.upper(): record[item] for item in data_fields
                 if not pd.isna(record.get(item, None))
                 }
            )
        )
        logging.debug("animal records from new -> " + animal.field_data)

        tag = Tags(tag_id=record['TAG_ID'], description="System Added - please update description")
        tagowner = TagOwner(tag_id=record['TAG_ID'], user_id=user_id, start_time=datetime.now(pytz.utc))

        # All fields that are not specifically used elsewhere should be captured in the field_data column
        tagged_animal_field_data_keys = list(
            set(record.keys()) - set(reserved_fields)
        )
        # FIXME: running same import twice indicates an update to tagged animal field data - order of json fields
        taggedanimal = TaggedAnimal(
            start_time=record['TAG_STARTDATE'] if record['TAG_STARTDATE'] is not pd.NaT else None,
            end_time=record['TAG_ENDDATE'] if record['TAG_ENDDATE'] is not pd.NaT else None,
            field_data=dumps(
                # All remaining fields that are not used in other tables
                {item.upper(): record[item] for item in tagged_animal_field_data_keys
                 if not pd.isna(record.get(item, None))}
            )
        )
        logging.debug("tagged animal record from new -> " + taggedanimal.field_data)

        taggedanimal.tags = tag
        taggedanimal.animals = animal
        session.add(taggedanimal)
        session.add(tagowner)

    try:
        logging.debug("new {0}".format(len(session.new)))
        logging.debug("updated {0}".format(updated))
        logging.debug("dirty {0}".format(len(session.dirty)))
        logging.debug("deleted {0}".format(len(session.deleted)))
        session.commit()
        success = True
    except SQLAlchemyError as e:
        logging.error(e.message)
        session.rollback()
        success = False
    finally:
        session.close()
        return {"success": success}  # TODO: add error details to output

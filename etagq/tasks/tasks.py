from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests
import json
import os, sys
import pandas as pd
from datetime import datetime

from db_utils import get_columns

#Default base directory 
basedir="/data/static/"


def parseFile(path, filetype, userid):
    """
    Parse file and check for required columns
    """
    df = pd.read_csv(path)
    file_fields = list(df.columns.str.upper())
    df.columns = map(str.upper(), df.columns)  # set column headers to upper case

    if filetype == "animals":
        # details regarding tagged animals
        """
        Relationship between file fields and database tables and fields
        Animals Table:
            animal_id ??
            ANIMAL_SPECIES = species
            field_data ??
        TaggedAnimal Table:
            TAG_ID = tag_id
            animal_id ??
            TAG_STARTDATE = start_time
            TAG_ENDDATE = end_time
            field_data ??
        Tags Table:
            TAG_ID = tag_id
            description ??
        TagOwner Table:
            user_id ?? default to logged in user - how to handle if already exists?
            TAG_ID = tag_id
            TAG_STARTDATE = start_time
            TAG_ENDDATE = end_time ??
        """
        required_fields = ["TAG_ID", "TAG_STARTDATE", "ANIMAL_SPECIES"]  # TODO: add required fields
        optional_fields = ["TAG_ENDDATE"]
        # remaining fields should be added to field_data as a single json object
        if not all([column in file_fields for column in required_fields]):
            return {"ERROR": "file does not have all required fields"}

    if filetype == "locations":
        # reader locations
        """
        Relationship between file fields and database tables and fields
        Readers Table:
           UUID = reader_id
           DESCRIPTION = description
           user_id ?? default to logged in user
        Locations Table:
           location_id ??
           NAME = name
           LATITUDE = latitude
           LONGITUDE = longitude
           active ?? default to True
        ReaderLocation Table:
           reader_id ??
           location_id ??
           STARTDATE = start_timestamp
           ENDDATE = end_timestamp
        """
        # TODO: update if changes are made to the database tables
        required_fields = ["UUID", "NAME", "STARTDATE", "LATITUDE",
            "LONGITUDE", "DESCRIPTION"]
        optional_fields = ["ENDDATE", "STUDYTYPE"]
        if not all([column in file_fields for column in required_fields]):
            return {"ERROR": "file does not have all required fields"}

    if filetype == "tags":
        # tags seen by readers
        """
        Relationship between file fields and database tables and fields
        AnimalHitReader (animal_hit_reader) Table:
            UUID = reader_id
            animal_id ??
            TAG_ID = tag_id
        TagReads (tag_reads) Table:
            tag_reads_id ??
            UUID = reader_id
            TAG_ID = tag_id
            user_id ?? default to logged in user
            TIMESTAMP = tag_read_time
            field_data ??
        """
        # UUID is the reader ID
        required_fields = ["UUID", "TAG_ID", "TIMESTAMP"]
        if not all([column in file_fields for column in required_fields]):
            return {"ERROR": "file does not have all required fields"}
        animalhitreader_df = df[["UUID", "TAG_ID"]]
        # TODO: add animal_id to animalhitreader_df dataframe
        tagreads_df = df[["UUID", "TAG_ID", "TIMESTAMP"]]
        tagreads_df["USERID"] = userid
        return dict(get_columns("animal_hit_reader", ["reader_id", "tag_id"]))

    return("Success")
    #TODO upsert file data into databse


@task()
def etagDataUpload(local_file,request_data):
    
    """
    This task is associated with the etag-file-upload view.
    The view URl: /api/etag/file-upload/ . Provides a mechanism 
    to upload local file and runs this task. If you are unsure 
    you should go to the upload view to run task.

    etagDataUpload(local_file,request_data)
    args:
        local_file - local filepath (associated with etag-file-upload view
    """
    
    filetypes = ["animals", "locations", "tags"]
    filetype = request_data.get('filetype', None)
    userid = request_data.get('userid', None)
    if filetype not in filetypes:
        return {"ERROR": "filetype must be one of: animals, locations, tags"}
    if not userid:
        return {"ERROR": "missing userid"}
    parse_status = parseFile(local_file, filetype, userid)
    #TODO update return to provide results to user
    return (local_file, request_data, parse_status)

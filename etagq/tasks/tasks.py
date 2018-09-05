from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests
import json
import os, sys
import pandas as pd
from datetime import datetime

#Default base directory 
basedir="/data/static/"

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

    return (local_file, request_data)

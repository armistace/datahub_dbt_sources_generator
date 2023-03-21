####################GENERATE SOURCES#######################
#This file will generate the yaml file that will act as the sources for DBT models
#It does this by hitting datahub and scraping the metadata to generate what datahub found in its connection
#It will attempt to hit datahub for 10 minutes and if it fails raise and error
#TODO: This has been made more modular for quick release, this needs to be an object with proper get, set methods
#functions with 6 odd variable calls indicate something is very very wrong hahaha

import logging
import os
import yaml
import requests
import time
from datahub.emitter.mce_builder import make_dataset_urn
# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetKeyClass,
    StatusClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldKeyClass,
    EditableSchemaMetadataClass
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



gms_endpoint = os.environ['DATAHUB_GMS_URL']
gms_token = os.environ['DATAHUB_GMS_TOKEN']
DATAHUB_ENV = os.environ["DATAHUB_ENV"]


def get_graph(gms_endpoint, gms_token, num_tries):
    """
        This function will attempt to set the graph variable
        It does that by attempting to the gms endpoint for 10 minutes
        If, after 10 minutes gms_endpoint does not return 200 it will cause this function to error
        the datahubgraph call does already do something similar however it doesn't last long enough
        so this will add a bit more time to that attempt 
    """
    try:
        #establish headers and url so that we can get a valid 200 code
        headers={
                    'Content-Type':'application/json',
                    'Accept':'application/json',
                    'Authorization': 'Bearer {}'.format(gms_token)
                }
        #This url is to swagger ui and should be present regardless of the state of metadata
        url = f"{gms_endpoint}/openapi/swagger-ui/index.html"

        response = requests.get(url=url, headers=headers, timeout=5)
        if response.status_code == 200:
            graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=gms_token))
            return graph
        elif num_tries > 60:
            raise Exception(f"{gms_endpoint} Has not been returned status code 200 for 10 minutes stopping")
        else:
            log.info(f"{gms_endpoint} not available trying again in 10 seconds")
            time.sleep(10)
            num_tries += 1
            return get_graph(gms_endpoint, gms_token, num_tries)
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        if num_tries > 60:
            raise Exception("Error Massive timeout on URL: {}".format(e))
        else:
            log.info(
                    f"""
                        {gms_endpoint} Not available and has timedout or otherwise not returned an expected status code.
                        exception returned: {e}
                        retrying again in 10 seconds
                        This is try number {num_tries}
                    """
                    )
            time.sleep(10)
            num_tries += 1
            return get_graph(gms_endpoint, gms_token, num_tries)



#This needs to go here as it is global used by every function and we want to call it once not 
#every function
######################DATAHUB DATA VARIABLE IS HERE###########################
graph = get_graph(gms_endpoint, gms_token, 0)
#############################################################################

def get_urns(platform, database):
    log.info(f"Determine urns")
    log.info(f"Using env {DATAHUB_ENV}")
    start=0

    glue_search = f"dataPlatform:{platform},{database}"

    listresult = graph.list_all_entity_urns(entity_type="dataset", start=start, count=1)

    glue_urns=[]


    while listresult != []:
        if glue_search in listresult[0]:
            #we get this odd no table result, don't want that
            if listresult[0] != f"urn:li:dataset:(urn:li:dataPlatform:{platform},{database}.,{DATAHUB_ENV})":
                if DATAHUB_ENV in listresult[0]:
                    log.info(f"Adding {listresult[0]} to list")
                    glue_urns.append(listresult[0])
        start += 1
        listresult=graph.list_all_entity_urns(entity_type="dataset",start=start, count=1)
    return{"glue": glue_urns}


def get_schema(dataset_urn):
    log.info(f"Getting schema for {dataset_urn}")
    schema=graph.get_schema_metadata(dataset_urn)
    return schema

def get_editable_schema_info(dataset_urn):
    log.info(f"Getting editableSchema for {dataset_urn}")
    schema=graph.get_aspect(entity_urn=dataset_urn, aspect_type=EditableSchemaMetadataClass)
    return schema

def get_tags(dataset_urn):
    log.info(f"retrieving tags for {dataset_urn}")
    schema=graph.get_tags(dataset_urn)

def strip_urn(dataset_urn, platform, database, strip_platform, strip_database):
    glue_search = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{database}."
    mysql_search = f"urn:li:dataset:(urn:li:dataPlatform:{strip_platform},{strip_database}."
    end = f",{DATAHUB_ENV})"

    table = dataset_urn.replace(glue_search,"").replace(mysql_search, "").replace(end,"")
    log.info(f"Stripping {dataset_urn} to {table}")
    return table

def find_tag(globalTags, tag_name):
    log.info("Entered find_tag")
    tag_found = False
    if globalTags != None:
        log.info(f"{globalTags} Determined to be not none")
        these_tags = globalTags
        if these_tags != None:
            log.info(f"{these_tags} determined to be not none")
            if these_tags.tags != None:
                log.info(f"{these_tags.tags} determined to be not none")
                for atag in these_tags.tags:
                #hit issue whereby I delete a tag and it leaves the object with an empty list causing
                #attribute error
                    if atag != []:
                        log.info(f"{atag} determined to be not []")
                        if atag.tag == tag_name:
                            log.info(f"atag.tag = {atag.tag}")
                            log.info(f"tag {tag_name} found in {globalTags}")
                            tag_found = True

    if tag_found == False:
        log.info(f"{tag_name} not found in {globalTags}")
    return tag_found



def build_schema(platform, database, strip_urns=False, strip_platform="", strip_database=""):
    urns=get_urns(platform, database)
    schemas={}
    for item in urns["glue"]:
        log.info(f"building schema for {item}")
        fields=[]
        try:
            #Dear God, unfortunately stuff edited direct in the UI
            #is considered a different class to stuff updated through ingestion
            #consequence is I now need to pull down multiple classes
            #to compare them to eachother and get the relevant data for
            #dbt. This block is the result of that... sigh
            log.info("Atttempting to get editableSchema")
            if get_editable_schema_info(item) != None:
                ui_info = get_editable_schema_info(item)
                for name in get_schema(item).fields:
                    log.info("editableSchema Checking...")
                    final_name = name.fieldPath.split(".")[3]
                    description = None
                    for ed_name in ui_info.editableSchemaFieldInfo:
                        if ed_name.fieldPath == name.fieldPath:
                            log.info(f"editable schema is same as {final_name}")
                            if find_tag(ed_name.globalTags, "urn:li:tag:dbtPrimary"):
                                description = "primary_key"
                            else:
                                if find_tag(name.globalTags, "urn:li:tag:dbtPrimary"):
                                    description = "primary_key"
                                else:
                                    if ed_name.description == None:
                                        description = name.description
                                    else:
                                        description = ed_name.description

                    schema_dict = {final_name: description}
                    log.info(f"Appending {schema_dict}")
                    fields.append(schema_dict)
            else:
                log.info("No editableSchema found")
                #Heres what it should have been :/
                #TODO: clean up this monstrosity to make it more dry
                for name in get_schema(item).fields:
                    final_name = name.fieldPath.split(".")[3]
                    log.info(f"Checking {final_name} for tags")
                    if find_tag(name.globalTags, "urn:li:tag:dbtPrimary"):
                        description = "primary_key"
                    else:
                        description = name.description
                    schema_dict = {final_name: description}
                    log.info(f"Appending {schema_dict}")
                    fields.append(schema_dict)
        # if something fallse over lets grab it here
        except AttributeError as ae:
            log.warn(f'''{item} Has AttributeError {ae} ''')
            log.warn("It has been skipped")
            continue
        except TypeError as te:
            log.warn(f'''{item} has TypeError {te}''')
            log.warn("It has been skipped")
            continue
        if strip_urns:
            if strip_platform == "" or strip_database == "":
                raise ValueError( """
                                 You can not perform strip urn without \
                                 a platform and database to strip from \
                                 """)

            schemas[strip_urn(item, platform, database, strip_platform, strip_database)] = fields

    return schemas

def build_config_dict(name, description, schema, database, loader, strip_urns, strip_platform, strip_database):
    #unfortunately this is a bit of an issue with athena, what athena refers to as a schema
    #datahub treats as a database, so here we already
    #TODO: find a better nameing scheme that doesn't just cater for athena/presto/trino
    schemas = build_schema(loader, schema, strip_urns, strip_platform, strip_database)
    log.info("building dbt schema")
    log.info("Building Header")
    yaml_builder = {
            "version" : 2,
            "sources" :[
                {
                "name" : f"{name}",
                "description": f"{description}",
                "schema": f"{schema}",
                "database": f"{database}",
                "loader": f"{loader}",
                "tables" : []
                }
            ]
        }
    tables = []
    log.info("Building Tables")
    for dataset in schemas:
        table={}
        table["name"] = dataset
        table["description"] = "TBD"
        fields = schemas[dataset]
        columns = []
        log.info(f"Building Table {dataset}")
        for field in fields:
            for name in field:
                log.info(f"Building Column {name}")
                column={}
                column["name"] = name
                if field[name] == None:
                    column["description"] = "No Description Present in Datahub"
                else:
                    column["description"] = field[name]
                columns.append(column)
        table["columns"] = columns
        tables.append(table)
        #we can slice here at 0 as we don't have multiple sources yet
        #but we may 
        #TODO: genercise and work out number of sources
    yaml_builder["sources"][0]["tables"]=tables
    return yaml_builder

def write_to_file(file, name, description, schema, database, loader, strip_urns, strip_platform, strip_database):
    with open(file, "+w", encoding="utf-8") as yaml_file:
        dump = yaml.dump(build_config_dict(name, description, schema, database, loader, strip_urns, strip_platform, strip_database), default_flow_style = False, allow_unicode = True, encoding = None)
        log.info(f"Writing Diction to {file} as Yaml")
        yaml_file.write(dump)



#file = "../models/sources.yaml"
#write_to_file("../models/sources.yaml")



# DATAHUB DBT Sources Generator

This python script can be used to connect to a Datahub instance using the GMS URL and Token and use the [Rest API availabe in datahub gms](https://datahubproject.io/docs/api/openapi/openapi-usage-guide) to take in the schema information from datahub and build a field level sources.yaml file that complies with the [DBT sources config](https://docs.getdbt.com/docs/build/sources) 

It can detect both scraped and custom metadata but doesn't currently do a lot with that information
_great place to expand :D_

To use you need to set the following environment variables

```
DATAHUB_GMS_URL
DATAHUB_GMS_TOKEN
DATAHUB_ENV
```

The GMS URL is the url endpoint for the datahub api of your datahub instance
The GMS Token is the token generated in Datahub for the user with API access
The Env is the Datahub Environment, by default Datahub has an environment of PROD but there can be mulitple example, DEV, BETA etc

To use you will need to install the requirements in your python environment, it is strongly recomended you use virtual environments

`pip install -r requirements.txt`

To run simply invoke the click the script with 

`python build_base_models.py <options>

## Options

```
Usage: build_base_models.py [OPTIONS]

  Tool to build the sources file for DBT from Datahub

  To invoke manully build_base_models.py <yaml_source_file>

  yaml source file is assumed to follow dbt conventions
  https://docs.getdbt.com/docs/build/sources

Options:
  --source_path TEXT     Sources file in yaml format as per dbt requirements
  --name TEXT            Name for the data source that DBT will get
  --description TEXT     Human readable description of the data source for DBT
  --schema TEXT          name of the schema in the base data source
  --database TEXT        name of the database in the base data source
  --loader TEXT          name of the datahub loader you are scraping
  --strip_urns BOOLEAN   if you require the renaming or striping of and data
                         based on another database set this to true (optional)
  --strip_platform TEXT  name of the platform you are stripping from (optional)
  --strip_database TEXT  name of the database you are stripping from (optional)
  --help                 Show this message and exit.
```

You can set a tag in Datahub of dbtPrimary and this script will add a description to the sources file for that field of 'primary_key'

_This is a WIP, It likely contains config issues that will require hand coding in some instances for your specific use case it could change at any time._
*_If you can please contribute back with changes so others can share in the automation!_*

### Contributors

*If you would like to contribute there are lots of places this can expand*

1. Generating Common Models from the Sources File 
2. Other data catalogs ([Open Lineage](https://openlineage.io/), [Glue](https://docs.aws.amazon.com/glue/latest/dg/start-data-catalog.html), [Hive](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/hive/hive_catalog/), etc...)
3. Better use of the editableSchema for more fine grained control using the datahub frontend 
4. Refactor to use an object model
5. further clean up of comments or config 
6. Whatever else you can think of!

#### Thanks

Big thanks to [Redeye](https://www.redeye.co/) For investing in Data Engineering and allowing me to share this little script with the world!

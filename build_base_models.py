import python_helpers.generate_sources as gs
import click

@click.command()
@click.option('--source_path', type=str, help="Sources file in yaml format as per dbt requirements")
@click.option('--name', type=str, help="Name for the data source that DBT will get")
@click.option('--description', type=str, help="Human readable description of the data source for DBT")
@click.option('--schema', type=str, help="name of the schema in the base data source")
@click.option('--database', type=str, help="name of the database in the base data source")
@click.option('--loader', type=str, help="name of the datahub loader you are scraping")
@click.option('--strip_urns', required=False, type=bool, help="if you require the renaming or striping of and data based on another database set this to true")
@click.option('--strip_platform', required=False, type=str, help="name of the platform you are stripping from")
@click.option('--strip_database', required=False, type=str, help="name of the database you are stripping from")

def main(source_path, name, description, schema, database, loader, strip_urns=False, strip_platform="", strip_database=""):
    '''
        Tool to build the sources file for DBT from Datahub

        To invoke manully
        build_base_models.py <yaml_source_file>

        yaml source file is assumed to follow dbt conventions
        https://docs.getdbt.com/docs/build/sources
    '''
    gs.write_to_file(source_path, name, description, schema, database, loader, strip_urns, strip_platform, strip_database)

if __name__ == "__main__":
    main()




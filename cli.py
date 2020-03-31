from datetime import datetime
import logging
import io
from pathlib import Path

import click
import requests

import geopandas as gpd
import pandas as pd


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
format_string = '%(levelname)s | %(asctime)s | %(message)s'
formatter = logging.Formatter(format_string)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


@click.group()
@click.option('--debug', is_flag=True, default=False)
@click.pass_context
def cli(ctx, debug):
    """
    🦠 Corona
    """
    if debug:
        logger.setLevel(logging.DEBUG)
    ctx.obj = dict()
    ctx.obj['logger'] = logger
    cli.get_help(ctx)


@cli.command('update-data')
def update_data():
    """Update New York Times COVID-19  data"""
    url_states = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv'
    url_counties = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'

    data_folder = Path(Path(__file__).parent, 'data')
    data_folder.mkdir(exist_ok=True)
    
    with open(Path(data_folder, 'us-states.csv'), 'wb') as fout:
        fout.write(_get_data_from_url(url_states))
   
    with open(Path(data_folder, 'us-counties.csv'), 'wb') as fout:
            fout.write(_get_data_from_url(url_counties))

@cli.command('update-county-shapes')
def update_shapefile():
    """Create geojson from COVID-19 data"""
    
    data_folder = Path('data')
    usa_county_geojson = Path(data_folder, 'COUNTY_SHAPES.geojson')
    us_counties_covid_csv = Path(data_folder, 'us-counties.csv')

    # define DataFrames
    usa_county_shapes = gpd.read_file(usa_county_geojson)
    date_parser = lambda x: datetime.strptime(x, '%Y-%m-%d')
    us_counties_covid = pd.read_csv(us_counties_covid_csv, parse_dates=['date'], date_parser=date_parser, dtype={'fips': str})

    # fix values for New York City
    # counties that need fixing: Queens, New York, Richmond, Bronx, Kings
    filt = us_counties_covid['county'] == 'New York City'
    us_counties_covid.loc[filt, 'fips'] = '36666'
    us_counties_covid.to_csv(us_counties_covid_csv, sep=',', encoding='utf-8')

    # filter most recent data
    highest_date = us_counties_covid.nlargest(1, columns='date')['date'].values[0]
    filt = (us_counties_covid['date'] >= highest_date)
    us_counties_covid = us_counties_covid.loc[filt]

    usa_county_shapes.rename(columns={'GEOID': 'fips'}, inplace=True)

    covid_shapes = usa_county_shapes.merge(us_counties_covid, on='fips', how='left')

    outfile = Path(data_folder, 'corona_counties.geojson')
    covid_shapes.to_file(outfile, driver='GeoJSON')


def _get_data_from_url(url):
    r = requests.get(url)
    r.raise_for_status
    return r.content


if __name__ == '__main__':
    cli()
#! /usr/bin/env python3

# Ingest script
#
# This script takes a tarball of GRIB files and extracts the first band: the one
# whose validity time is closest to the forecast time. It saves them into an
# HDF5 file where the paths are akin to the following.
#
# First `2.5/` or `5/`. The datasets under the former have 2.5km grids and the
# datasets under the latter have 5km grids.
#
# Then the next leaf of the path is an ISO8601-formatted date that refers to the
# **validity date** for the data point in question. There are two special leafs:
# `lats` and `lons`, which are arrays that give the latitude and longitude of
# each point in the grid.
#
# I.e.
# 2.5/2019-01-01T03:00:00
# 2.5/2019-01-01T04:00:00
# 2.5/2019-01-01T05:00:00
# 2.5/lats
# 2.5/lons

# TODO: Create index file on disk that has the names of the GRIB and/or tar
# files already imported

from s3 import S3

import json
import click
import pygrib
import tarfile
import requests
import tempfile
import numpy as np
import dateutil.parser

from io import BytesIO
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from urllib.request import urlretrieve

##
# Download index to machine; add all current id's to index; upload back to s3


@click.command()
@click.option('-i', '--order_id', required=True, help='NDFD Order ID')
def main(order_id):
    # Start session connected to S3
    s3_session = S3(bucket_name='hist-wx-map-layer')

    tarball_urls = get_download_urls_for_order(order_id)
    print('Got download urls for tarball')

    for tarball_url in tarball_urls:
        wmo_code = tarball_url['wmo_code']

        with tempfile.TemporaryDirectory() as dirpath:
            tarball_dest = download_tarball(tarball_url['tar_url'], dirpath)
            print('Finished downloading tarball')

            with tarfile.open(tarball_dest, 'r') as tar:
                print('Extracting files and saving to S3')
                extract_files_from_tarball(s3_session, tar, wmo_code)


def get_download_urls_for_order(order_id: str):
    """Given an order id, retrieves the urls for each tarball of the order.

    Returns: (List[str]) List of tar files
    """
    if not order_id.startswith('HAS'):
        raise ValueError('order_id must start with HAS')

    url = f'https://www1.ncdc.noaa.gov/pub/has/{order_id}/'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    ext = '.tar'

    # Get the URLs of each tarball in the order
    tar_files = [
        url + node.get('href')
        for node in soup.find_all('a')
        if node.get('href').endswith(ext)]

    # Only download the files for all of CONUS, these have a U in the 3rd slot
    tar_files = [x for x in tar_files if Path(x).name[12:13] == 'U']

    # Parse the URLs to create a list of dicts, where each dict has the WMO code
    # as well as the year, month, and day of the tarball
    tar_dict = [{
        'tar_url': x,
        'wmo_code': Path(x).name[10:12],
        'year': int(Path(x).name[14:18]),
        'month': int(Path(x).name[18:20]),
        'day': int(Path(x).name[20:22]), } for x in tar_files]
    return tar_dict


def download_tarball(tarball_url: str, dirpath: str):
    """Download tarball to disk. This uses urllib3 instead of requests in order
    to easily download files larger than memory
    """
    tarball_name = Path(tarball_url).name
    dest = Path(dirpath) / tarball_name
    urlretrieve(tarball_url, dest)
    return dest


def extract_files_from_tarball(s3_session, tar, wmo_code):
    """Extract files from tarball and save to S3

    S3 path: wmo_code/year/month/day/hour.{npy,json}

    Args:
        s3_session: an S3 session object (defined in s3.py) that is already connected to an S3 bucket
        tar: open tarfile
    """
    # For each
    fnames = tar.getnames()

    # don't look at Z97; those are 4-7 day forecasts
    fnames = [x for x in fnames if x[3:6] != 'Z97']

    for fname in fnames:
        print(f'Loading fname: {fname}')

        # Load grib file from tar archive into memory
        f = tar.extractfile(fname)
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(f.read())
            grbs = pygrib.open(fp.name)

        # Only ever care about the first message, the nearest one to when the
        # forecast was made
        grb = grbs.message(1)

        metadata = {}

        if grb['Nx'] == 1073:
            metadata['grid_size'] = '5'
        elif grb['Nx'] == 2145:
            metadata['grid_size'] = '2.5'
        else:
            raise ValueError('Grid has unexpected number of columns')

        # valid_date is when the forecast is _for_
        # forecast_date is when the forecast was _made_
        valid_date = grb.validDate
        valid_date_str = valid_date.isoformat()
        forecast_date = datetime(
            year=grb.year, month=grb.month, day=grb.day, hour=grb.hour,
            minute=grb.minute)
        forecast_date_str = forecast_date.isoformat()

        metadata['valid_date'] = valid_date_str
        metadata['forecast_date'] = forecast_date_str

        # Add all key-value metadata from grb to metadata file
        for key in grb.keys():
            if grb.valid_key(key):
                val = grb[key]
                if isinstance(val, str):
                    metadata[key] = val

        s3_path = f'ndfd_data/{wmo_code}/{metadata["grid_size"]}'
        s3_path += f'/{valid_date.year}/{valid_date.month}'
        s3_path += f'/{valid_date.day}/{valid_date.hour}'

        # If npy file already exists, check when the forecast was made (saved in
        # the json file)
        file_exists = s3_session.file_exists(s3_path + '.npy')
        if file_exists:
            # .npy file already exists: check the forecast time in the json file
            file_metadata = s3_session.client.get_object(
                Bucket=s3_session.bucket_name, Key=s3_path + '.json')
            file_metadata_dict = json.loads(file_metadata['Body'].read())

            existing_forecast_date_str = file_metadata_dict['forecast_date']
            existing_forecast_date = dateutil.parser.parse(
                existing_forecast_date_str)

            if forecast_date <= existing_forecast_date:
                print('NOT replacing')
                continue

            print('replacing')

        save_grb_to_s3(grb, s3_session, s3_path, metadata)


def save_grb_to_s3(grb, s3_session, s3_path, metadata):
    # Save JSON file with metadata
    # Save JSON first so that if the numpy array exists, the metadata always exists
    json_buf = json.dumps(metadata)
    s3_session.client.put_object(
        Body=json_buf, Bucket=s3_session.bucket_name, Key=s3_path + '.json',
        ContentType='application/json')

    # Save numpy array
    buf = BytesIO()
    np.save(buf, grb.data()[0])
    s3_session.client.put_object(
        Body=buf.read(), Bucket=s3_session.bucket_name, Key=s3_path + '.npy')
    print(f'Array saved to {s3_path}.npy')


if __name__ == '__main__':
    main()

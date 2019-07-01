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

import pygrib
import tarfile
import tempfile
import h5py
import dateutil.parser
from datetime import datetime
from pathlib import Path

wmo_code = 'YEU'
file_date = '20190101'

# Load tar file
tar = tarfile.open(f'../data/9959_NDFD_{wmo_code}_{file_date}.tar')

# For each
fnames = tar.getnames()

# don't look at Z97; those are 4-7 day forecasts
fnames = [x for x in fnames if x[3:6] != 'Z97']

h5_save_fname = f'{wmo_code}.hdf5'

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

    if grb['Nx'] == 1073:
        grid_size = '5'
        print('grid size is 5')
    elif grb['Nx'] == 2145:
        grid_size = '2.5'
        print('grid size is 2.5')
    else:
        raise ValueError('Grid has unexpected number of columns')

    valid_date_str = grb.validDate.isoformat()
    forecast_date = datetime(
        year=grb.year, month=grb.month, day=grb.day, hour=grb.hour,
        minute=grb.minute)
    forecast_date_str = forecast_date.isoformat()

    # 2.5km / forecast validity date in iso format > dataset
    group_name = f'{grid_size}/{valid_date_str}'

    if Path(h5_save_fname).exists():
        open_mode = 'r+'
    else:
        open_mode = 'w'

    # f = h5py.File(h5_save_fname, open_mode)
    with h5py.File(h5_save_fname, open_mode) as f:

        # Check if a dataset for the given validity DateTime exists already in
        # the HDF5 file. If so, then the closest prediction from another file is
        # the same validity time. In this case, if the forecast datetime of the
        # current grb is later than the forecast datetime of the saved file,
        # replace the saved data with the data from the current grb.
        if group_name in f:
            print(f'Group already exists: {group_name}')
            existing_forecast_date_str = f[group_name].attrs['forecast_date_str']
            existing_forecast_date = dateutil.parser.parse(
                existing_forecast_date_str)

            print(f'existing forecast date: {existing_forecast_date_str}')
            print(f'current memory forecast date: {forecast_date_str}')

            if forecast_date <= existing_forecast_date:
                print('NOT replacing')
                continue

            print('replacing')

        # Write data to HDF5 file and then write forecast timestamp
        # First check that grid hasn't changed
        msg = 'ERROR! Grid has changed! Stopping'
        if f'{grid_size}/lats' in f:
            # Check that lats array is still the same
            assert np.array_equal(f[f'{grid_size}/lats'][:], grb.data()[1]), msg
        else:
            f.create_dataset(f'{grid_size}/lats', data=grb.data()[1])

        if f'{grid_size}/lons' in f:
            # Check that lons array is still the same
            assert np.array_equal(f[f'{grid_size}/lons'][:], grb.data()[2]), msg
        else:
            f.create_dataset(f'{grid_size}/lons', data=grb.data()[2])

        if group_name in f:
            # Replace dataset
            h5data = f[group_name]
            h5data[...] = grb.data()[0]
        else:
            # Create dataset
            f.create_dataset(group_name, data=grb.data()[0])

        # Add forecast time as an attribute to this dataset
        f[group_name].attrs['forecast_date_str'] = forecast_date_str

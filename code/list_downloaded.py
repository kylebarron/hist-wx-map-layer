# Script to get a better picture of what NDFD files have been downloaded and exist on S3. Helpful to keep track of what to request next from NDFD

from s3 import S3

import pandas as pd
from pathlib import Path
from calendar import monthrange
from tabulate import tabulate


def main():
    s3_client = S3('hist-wx-map-layer')
    files = s3_client.get_matching_s3_keys(prefix='ndfd_data/')
    df = generate_df(files)
    len(df)


def generate_df(files):
    """Generate DataFrame with forecast validityTime of files on S3
    """
    names = [x[:-4] for x in files if x.endswith('.npy')]
    df = pd.DataFrame([Path(x).parts[1:] for x in names], columns=[
        'wmo_code', 'grid_size', 'year', 'month', 'day', 'hour'])

    # Cast numerical columns to numeric
    df['grid_size'] = pd.to_numeric(df['grid_size'], downcast='float')
    df['year'] = pd.to_numeric(df['year'], downcast='integer')
    df['month'] = pd.to_numeric(df['month'], downcast='integer')
    df['day'] = pd.to_numeric(df['day'], downcast='integer')
    df['hour'] = pd.to_numeric(df['hour'], downcast='integer')

    return df


def hours_in_month(year, month):
    return monthrange(year, month)[1]


def full_month(year, month):
    """Determine whether all data for a month is saved
    """


def parse_df(df):
    """Parse DataFrame to find full date ranges
    """

    # grouped_day = df.groupby(['wmo_code', 'year', 'month',
    #                           'day']).size().reset_index(name='counts')
    grouped_month = df.groupby(['wmo_code', 'year',
                                'month']).size().reset_index(name='counts')

    print(
        tabulate(
            grouped_month, headers='keys', tablefmt='github', showindex=False))


if __name__ == '__main__':
    main()

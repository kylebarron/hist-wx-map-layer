import pygrib
grbs = pygrib.open('../data/ds.temp.bin')
grbs.message(1).values[1].shape

from pathlib import Path

datadir = Path('../data/ygu/')
files = [x for x in datadir.iterdir() if x.stem.startswith('YGUZ98')]
files = sorted(files)
for file in files:
    grbs = pygrib.open(str(file))
    print(grbs.message(1))
    grb = grbs.message(1)
    print(grb.data()[0][700][700])

grb.data()[1].shape
grb.data()[1]
grb.data()[2]
grb.latlons()
grb.forecastTime
grb.keys()
dir(grb)
print('hi')

grbs = pygrib.open('../data/ygu/YGUZ98_KWBN_201901010147')
grbs.message(1)


grbs1 = pygrib.open('../data/has_data/YEUZ98_KWBN_201201010048')
grbs2 = pygrib.open('../data/has_data/YEUZ98_KWBN_201201010148')
grbs3 = pygrib.open('../data/has_data/YEUZ98_KWBN_201201010249')
grbs18 = pygrib.open('../data/has_data/YEUZ98_KWBN_201201011648')

grb1 = grbs1.message(6)
grb2 = grbs2.message(6)
grb3 = grbs3.message(1)
grb18 = grbs18.message(1)

grb1.validDate
grb2.validDate
grb3.validDate
grb18.validDate

grb1.data()[0][300][500]
grb2.data()[0][300][500]
grb3.data()[0][300][500]
grb18.data()[0][300][500]

grb1.hour
grb2.hour
grb3.hour
grb18.hour

grbs = pygrib.open('../data/ds.maxt.bin')
grbs.message(1)

grb1.data()[0].shape
grb = grbs.message(1)
grb1
grb
grb
grb['hour']

grb.data()
grb.analDate
grb.validDate

grb['validityTime']
grb.keys()
grbs.message(3)

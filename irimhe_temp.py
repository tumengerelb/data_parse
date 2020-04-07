from osgeo import gdal
from nso_data_collect import conn_postgis
import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import griddata

def idw_interpolation(rasterx,rastery,rasterz):
    try:

        x = rasterx
        y = rastery
        z = rasterz
        # target grid to interpolate to

        #interpolation power 6
        xi = yi = np.arange(0, 1.01, 0.01)
        xi, yi = np.meshgrid(xi, yi)

        # set mask
        mask = (xi > 0.5) & (xi < 0.6) & (yi > 0.5) & (yi < 0.6)

        # interpolate
        zi = griddata((x, y), z, (xi, yi), method='linear')

        # mask out the field
        zi[mask] = np.nan

        # plot
        fig = plt.figure()
        ax = fig.add_subplot(111)
        plt.contourf(xi, yi, zi, np.arange(0, 1.01, 0.01))
        plt.plot(x, y, 'k.')
        plt.xlabel('xi', fontsize=16)
        plt.ylabel('yi', fontsize=16)
        plt.savefig('interpolated.png', dpi=100)
        plt.close(fig)

    except:
        print("interpolation went wrong")

def read_geotiff():
    try:
        print("reading raster dem from tiff file")
    except:
        print("reading raster dem")

def write_geotiff():
    try:
        print("writing raster dem")
    except:
        print("writing raster dem failed")


def get_t800_80_catal(year,month,num_ofmonth):

    sqlstr1 = "select *,CASE WHEN t_800_80.sindex < 400 then 'agrostation' else 'post' end from t_800_80 INNER JOIN agro_catal_t on t_800_80.sindex = agro_catal_t.index where t_800_80.year=2020 and t_800_80.month=1 and t_800_80.num_of_month=2 order by agro_catal_t.aimagid,t_800_80.sindex"
    hostname = '3.136.214.209'
    username = 'prism'
    password = 'zerohunger2018'
    database = 'irimhe'
    tablename = 't_800_80'
    try:
        conn_postgis(database,username,hostname,password,tablename,None)
        print("reading 800 table from postgesql")
    except:
        print("getting 800 giving error")

if __name__ == "__main__":
    try:
        get_t800_80_catal()
    except Exception as e:
        print(str(e))
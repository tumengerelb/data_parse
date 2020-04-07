import os

import gdal
from gdalconst import *


def get_hdf(filepath):

    try:
        g = gdal.Open(filepath)

        if g is None:
            print("Problem" % filepath)
        else:
            print("Processing" % filepath)
            b = g.GetRasterBand(1)
            xSize = b.XSize
            ySize = b.YSize
            geoTrans = g.GetGeoTransform()
            wktProjection = g.GetProjection()

        subdatasets = g.GetSubDatasets()

        for fname, name in subdatasets:
            print("name", name)
            print("\t", fname)
        # Read subdataset
        bArr = gdal.Band.ReadAsArray(b)
        dst_filename = os.path.join(r"/", "afilename.tif")
        driver = gdal.GetDriverByName('GTiff')

        dataset = driver.Create(dst_filename, xSize, ySize, 1, gdal.GDT_Int32)

        dataset.SetGeoTransform(geoTrans)
        dataset.SetProjection(wktProjection)

        oBand = dataset.GetRasterBand(1)

        oBand.WriteArray(bArr)

    except Exception as e:
        print(str(e))


if __name__ == "__main__":
    try:
        get_hdf()
    except Exception as e:
        print(str(e))

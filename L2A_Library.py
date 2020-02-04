#!/usr/bin/env python

from numpy import *
from scipy import interpolate as sp

try:
    from osgeo import gdal,osr
    from osgeo.gdalconst import *
    gdal.TermProgress = gdal.TermProgress_nocb
except ImportError:
    import gdal,osr
    from gdalconst import *

import sys, os

class Error(EnvironmentError):
    pass

def statistics(arr, comment=''):
    if len(arr) == 0:
        return False
    s = 'object:' + str(comment) + '\n'
    s += '--------------------' + '\n'
    s += 'shape: ' + str(shape(arr)) + '\n'
    s += 'sum  : ' + str(arr.sum()) + '\n'
    s += 'mean : ' + str(arr.mean()) + '\n'
    s += 'std  : ' + str(arr.std()) + '\n'
    s += 'min  : ' + str(arr.min()) + '\n'
    s += 'max  : ' + str(arr.max()) + '\n'
    s += '-------------------' + '\n'
    return s


def showImage(arr):
    from PIL import Image
    if (arr.ndim) != 2:
        sys.stderr.write('Must be a two dimensional array.\n')
        return False
    arrmin = arr.min()
    arrmax = arr.max()
    # arrmin = arr.mean() - 3*arr.std()
    # arrmax = arr.mean() + 3*arr.std()
    arrlen = arrmax - arrmin
    arr = clip(arr, arrmin, arrmax)
    scale = 255.0
    scaledArr = (arr - arrmin).astype(float32) / float32(arrlen) * scale
    arr = (scaledArr + 0.5).astype(uint8)
    img = Image.fromarray(arr)
    img.show()
    return True


def rectBivariateSpline(xIn, yIn, zIn):
    x = arange(zIn.shape[0], dtype=float32)
    y = arange(zIn.shape[1], dtype=float32)

    f = sp.RectBivariateSpline(x, y, zIn)
    del x
    del y
    return f(xIn, yIn)


def chmod_recursive(targetdir, mode):
    # fix for SIIMPC-598.2 UMW:
    filelist = os.listdir(targetdir)
    errors = []
    for fnshort in filelist:
        filename = os.path.join(targetdir, fnshort)
        try:
            if os.path.islink(filename):
                pass
            elif os.path.isdir(filename):
                os.chmod(filename, mode)
                chmod_recursive(filename, mode)
            else:
                os.chmod(filename, mode)
        except Error, err:
            errors.extend(err.args[0])
        except EnvironmentError, why:
            errors.append((filename, str(why)))
    if errors:
        raise Error, errors
    # end fix for SIIMPC-598.2
    return


def getResolutionIndex(resolution):
    res = resolution
    if res == 10:
        return 0
    elif res == 20:
        return 1
    elif res == 60:
        return 2
    else:
        return False


def GetExtent(gt, cols, rows):
    ''' Return list of corner coordinates from a geotransform

        @type gt:   C{tuple/list}
        @param gt: geotransform
        @type cols:   C{int}
        @param cols: number of columns in the dataset
        @type rows:   C{int}
        @param rows: number of rows in the dataset
        @rtype:    C{[float,...,float]}
        @return:   coordinates of each corner
    '''
    ext = []
    xarr = [0, cols]
    yarr = [0, rows]

    for px in xarr:
        for py in yarr:
            x = gt[0] + (px * gt[1]) + (py * gt[2])
            y = gt[3] + (px * gt[4]) + (py * gt[5])
            ext.append([x, y])
        yarr.reverse()
    return ext


def ReprojectCoords(coords, src_srs, tgt_srs):
    ''' Reproject a list of x,y coordinates.
        @type geom:     C{tuple/list}
        @param geom:    List of [[x,y],...[x,y]] coordinates
        @type src_srs:  C{osr.SpatialReference}
        @param src_srs: OSR SpatialReference object
        @type tgt_srs:  C{osr.SpatialReference}
        @param tgt_srs: OSR SpatialReference object
        @rtype:         C{tuple/list}
        @return:        List of transformed [[x,y],...[x,y]] coordinates
    '''
    trans_coords = []
    transform = osr.CoordinateTransformation(src_srs, tgt_srs)
    for x, y in coords:
        x, y, z = transform.TransformPoint(x, y)
        trans_coords.append([x, y])
    return trans_coords


def transform_utm_to_wgs84(easting, northing, zone1, zone2):
    utm_coordinate_system = osr.SpatialReference()  # Create a new spatial reference object using a named parameter
    utm_coordinate_system.SetWellKnownGeogCS("WGS84")  # Set geographic coordinate system to handle lat/lon
    zone = zone1
    hemi = zone2
    # SIITBX-48:
    if (hemi == 'N'):  # N is Northern Hemisphere
        utm_coordinate_system.SetUTM(zone, 1)  # call sets detailed projection transformation parameters
    else:
        utm_coordinate_system.SetUTM(zone, 0)
    wgs84_coordinate_system = utm_coordinate_system.CloneGeogCS()  # Clone ONLY the geographic coordinate system
    # create transform component
    utm_to_wgs84_geo_transform = osr.CoordinateTransformation(utm_coordinate_system, wgs84_coordinate_system)
    return utm_to_wgs84_geo_transform.TransformPoint(easting, northing, 0)  # returns lon, lat, altitude


def transform_wgs84_to_utm(lon, lat):
    utm_coordinate_system = osr.SpatialReference()
    utm_coordinate_system.SetWellKnownGeogCS("WGS84")  # Set geographic coordinate system to handle lat/lon
    utm_coordinate_system.SetUTM(get_utm_zone(lon), is_northern(lat))
    wgs84_coordinate_system = utm_coordinate_system.CloneGeogCS()  # Clone ONLY the geographic coordinate system
    # create transform component
    wgs84_to_utm_geo_transform = osr.CoordinateTransformation(wgs84_coordinate_system, utm_coordinate_system)
    return wgs84_to_utm_geo_transform.TransformPoint(lon, lat, 0)  # returns easting, northing, altitude


def get_utm_zone(longitude):
    return (int(1 + (longitude + 180.0) / 6.0))


def is_northern(latitude):  # Determines if given latitude is a northern for UTM
    if (latitude < 0.0):
        return 0
    else:
        return 1


def getDayOfYear(dateStr):
    ''' returns the day of year from string.

        :param date
        :type date : str
        :return: day of year
        :rtype: unsigned int

    '''
    from datetime import datetime
    from calendar import isleap

    doy = datetime.strptime(dateStr, '%Y-%m-%dT%H:%M:%S.%fZ').timetuple().tm_yday
    year = datetime.strptime(dateStr, '%Y-%m-%dT%H:%M:%S.%fZ').timetuple().tm_year
    return doy, isleap(year)

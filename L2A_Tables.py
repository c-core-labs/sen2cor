#!/usr/bin/env python
'''
Created on Feb 24, 2012
@author: TPZV
modified for Sen2Cor 2.7.1
included fixes for: SIIMPC-792, SIIMPC-550, SIIMPC-887 and SIIMPC-944.
fix for SCOR-6: failure due to "Tile is crossing the international date line"
'''
import fnmatch
import subprocess
import tempfile, logging, shutil
import re
from L2A_Library import *
from time import sleep
import glymur
from PIL import Image
from skimage.measure import block_reduce
from skimage.transform import resize as skit_resize
from tables import *
from numpy import *
from tables.description import *
from shutil import copyfile, copytree
from scipy.ndimage.interpolation import zoom
from scipy.ndimage.filters import median_filter
from lxml import etree, objectify
from L2A_XmlParser import L2A_XmlParser

from osgeo.gdal_array import BandReadAsArray
import gdal

from multiprocessing import Lock, cpu_count
l = Lock()

try:
    from osgeo import gdal,osr
    from osgeo.gdalconst import *
    gdal.TermProgress = gdal.TermProgress_nocb
except ImportError:
    import gdal,osr
    from gdalconst import *

# SIITBX-47: to suppress user warning due to the fact that 
# http://trac.osgeo.org/gdal/ticket/5480 is not implemented
# in the current openJPEG driver for windows used by ANACONDA:
gdal.PushErrorHandler('CPLQuietErrorHandler')
gdal.UseExceptions()

class Particle(IsDescription):
    bandName = StringCol(8)
    projectionRef = StringCol(512)
    geoTransformation = Int32Col(shape=6)
    rasterXSize = UInt16Col()
    rasterYSize = UInt16Col()
    rasterCount = UInt8Col()

class L2A_Tables(object):
    def __init__(self, config):
        self._config = config
        self._logger = config.logger
        self._DEV0 = None
        self._SHELL = None
        self._firstInit = False

        AUX_DATA = 'AUX_DATA'
        IMG_DATA = 'IMG_DATA'
        QI_DATA = 'QI_DATA'
        GRANULE = 'GRANULE'
        
        self.aux_src = None
        self._tmpdir = ''

        if self._logger.level == logging.DEBUG:
            self._DEV0 = ''
            if os.name == 'posix':
                self._SHELL = True
            else:
                self._SHELL = False
        else:
            if os.name == 'posix':
                self._DEV0 = ' >/dev/null 2>&1'
                self._SHELL = True
            else:
                self._DEV0 = ''
                self._SHELL = False
        # Resolution:
        self._resolution = int(config.resolution)
        if(self._resolution == 10):
            self._bandIndex = [1,2,3,7]
            bandDir = 'R10m'
        elif(self._resolution == 20):
            self._bandIndex = [0,1,2,3,4,5,6,8,9,10,11,12]
            bandDir = 'R20m'
        elif(self._resolution == 60):
            self._bandIndex = [0,1,2,3,4,5,6,8,9,10,11,12]
            bandDir = 'R60m'
        
        if config.raw:
            self._L2A_ImageExtention = '.rawl'
        elif config.tif:
            self._L2A_ImageExtention = '.tif'
        else:
            self._L2A_ImageExtention = '.jp2'

        BANDS = bandDir

        if config.operationMode == 'TOOLBOX':
            L1C_TILE_ID = os.path.join(config.L1C_UP_DIR, GRANULE, config.L1C_TILE_ID)
            L2A_TILE_ID = os.path.join(config.L2A_UP_DIR, GRANULE, config.L2A_TILE_ID)
        else:
            L1C_TILE_ID = os.path.join(config.input_dir,config.L1C_TILE_ID)
            L2A_TILE_ID = os.path.join(config.output_dir, config.L2A_TILE_ID)

        if('L2A_CO_' in L2A_TILE_ID):
            self.logger.info('consolidated tile ' + config.L2A_TILE_ID + ': no entry in datastrip metadata generated')

        L1C_ImgDataDir = os.path.join(L1C_TILE_ID, IMG_DATA)
        self._L2A_ImgDataDir = os.path.join(L2A_TILE_ID, IMG_DATA)

        self._L1C_bandDir = L1C_ImgDataDir
        self._L2A_bandDir = os.path.join(self._L2A_ImgDataDir, BANDS)

        if not os.path.exists(self._L2A_bandDir):
            os.makedirs(self._L2A_bandDir)

        self._L1C_QualityMasksDir = os.path.join(L1C_TILE_ID, QI_DATA)
        self._L2A_QualityDataDir = os.path.join(L2A_TILE_ID, QI_DATA)
        self._L2A_AuxDataDir = os.path.join(L2A_TILE_ID, AUX_DATA)
        fm_short = 'AUX_ECMWFT'
        try:
            os.stat(self._L2A_AuxDataDir)
            self.aux_src = fm_short
        except:
            L1C_AUX = os.path.join(L1C_TILE_ID, AUX_DATA)
            L2A_AUX = os.path.join(L2A_TILE_ID, AUX_DATA)
            copytree(L1C_AUX, L2A_AUX)
            chmod_recursive(L2A_AUX, 0755)
            filelist = sorted(os.listdir(self._L2A_AuxDataDir))
            for filenameAux in filelist:
                fm_long = 'S2?_OPER_AUX_*'
                if fnmatch.fnmatch(filenameAux, fm_long):
                    os.rename(os.path.join(self._L2A_AuxDataDir, filenameAux),
                              os.path.join(self._L2A_AuxDataDir, fm_short))
                    self.aux_src=fm_short
                    break
                elif fnmatch.fnmatch(filenameAux, fm_short):
                    self.aux_src=fm_short
                    break
        try:
            os.stat(self._L2A_QualityDataDir)
            # 10 m was called before 20 m is processed:
            if self._resolution == 20 and not self.checkAotMapIsPresent(10):
                self._firstInit = True
        except:
            self._firstInit = True
            L1C_QI_DATA = os.path.join(L1C_TILE_ID, QI_DATA)
            L2A_QI_DATA = os.path.join(L2A_TILE_ID, QI_DATA)
            copytree(L1C_QI_DATA, L2A_QI_DATA)
            chmod_recursive(L2A_QI_DATA, 0755)

            filelist = sorted(os.listdir(self._L2A_QualityDataDir))
            fm_MSI = 'S2?_OPER_MSI_*'
            fm_MSK = 'S2?_OPER_MSK_*'
            fm_PVI = 'S2?_OPER_PVI_*'

            for fnIn in filelist:
                # Improvment for SIIMPC-1227 - UMW: no need for double check if '__' is replaced by '_':
                fnInR = fnIn.replace('__', '_')
                if fnmatch.fnmatch(fnIn, fm_MSI):
                    os.remove(os.path.join(self._L2A_QualityDataDir, fnIn))
                elif fnmatch.fnmatch(fnInR, fm_MSK):
                    fnInS = fnInR.split('_')
                    fnOut = fnInS[2] + '_' + fnInS[3] + '_' + fnInS[8] + '.gml'
                    os.rename(os.path.join(self._L2A_QualityDataDir, fnIn),
                              os.path.join(self._L2A_QualityDataDir, fnOut))
                elif fnmatch.fnmatch(fnInR, fm_PVI):
                    fnInS = fnInR.split('_')
                    fnOut = fnInS[8][:-4] + '_' + config.getDatatakeSensingStart() + '_' + fnInS[2] + ".jp2"
                    os.rename(os.path.join(self._L2A_QualityDataDir, fnIn),
                              os.path.join(self._L2A_QualityDataDir, fnOut))

        # fix for SIIMPC-556.1 UMW:
        try:
            os.stat(self._tmpdir)
        except os.error:
            self._tmpdir = tempfile.mkdtemp(dir = config.work_dir)
        # end fix for SIIMPC-556.1

        #
        # the File structure:
        #-------------------------------------------------------

        tile1cS = config.tile.split('_')
        S2A_L1C_mask_safe_standard = 'S2?_OPER_MSI_L1C_TL*'
        if (fnmatch.fnmatch(config.tile, S2A_L1C_mask_safe_standard) == True):
            if tile1cS[6] == '':
                L2A_TILE_ID = tile1cS[9] + '_' + config.getDatatakeSensingStart()
            else:
                L2A_TILE_ID = tile1cS[8] + '_' + config.getDatatakeSensingStart()
        else:
            L2A_TILE_ID = tile1cS[1] + '_' + config.getDatatakeSensingStart()

        if self._firstInit:
            L1C_Tile_PVI_File = os.path.join \
                (self._L2A_QualityDataDir, L2A_TILE_ID + '_PVI.jp2')
            if os.path.exists(L1C_Tile_PVI_File):
                indataset = glymur.Jp2k(L1C_Tile_PVI_File)
                self.config.geoboxPvi = indataset.box[3]
                os.remove(L1C_Tile_PVI_File)
            self._L2A_Tile_PVI_File = os.path.join\
            (self._L2A_QualityDataDir, L2A_TILE_ID + '_PVI' + self._L2A_ImageExtention)

        self._L2A_Tile_SCL_File = os.path.join(self._L2A_bandDir,
            L2A_TILE_ID + '_SCL_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._L2A_Tile_BND_File = os.path.join(self._L2A_bandDir,
            L2A_TILE_ID + '_BXX_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)

        self._L2A_Tile_VIS_File = os.path.join(self._L2A_bandDir        , L2A_TILE_ID + '_VIS_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._L2A_Tile_AOT_File = os.path.join(self._L2A_bandDir        , L2A_TILE_ID + '_AOT_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._L2A_Tile_WVP_File = os.path.join(self._L2A_bandDir        , L2A_TILE_ID + '_WVP_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._L2A_Tile_TCI_File = os.path.join(self._L2A_bandDir        , L2A_TILE_ID + '_TCI_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._L2A_Tile_CLD_File = os.path.join(self._L2A_QualityDataDir , 'MSK_CLDPRB_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._L2A_Tile_SNW_File = os.path.join(self._L2A_QualityDataDir , 'MSK_SNWPRB_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._L2A_Tile_DDV_File = os.path.join(self._L2A_QualityDataDir , 'MSK_DDVPXL_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._L2A_Tile_SDW_File = os.path.join(self._L2A_AuxDataDir     , L2A_TILE_ID + '_SDW_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._L2A_Tile_SLP_File = os.path.join(self._L2A_AuxDataDir     , L2A_TILE_ID + '_SLP_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._L2A_Tile_ASP_File = os.path.join(self._L2A_AuxDataDir     , L2A_TILE_ID + '_ASP_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._L2A_Tile_DEM_File = os.path.join(self._L2A_AuxDataDir     , L2A_TILE_ID + '_DEM_' + str(self._resolution) + 'm' + self._L2A_ImageExtention)
        self._imgdb = os.path.join(self.config.img_database_dir, L2A_TILE_ID + '_imgdb.h5')
        if not os.path.exists(self._imgdb):
            self.initTable(self._imgdb)
        self._resdb = os.path.join(self.config.res_database_dir, L2A_TILE_ID + '_resdb.h5')
        if not os.path.exists(self._resdb):
            self.initTable(self._resdb)
        self._acMode = False # default setting for scene classification

        # Geodata from image metadata:
        self._cornerCoordinates = None
        self._geoTransformation = None
        self._geoExtent = None
        self._projectionRef = None

        # Band Names:
        self._bandNames = ['B01','B02','B03','B04','B05','B06','B07','B08','B8A',\
                        'B09','B10','B11','B12','DEM','SCL','SNW','CLD','AOT',\
                        'WVP','VIS','SCM','PRV','ILU','SLP','ASP','HAZ','SDW',\
                        'DDV','HCW','ELE', 'PWC', 'MSL', 'OZO', 'TCI', 'WBI', 'LCM', 'SNC',\
                        'RDEM','RSCL','RSNW','RCLD','RAOT','RWVP','RDDV','RTCI'] # last line: band names for 20 to 60 m downsampling

        # the mapping of the channels and bands
        self._B01 = 0
        self._B02 = 1
        self._B03 = 2
        self._B04 = 3
        self._B05 = 4
        self._B06 = 5
        self._B07 = 6
        self._B08 = 7
        self._B8A = 8
        self._B09 = 9
        self._B10 = 10
        self._B11 = 11
        self._B12 = 12
        self._DEM = 13
        self._SCL = 14
        self._SNW = 15
        self._CLD = 16
        self._AOT = 17
        self._WVP = 18
        self._VIS = 19
        self._SCM = 20
        self._PRV = 21
        self._ILU = 22
        self._SLP = 23
        self._ASP = 24
        self._HAZ = 25
        self._SDW = 26
        self._DDV = 27
        self._HCW = 28
        self._ELE = 29
        self._PWC = 30
        self._MSL = 31
        self._OZO = 32
        self._TCI = 33
        self._WBI = 34
        self._LCM = 35
        self._SNC = 36

        self.logger.debug('Module L2A_Tables initialized with resolution %d' % self._resolution)

    def get_ac_mode(self):
        return self._acMode


    def set_ac_mode(self, value):
        self._acMode = value


    def del_ac_mode(self):
        del self._acMode


    def get_logger(self):
        return self._logger


    def set_logger(self, value):
        self._logger = value


    def del_logger(self):
        del self._logger


    def get_corner_coordinates(self):
        return self._cornerCoordinates


    def get_geo_extent(self):
        return self._geoExtent


    def get_projection(self):
        return self._projection


    def set_corner_coordinates(self, value):
        self._cornerCoordinates = value


    def set_geo_extent(self, value):
        self._geoExtent = value


    def set_projection(self, value):
        self._projection = value


    def del_corner_coordinates(self):
        del self._cornerCoordinates


    def del_geo_extent(self):
        del self._geoExtent


    def del_projection(self):
        del self._projection


    def getBandNameFromIndex(self, index):
        return self._bandNames[index]


    def get_band_index(self):
        return self._bandIndex


    def get_db_name(self):
        return self._dbName


    def set_band_index(self, value):
        self._bandIndex = value


    def set_db_name(self, value):
        self._dbName = value


    def del_band_index(self):
        del self._bandIndex


    def del_db_name(self):
        del self._dbName

        # end mapping of channels and bands

    def __del__(self):
        try:
            shutil.rmtree(self._tmpdir)
        except:
            pass

        self.logger.debug('Module L2A_Tables deleted')

    def get_config(self):
        return self._config


    def set_config(self, value):
        self._config = value


    def del_config(self):
        del self._config


    def get_b01(self):
        return self._B01


    def get_b02(self):
        return self._B02


    def get_b03(self):
        return self._B03


    def get_b04(self):
        return self._B04


    def get_b05(self):
        return self._B05


    def get_b06(self):
        return self._B06


    def get_b07(self):
        return self._B07


    def get_b08(self):
        return self._B08


    def get_b8a(self):
        return self._B8A


    def get_b09(self):
        return self._B09


    def get_b10(self):
        return self._B10


    def get_b11(self):
        return self._B11


    def get_b12(self):
        return self._B12


    def get_dem(self):
        return self._DEM


    def get_scl(self):
        return self._SCL


    def get_qsn(self):
        return self._SNW


    def get_qcl(self):
        return self._CLD


    def get_aot(self):
        return self._AOT


    def get_wvp(self):
        return self._WVP


    def get_vis(self):
        return self._VIS


    def get_scm(self):
        return self._SCM


    def get_prv(self):
        return self._PRV
    
   
    def get_pwc(self):
        return self._PWC


    def get_msl(self):
        return self._MSL


    def get_ozo(self):
        return self._OZO


    def get_tci(self):
        return self._TCI


    def get_wbi(self):
        return self._WBI


    def get_snc(self):
        return self._SNC


    def get_lcm(self):
        return self._LCM


    def set_b01(self, value):
        self._B01 = value


    def set_b02(self, value):
        self._B02 = value


    def set_b03(self, value):
        self._B03 = value


    def set_b04(self, value):
        self._B04 = value


    def set_b05(self, value):
        self._B05 = value


    def set_b06(self, value):
        self._B06 = value


    def set_b07(self, value):
        self._B07 = value


    def set_b08(self, value):
        self._B08 = value


    def set_b8a(self, value):
        self._B8A = value


    def set_b09(self, value):
        self._B09 = value


    def set_b10(self, value):
        self._B10 = value


    def set_b11(self, value):
        self._B11 = value


    def set_b12(self, value):
        self._B12 = value


    def set_dem(self, value):
        self._DEM = value


    def set_scl(self, value):
        self._SCL = value


    def set_qsn(self, value):
        self._SNW = value


    def set_qcl(self, value):
        self._CLD = value


    def set_aot(self, value):
        self._AOT = value


    def set_wvp(self, value):
        self._WVP = value


    def set_vis(self, value):
        self._VIS = value


    def set_scm(self, value):
        self._SCM = value


    def set_prv(self, value):
        self._PRV = value
        
        
    def set_pwc(self, value):
        self._PWC = value


    def set_msl(self, value):
        self._MSL = value


    def set_ozo(self, value):
        self._OZO = value


    def set_tci(self, value):
        self._TCI = value


    def set_wbi(self, value):
        self._WBI = value


    def set_lcm(self, value):
        self._LCM = value


    def set_snc(self, value):
        self._SNC = value


    def del_b01(self):
        del self._B01


    def del_b02(self):
        del self._B02


    def del_b03(self):
        del self._B03


    def del_b04(self):
        del self._B04


    def del_b05(self):
        del self._B05


    def del_b06(self):
        del self._B06


    def del_b07(self):
        del self._B07


    def del_b08(self):
        del self._B08


    def del_b8a(self):
        del self._B8A


    def del_b09(self):
        del self._B09


    def del_b10(self):
        del self._B10


    def del_b11(self):
        del self._B11


    def del_b12(self):
        del self._B12


    def del_dem(self):
        del self._DEM


    def del_scl(self):
        del self._SCL


    def del_qsn(self):
        del self._SNW


    def del_qcl(self):
        del self._CLD


    def del_aot(self):
        del self._AOT


    def del_wvp(self):
        del self._WV


    def del_vis(self):
        del self._VIS


    def del_scm(self):
        del self._SCM


    def del_prv(self):
        del self._PRV
        
        
    def del_pwc(self):
        del self._PWC


    def del_msl(self):
        del self._MSL


    def del_ozo(self):
        del self._OZO


    def del_tci(self):
        del self._TCI


    def del_wbi(self):
        del self._WBI


    def del_lcm(self):
        del self._LCM


    def del_snc(self):
        del self._SNC


    def get_ilu(self):
        return self._ILU


    def get_slp(self):
        return self._SLP


    def get_asp(self):
        return self._ASP


    def set_ilu(self, value):
        self._ILU = value


    def set_slp(self, value):
        self._SLP = value


    def set_asp(self, value):
        self._ASP = value


    def del_ilu(self):
        del self._ILU


    def del_slp(self):
        del self._SLP


    def del_asp(self):
        del self._ASP


    def get_sdw(self):
        return self._SDW


    def set_sdw(self, value):
        self._SDW = value


    def del_sdw(self):
        del self._SDW


    def get_ddv(self):
        return self._DDV


    def set_ddv(self, value):
        self._DDV = value


    def del_ddv(self):
        del self._DDV

    def get_hcw(self):
        return self._HCW


    def get_ele(self):
        return self._ELE


    def set_hcw(self, value):
        self._HCW = value


    def set_ele(self, value):
        self._ELE = value


    def del_hcw(self):
        del self._HCW


    def del_ele(self):
        del self._ELE


    B01 = property(get_b01, set_b01, del_b01, "B01's docstring")
    B02 = property(get_b02, set_b02, del_b02, "B02's docstring")
    B03 = property(get_b03, set_b03, del_b03, "B03's docstring")
    B04 = property(get_b04, set_b04, del_b04, "B04's docstring")
    B05 = property(get_b05, set_b05, del_b05, "B05's docstring")
    B06 = property(get_b06, set_b06, del_b06, "B06's docstring")
    B07 = property(get_b07, set_b07, del_b07, "B07's docstring")
    B08 = property(get_b08, set_b08, del_b08, "B08's docstring")
    B8A = property(get_b8a, set_b8a, del_b8a, "B8A's docstring")
    B09 = property(get_b09, set_b09, del_b09, "B09's docstring")
    B10 = property(get_b10, set_b10, del_b10, "B10's docstring")
    B11 = property(get_b11, set_b11, del_b11, "B11's docstring")
    B12 = property(get_b12, set_b12, del_b12, "B12's docstring")
    DEM = property(get_dem, set_dem, del_dem, "DEM's docstring")
    SCL = property(get_scl, set_scl, del_scl, "SCL's docstring")
    SNW = property(get_qsn, set_qsn, del_qsn, "SNW's docstring")
    CLD = property(get_qcl, set_qcl, del_qcl, "CLD's docstring")
    AOT = property(get_aot, set_aot, del_aot, "AOT's docstring")
    WVP = property(get_wvp, set_wvp, del_wvp, "WVP's docstring")
    VIS = property(get_vis, set_vis, del_vis, "VIS's docstring")
    SCM = property(get_scm, set_scm, del_scm, "SCM's docstring")
    PRV = property(get_prv, set_prv, del_prv, "PRV's docstring")
    PWC = property(get_pwc, set_pwc, del_pwc, "PWC's docstring")
    MSL = property(get_msl, set_msl, del_msl, "MSL's docstring")
    OZO = property(get_ozo, set_ozo, del_ozo, "OZO's docstring")
    TCI = property(get_tci, set_tci, del_tci, "TCI's docstring")
    WBI = property(get_wbi, set_wbi, del_wbi, "WBI's docstring")
    LCM = property(get_lcm, set_lcm, del_lcm, "LCM's docstring")
    SNC = property(get_snc, set_snc, del_snc, "SNC's docstring")
    ILU = property(get_ilu, set_ilu, del_ilu, "ILU's docstring")
    SLP = property(get_slp, set_slp, del_slp, "SLP's docstring")
    SDW = property(get_sdw, set_sdw, del_sdw, "SDW's docstring")
    ASP = property(get_asp, set_asp, del_asp, "ASP's docstring")
    DDV = property(get_ddv, set_ddv, del_ddv, "DDV's docstring")
    HCW = property(get_hcw, set_hcw, del_hcw, "HCW's docstring")
    ELE = property(get_ele, set_ele, del_ele, "ELE's docstring")
    config = property(get_config, set_config, del_config, "config's docstring")
    logger = property(get_logger, set_logger, del_logger, "logger's docstring")
    bandIndex = property(get_band_index, set_band_index, del_band_index, "bandIndex's docstring")
    dbName = property(get_db_name, set_db_name, del_db_name, "dbName's docstring")
    cornerCoordinates = property(get_corner_coordinates, set_corner_coordinates, del_corner_coordinates, "cornerCoordinates's docstring")
    geoExtent = property(get_geo_extent, set_geo_extent, del_geo_extent, "geoExtent's docstring")
    projection = property(get_projection, set_projection, del_projection, "projection's docstring")
    acMode = property(get_ac_mode, set_ac_mode, del_ac_mode, "acMode's docstring")


    def checkAotMapIsPresent(self, resolution):
        sourceDir = os.path.join(self._L2A_ImgDataDir, 'R' + str(resolution) + 'm')
        try:
            dirs = sorted(os.listdir(sourceDir))
            filemask = '*_AOT_???.???'

            for filename in dirs:
                if fnmatch.fnmatch(filename, filemask):
                    return True
            return False
        except:
            return False

    def checkB2isPresent(self, resolution):
        sourceDir = os.path.join(self._L2A_ImgDataDir, 'R' + str(resolution) + 'm')
        try:
            dirs = sorted(os.listdir(sourceDir))
            filemask = '*_B02_??m.jp2'

            for filename in dirs:
                if fnmatch.fnmatch(filename, filemask):
                    return True
            return False
        except:
            return False


    def checkBandCount(self):
        sourceDir = self._L1C_bandDir
        dirs = sorted(os.listdir(sourceDir))
        bandIndex = self.bandIndex
        bandCount = 0
        for i in bandIndex:
            for filename in dirs:
                bandName = self.getBandNameFromIndex(i)
                filemask = '*_%3s.jp2' % bandName
                if not fnmatch.fnmatch(filename, filemask):
                    continue
                bandCount += 1
                break
        if len(bandIndex) > bandCount:
            return False
        return True


    def importBandList(self):
        # convert JPEG-2000 input files to H5 file format
        sourceDir = self._L1C_bandDir
        rasterX = False
        if self._resolution == 10:
            self.config.timestamp('L2A_Tables: remove unused bands for 10m resolution')
            for i in [0, 4, 5, 6, 8, 9, 10, 11, 12]:
                if self.hasBand(i):
                    self.removeBandImg(i);
        self.config.timestamp('L2A_Tables: start import')
        dirs = sorted(os.listdir(sourceDir))
        bandIndex = self.bandIndex
        for i in bandIndex:
            for filename in dirs:
                bandName = self.getBandNameFromIndex(i)
                filemask = '*_%3s.jp2' % bandName
                if not fnmatch.fnmatch(filename, filemask):
                    continue
                if(rasterX == False):
                    self.setCornerCoordinates()
                    rasterX = True
                res = self.importBandImg(i, os.path.join(sourceDir, filename))
                if not res:
                    return False
                break

        upsampling = False
        if(self._resolution == 10):
            # 10m bands only: perform an up sampling of SCL, AOT, WVP, and VIS from 20 m channels to 10
            # https://jira.acri-cwa.fr/browse/SIIMPC-1300 :
            self.logger.info('perform up sampling of SCL, AOT and VIS from 20m channels to 10m')
            srcResolution = '_20m'
            channels = [14,17,18,19]
            sourceDir = self._L2A_bandDir.replace('R10m', 'R20m')
            upsampling = True
        
        if upsampling:
            tmpdb = self._resdb + '_tmp'
            self.initTable(tmpdb)
            dirs = sorted(os.listdir(sourceDir))
            for i in channels:
                for filename in dirs:
                    bandName = self.getBandNameFromIndex(i)
                    # if (bandName == 'VIS') or (bandName == 'AOT') or (bandName == 'WVP':
                    filemask = '*_' + bandName + srcResolution + self._L2A_ImageExtention
                    if not fnmatch.fnmatch(filename, filemask):
                        continue
                    res = self.importBandRes(i, os.path.join(sourceDir, filename))
                    if bandName == 'VIS':
                        try:
                            os.remove(os.path.join(sourceDir, filename))
                        except:
                            pass
                    if not res:
                        return False
                    break
            if (os.path.isfile(tmpdb)):
                if (os.path.isfile(self._resdb)):
                    os.remove(self._resdb)
                os.rename(tmpdb, self._resdb)
                self.logger.info("renaming hd5 result database (size: %s)" % os.path.getsize(self._resdb))

        self.dem = False
        demDir =  self.config.demDirectory
        if demDir == 'NONE':
            self.logger.info('DEM directory not specified, flat surface is used')
            return True

        # check if DEM is a DTED type, these files must exist in the given directory:
        if self.isDted():
            # yes it is, run dem preparation for DTED:
            demfile = self.gdalDEM_dted()
            if(demfile == False):
                self.config.demDirectory = 'NONE'
                self.config.demType = 'NONE'
                # continue with flat surface ...
                return True
            else:
                self.config.demType = 'DTED'
        else: # run DEM preparation for SRTM:
            demfile = self.gdalDEM_srtm()
            if(demfile == False):
                self.config.demDirectory = 'NONE'
                self.config.demType = 'NONE'
                # continue with flat surface ...
                return True
            else:
                self.config.demType = 'SRTM'
        # generate hill shadow, slope and aspect using DEM:
        if(self.gdalDEM_Shade(demfile) == False):
            self.logger.fatal('shell execution error generating DEM shadow')
            return False

        if(self.gdalDEM_Slope(demfile) == False):
            self.logger.fatal('shell execution error generating DEM slope')
            return False

        if self._resolution > 10:
            if(self.gdalDEM_Aspect(demfile) == False):
                self.logger.fatal('shell execution error generating DEM aspect')
                return False

        try:
            os.remove(demfile)
        except:
            pass

        if(self._resolution == 10):
            try:
                shutil.rmtree(self._tmpdir)
            except:
                pass
            return True

        else:
            if(self.gdalCCI_wb() == False):
                # Continue without ESA CCI Water Bodies (150m) a priori information
                return True

            if(self.gdalCCI_lccs() == False):
                # Continue without ESA CCI Land Cover (300m) a priori information (urban)
                return True

            if(self.gdalCCI_snowc() == False):
                # Continue without ESA CCI Snow Condition (500m) a priori information
                return True

        try:
            shutil.rmtree(self._tmpdir)
        except:
            pass

        self.config.timestamp('L2A_Tables: stop import')

        return True

    # fix for SIIMPC-550.2, UMW:
    def isDted(self):
        demDir = os.path.join(self.config.home, self.config.demDirectory)
        filemask = 'e*.dt1'
        if os.path.exists(demDir):
            files = sorted(os.listdir(demDir))
            for filename in files:
                if fnmatch.fnmatch(filename, filemask):
                    return True
        return False


    def setCornerCoordinates(self):
        # get the target resolution and metadata for the resampled bands below:
        xp = L2A_XmlParser(self.config, 'T2A')           
        tg = xp.getTree('Geometric_Info', 'Tile_Geocoding')
        nrows = self.config.nrows
        ncols = self.config.ncols
        idx = getResolutionIndex(self._resolution)
        ulx = tg.Geoposition[idx].ULX
        uly = tg.Geoposition[idx].ULY
        res = float32(self._resolution)
        geoTransformation = [ulx,res,0.0,uly,0.0,-res]
        extent = GetExtent(geoTransformation, ncols, nrows)
        self._cornerCoordinates = asarray(extent)
        return


    def getAuxData(self, bandIndex):
        '''
        PWC (Precipitable Water Content), Grib Unit [kg/m^2]
        MSL (Mean Sea Level pressure),    Grib Unit [Pa]
        OZO (Ozone),                      Grib Unit [kg/m^2]
        
        calculation for Ozone according to R. Richter (20/1/2016):
        ----------------------------------------------------------
        GRIB_UNIT = [kg/m^2]
        standard ozone column is 300 DU (Dobson Units),
        equals to an air column of 3 mm at STP (standard temperature (0 degree C) and pressure of 1013 mbar).
        
        Thus, molecular weight of O3 (M = 48): 2.24 g (equals to 22.4 liter at STP)
        
        300 DU = 3 mm  (equals to (0.3*48 / 2.24) [g/m^2])
         = 6.428 [g/m^2] = 6.428 E-3 [kg/m^2]        

        Example:
        
        ozone (GRIB) = 0.005738 (equals to DU = 300 * 0.005738/6.428 E-3)
        ozone (DU)   = 267.4 DU
        
        Thus, ozone GRIB will be weighted with factor 155.5694 (equals to 1/6.428 E-3)
        in order to receive ozone in DU
        '''
        auxBands = [self.PWC, self.MSL, self.OZO]
        if bandIndex in auxBands == False:
            self.logger.error('wrong band index for aux data')
            return False
        
        bandIndex -= 29 # bandIndex starts at 30
        ozoneFactor = 155.5694 # 1/6.428 E-3
        standardOzoneColumn = 300.0
        
        straux_src = os.path.join(self._L2A_AuxDataDir, self.aux_src)
        curdir = os.path.curdir
        head, tail = os.path.split(straux_src)
        l.acquire()
        os.chdir(head)
        arr = False
        while True:
            try:
                dataSet = gdal.Open(tail, GA_ReadOnly)
                band = dataSet.GetRasterBand(bandIndex)
                arr = BandReadAsArray(band)
                if bandIndex == 3: # recalculate to 300 DU:
                    arr = arr * standardOzoneColumn * ozoneFactor
                break
            except:
                self.logger.error('error in reading ozone values from aux data')
            finally:
                os.chdir(curdir)
                l.release()
                return arr
    
    def gdalDEM_dted(self):
        import scipy.misc
        demDir = self.config.demDirectory
        if demDir == 'NONE':
            self.logger.info('DEM directory not specified, flat surface is used')
            return False

        self.logger.info('Start DEM alignment for tile')
        sourceDir = os.path.join(self.config.home, demDir)

        xy = self.cornerCoordinates
        xp = L2A_XmlParser(self.config, 'T2A')
        tg = xp.getTree('Geometric_Info', 'Tile_Geocoding')
        hcsName = tg.HORIZONTAL_CS_NAME.text
        zone = hcsName.split()[4]
        zone1 = int(zone[:-1])
        zone2 = zone[-1:].upper()
        lonMin, latMin, dummy = transform_utm_to_wgs84(xy[1,0], xy[1,1], zone1, zone2)
        lonMax, latMax, dummy = transform_utm_to_wgs84(xy[3,0], xy[3,1], zone1, zone2)
        # Fix for SIIMPC-573, 944 UMW - lat_cen, lon_cen is obsolete as check is moved within loop
        lonMin = int(round(lonMin))
        lonMax = int(round(lonMax))
        latMin = int(round(latMin))
        latMax = int(round(latMax))
        dtedf_src = ''

        filelist = sorted(os.listdir(sourceDir))
        found = False

        # Fix for SIIMPC-944 VD-JL - International Date Line handling for DEM mosaicking
        if lonMin <= lonMax:
            lons = range(lonMin - 1, lonMax + 1)
        else:
            lons = range(lonMin-1, 180) + range(-180, lonMax+2) # gives [178, 179, -178, -179, -180] for lonMin=179 and lonMax=-179
            self.logger.error('This tile is crossing the international date line, a particular processing is performed')

        for lon in lons:
            for lat in range(latMin - 1, latMax + 1):
                if lon < 0:
                    lonMask = 'w'
                else:
                    lonMask = 'e'
                if lat < 0:
                    latMask = 's'
                else:
                    latMask = 'n'

                file_mask = '%s%03d_%s%02d.dt1' % (lonMask, abs(lon), latMask, abs(lat))
                # end fix for SIIMPC-573, 944
                for filename in filelist:
                    if(fnmatch.fnmatch(filename, file_mask) == True):
                        found = True
                        dtedf_src += ' ' + os.path.join(sourceDir, filename)
                        break

        if not found:
            self.logger.info('DEM not found, flat surface is used')
            return False

        if lonMin <= lonMax: # Fix for SIIMPC-944 VD-JL - International Date Line handling for DEM mosaicking
            command = 'gdalwarp -r bilinear ' # fix for SIIMPC-1006.2 UMW
        else:
            command = 'gdalwarp -r bilinear -t_srs EPSG:4326 --config CENTER_LONG 180 '

        arguments = '-ot Int16 '
        # fix for SIIMPC-556.2 UMW:
        tmpDir = self._tmpdir

        dtedf_dest = os.path.join(tmpDir, 'dted_' + self.config.L2A_TILE_ID + '_src.tif')
        callstr = command + arguments + dtedf_src + ' ' + dtedf_dest + self._DEV0
        l.acquire()
        try:
            p = subprocess.Popen(callstr, shell=self._SHELL)
            p.wait()
        except:
            self.logger.fatal('shell execution error using gdalwarp')
            os.remove(dtedf_src)
            return False
        finally:
            l.release()

        dtedf_src = dtedf_dest
        dtedf_dest = os.path.join(tmpDir, 'dted_' + self.config.L2A_TILE_ID + '_dem.tif')
        hcsCode = tg.HORIZONTAL_CS_CODE.text
        t_srs = '-t_srs ' + hcsCode

        te = ' -te %f %f %f %f' % (xy[0,0], xy[2,1], xy[2,0], xy[0,1])
        tr = ' -tr %d %d' % (self._resolution, self._resolution)
        t_warp = te + tr + ' -r cubicspline '

        command = 'gdalwarp '
        # fix for SIIMPC-792, JL:
        arguments = '-ot Float32 '
        # end of fix for SIIMPC-792
        callstr = command + arguments + t_srs + t_warp + dtedf_src + ' ' + dtedf_dest + self._DEV0
        # fix for SIIMPC-563.1, UMW:
        l.acquire()
        # end fix for SIIMPC-563.1
        try:
            p =subprocess.Popen(callstr, shell=self._SHELL)
            p.wait()
        except:
            self.logger.fatal('Error reading DEM, flat surface will be used')
            os.remove(dtedf_src)
            return False
        finally:
            l.release()

        # fix for SIIMPC-792, JL:
        command = 'gdal_translate '
        arguments = '-ot Int16 '
        dtedf_dst_int16 = os.path.join(tmpDir, 'dted_' + self.config.L2A_TILE_ID + '_dem_int16.tif')
        callstr = command + arguments + dtedf_dest + ' ' + dtedf_dst_int16 + self._DEV0
        l.acquire()
        try:
            p = subprocess.Popen(callstr, shell=self._SHELL)
            p.wait()
        except:
            self.logger.fatal('Error reading DEM, flat surface will be used')
            os.remove(dtedf_dest)
            return False
        finally:
            l.release()
        # end of fix for SIIMPC-792

        self.importBandRes(self.DEM, dtedf_dst_int16) # fix for SIIMPC-792, JL
        # fix for SIIMPC-563.2, UMW:
        os.remove(dtedf_src)
        # end fix for SIIMPC-563.2

        # fix for SIIMPC-792, JL
        os.remove(dtedf_dst_int16)
        # end of fix for SIIMPC-792

        self.logger.info('DEM received and prepared')
        return dtedf_dest


    def gdalCCI_wb(self):
        esacciWaterBodies = self.config.esacciWaterBodiesReference
        esacciWaterBodies = os.path.join(self.config.auxDir, esacciWaterBodies)
        if((os.path.isfile(esacciWaterBodies)) == False):
            self.logger.warning('ESA CCI Water Bodies map not present, water detection will be performed without a priori information')
            return True

        xy = self.cornerCoordinates
        xp = L2A_XmlParser(self.config, 'T2A')
        tg = xp.getTree('Geometric_Info', 'Tile_Geocoding')
        hcsName = tg.HORIZONTAL_CS_NAME.text
        zone = hcsName.split()[4]
        zone1 = int(zone[:-1])
        zone2 = zone[-1:].upper()
        lonMin, latMin, dummy = transform_utm_to_wgs84(xy[1,0], xy[1,1], zone1, zone2)
        lonMax, latMax, dummy = transform_utm_to_wgs84(xy[3,0], xy[3,1], zone1, zone2)

        tmpDir = self._tmpdir

        # step 1: check if the S2 tile crosses the International Date Line:

        if lonMin <= lonMax:
            command = 'gdalwarp '
            arguments = ''
        else:
            self.logger.warning('International Date Line is crossed, ESA CCI map reframing is performed')

            ymin = clip(latMin - 0.5, -90.0, 90.0)
            ymax = clip(latMax + 0.5, -90.0, 90.0)

            cci_wb_dst_east = os.path.join(tmpDir, 'cci_wb' + self.config.L2A_TILE_ID + '_{0}m_east.tif'.format(self._resolution))
            cci_wb_dst_west = os.path.join(tmpDir, 'cci_wb' + self.config.L2A_TILE_ID + '_{0}m_west.tif'.format(self._resolution))
            cci_wb_dst_dateline = os.path.join(tmpDir, 'cci_wb' + self.config.L2A_TILE_ID + '_{0}m_dateline.tif'.format(self._resolution))

            command = 'gdalwarp '
            arguments = ' -te 178.5 {0} 180 {1} '.format(ymin, ymax)
            callstr = command + arguments + esacciWaterBodies + ' ' + cci_wb_dst_east + self._DEV0

            #self.logger.warning(callstr)

            l.acquire()
            try:
                p = subprocess.Popen(callstr, shell=self._SHELL)
                p.wait()
            except:
                self.logger.warning('Cannot perform reframing step 1, no water bodies a priori information will be used')
                return False
            finally:
                l.release()

            arguments = ' -te -180 {0} -178.5 {1} '.format(ymin, ymax)
            callstr = command + arguments + esacciWaterBodies + ' ' + cci_wb_dst_west + self._DEV0

            l.acquire()
            try:
                p = subprocess.Popen(callstr, shell=self._SHELL)
                p.wait()
            except:
                self.logger.warning('Cannot perform reframing step 2, no water bodies a priori information will be used')
                return False
            finally:
                l.release()

            arguments = ' -t_srs EPSG:4326 --config CENTER_LONG 180 '
            callstr = command + arguments + cci_wb_dst_west + ' ' + cci_wb_dst_east + ' ' + cci_wb_dst_dateline + self._DEV0

            l.acquire()
            try:
                p = subprocess.Popen(callstr, shell=self._SHELL)
                p.wait()
            except:
                self.logger.warning('Cannot perform reframing step 3, no water bodies a priori information will be used')
                return False
            finally:
                l.release()

            esacciWaterBodies = cci_wb_dst_dateline
            command = 'gdalwarp '
            arguments = ''

        # step 2: extraction and reprojection into S2 tile geometry:
        hcsCode = tg.HORIZONTAL_CS_CODE.text
        t_srs = '-t_srs ' + hcsCode

        te = ' -te %f %f %f %f' % (xy[0,0], xy[2,1], xy[2,0], xy[0,1])
        tr = ' -tr %d %d' % (self._resolution, self._resolution)
        t_warp = te + tr + ' -r cubicspline '

        cci_wb_dst = os.path.join(tmpDir, 'cci_wb' + self.config.L2A_TILE_ID + '_{0}m.tif'.format(self._resolution))
        callstr = command + arguments + t_srs + t_warp + esacciWaterBodies + ' ' + cci_wb_dst + self._DEV0
        l.acquire()
        try:
            p = subprocess.Popen(callstr, shell=self._SHELL)
            p.wait()
        except:
            self.logger.warning('Cannot read esa cci, no water bodies a priori information will be used')
            return False
        finally:
            l.release()

        self.importBandRes(self.WBI, cci_wb_dst)
        try:
            os.remove(cci_wb_dst)
            os.remove(cci_wb_dst_east)
            os.remove(cci_wb_dst_west)
            os.remove(cci_wb_dst_dateline)
        except:
            pass

        self.logger.info('ESA CCI Water Bodies received and prepared')
        return cci_wb_dst


    def gdalCCI_lccs(self):
        esacciLandCover = self.config.esacciLandCoverReference
        esacciLandCover = os.path.join(self.config.auxDir, esacciLandCover)
        if((os.path.isfile(esacciLandCover)) == False):
            self.logger.warning('ESA CCI Land Cover map not present, cloud detection over urban areas will be performed without a priori information')
            return True

        xy = self.cornerCoordinates
        xp = L2A_XmlParser(self.config, 'T2A')
        tg = xp.getTree('Geometric_Info', 'Tile_Geocoding')
        hcsName = tg.HORIZONTAL_CS_NAME.text
        zone = hcsName.split()[4]
        zone1 = int(zone[:-1])
        zone2 = zone[-1:].upper()
        lonMin, latMin, dummy = transform_utm_to_wgs84(xy[1,0], xy[1,1], zone1, zone2)
        lonMax, latMax, dummy = transform_utm_to_wgs84(xy[3,0], xy[3,1], zone1, zone2)

        tmpDir = self._tmpdir

        # step 1: check if the S2 tile crosses the International Date Line:

        if lonMin <= lonMax:
            command = 'gdalwarp '
            arguments = ''
        else:
            self.logger.warning('International Date Line is crossed, ESA CCI map reframing is performed')

            ymin = clip(latMin - 0.5, -90.0, 90.0)
            ymax = clip(latMax + 0.5, -90.0, 90.0)

            cci_lccs_dst_east = os.path.join(tmpDir, 'cci_lccs' + self.config.L2A_TILE_ID + '_{0}m_east.tif'.format(self._resolution))
            cci_lccs_dst_west = os.path.join(tmpDir, 'cci_lccs' + self.config.L2A_TILE_ID + '_{0}m_west.tif'.format(self._resolution))
            cci_lccs_dst_dateline = os.path.join(tmpDir, 'cci_lccs' + self.config.L2A_TILE_ID + '_{0}m_dateline.tif'.format(self._resolution))

            command = 'gdalwarp '
            arguments = ' -te 178.5 {0} 180 {1} '.format(ymin, ymax)
            callstr = command + arguments + esacciLandCover + ' ' + cci_lccs_dst_east + self._DEV0

            #self.logger.warning(callstr)

            l.acquire()
            try:
                p = subprocess.Popen(callstr, shell=self._SHELL)
                p.wait()
            except:
                self.logger.warning('Cannot perform reframing step 1, no water bodies a priori information will be used')
                return False
            finally:
                l.release()

            arguments = ' -te -180 {0} -178.5 {1} '.format(ymin, ymax)
            callstr = command + arguments + esacciLandCover + ' ' + cci_lccs_dst_west + self._DEV0

            l.acquire()
            try:
                p = subprocess.Popen(callstr, shell=self._SHELL)
                p.wait()
            except:
                self.logger.warning('Cannot perform reframing step 2, no water bodies a priori information will be used')
                return False
            finally:
                l.release()

            arguments = ' -t_srs EPSG:4326 --config CENTER_LONG 180 '
            callstr = command + arguments + cci_lccs_dst_west + ' ' + cci_lccs_dst_east + ' ' + cci_lccs_dst_dateline + self._DEV0

            l.acquire()
            try:
                p = subprocess.Popen(callstr, shell=self._SHELL)
                p.wait()
            except:
                self.logger.warning('Cannot perform reframing step 3, no water bodies a priori information will be used')
                return False
            finally:
                l.release()

            esacciLandCover = cci_lccs_dst_dateline
            command = 'gdalwarp '
            arguments = ''

        # step 2: extraction and reprojection into S2 tile geometry:
        hcsCode = tg.HORIZONTAL_CS_CODE.text
        t_srs = '-t_srs ' + hcsCode

        te = ' -te %f %f %f %f' % (xy[0,0], xy[2,1], xy[2,0], xy[0,1])
        tr = ' -tr %d %d' % (self._resolution, self._resolution)
        #t_warp = te + tr + ' -r cubicspline '
        t_warp = te + tr + ' -r near '

        cci_lccs_dst = os.path.join(tmpDir, 'cci_lccs_' + self.config.L2A_TILE_ID + '_{0}m.tif'.format(self._resolution))
        callstr = command + arguments + t_srs + t_warp + esacciLandCover + ' ' + cci_lccs_dst + self._DEV0
        l.acquire()
        try:
            p = subprocess.Popen(callstr, shell=self._SHELL)
            p.wait()
        except:
            self.logger.warning('Cannot read esa cci lccs, no land cover a priori information will be used')
            return False
        finally:
            l.release()

        self.importBandRes(self.LCM, cci_lccs_dst)
        try:
            os.remove(cci_lccs_dst)
            os.remove(cci_lccs_dst_east)
            os.remove(cci_lccs_dst_west)
            os.remove(cci_lccs_dst_dateline)
        except:
            pass

        self.logger.info('ESA CCI Land Cover map prepared')
        return cci_lccs_dst


    def gdalCCI_snowc(self):
        from datetime import datetime
        import glob
        doy_acq, isLeap = getDayOfYear(self.config.acquisitionDate)
        esacciSnowConditionDir = self.config.esacciSnowConditionDirReference
        esacciSnowConditionDir = os.path.join(self.config.auxDir, esacciSnowConditionDir)

        listSnowConditionFiles = glob.glob(os.path.join(esacciSnowConditionDir,'ESACCI-LC-L4-Snow-Cond-AggOcc-500m-P13Y7D-2000-2012-2000*-v2.0.tif'))
        if listSnowConditionFiles != []:
            listSnowConditionFiles.sort()
            if (doy_acq > 363) | ((doy_acq > 362) & (not isLeap)):
                esacciSnowCondition = listSnowConditionFiles[-1]

            else:
                # special handling for Day Of Year 61
                if doy_acq == 61:
                    esacciSnowCondition = listSnowConditionFiles[8]  # corresponds to 9th weekly product of the year

                datePattern = os.path.join(esacciSnowConditionDir,'ESACCI-LC-L4-Snow-Cond-AggOcc-500m-P13Y7D-2000-2012-%Y%m%d-v2.0.tif')
                for SnowConditionFile in listSnowConditionFiles:
                    doy_file = datetime.strptime(SnowConditionFile, datePattern).timetuple().tm_yday
                    if abs(doy_acq - doy_file) < 4:
                        esacciSnowCondition = SnowConditionFile
                        break

        else:
            self.logger.warning('ESA CCI Snow Condition map not present, no snow map post-processing will be done')
            return True

        xy = self.cornerCoordinates
        xp = L2A_XmlParser(self.config, 'T2A')
        tg = xp.getTree('Geometric_Info', 'Tile_Geocoding')
        hcsName = tg.HORIZONTAL_CS_NAME.text
        zone = hcsName.split()[4]
        zone1 = int(zone[:-1])
        zone2 = zone[-1:].upper()
        lonMin, latMin, dummy = transform_utm_to_wgs84(xy[1,0], xy[1,1], zone1, zone2)
        lonMax, latMax, dummy = transform_utm_to_wgs84(xy[3,0], xy[3,1], zone1, zone2)

        tmpDir = self._tmpdir

        # step 1: check if the S2 tile crosses the International Date Line:

        if lonMin <= lonMax:
            command = 'gdalwarp '
            arguments = ''
        else:
            self.logger.warning('International Date Line is crossed, ESA CCI map reframing is performed')

            ymin = clip(latMin - 0.5, -90.0, 90.0)
            ymax = clip(latMax + 0.5, -90.0, 90.0)

            cci_snowc_dst_east = os.path.join(tmpDir, 'cci_snowc' + self.config.L2A_TILE_ID + '_{0}m_east.tif'.format(self._resolution))
            cci_snowc_dst_west = os.path.join(tmpDir, 'cci_snowc' + self.config.L2A_TILE_ID + '_{0}m_west.tif'.format(self._resolution))
            cci_snowc_dst_dateline = os.path.join(tmpDir, 'cci_snowc' + self.config.L2A_TILE_ID + '_{0}m_dateline.tif'.format(self._resolution))

            command = 'gdalwarp '
            arguments = ' -te 178.5 {0} 180 {1} '.format(ymin, ymax)
            callstr = command + arguments + esacciSnowCondition + ' ' + cci_snowc_dst_east + self._DEV0

            #self.logger.warning(callstr)

            l.acquire()
            try:
                p = subprocess.Popen(callstr, shell=self._SHELL)
                p.wait()
            except:
                self.logger.warning('Cannot perform reframing step 1, no water bodies a priori information will be used')
                return False
            finally:
                l.release()

            arguments = ' -te -180 {0} -178.5 {1} '.format(ymin, ymax)
            callstr = command + arguments + esacciSnowCondition + ' ' + cci_snowc_dst_west + self._DEV0

            l.acquire()
            try:
                p = subprocess.Popen(callstr, shell=self._SHELL)
                p.wait()
            except:
                self.logger.warning('Cannot perform reframing step 2, no water bodies a priori information will be used')
                return False
            finally:
                l.release()

            arguments = ' -t_srs EPSG:4326 --config CENTER_LONG 180 '
            callstr = command + arguments + cci_snowc_dst_west + ' ' + cci_snowc_dst_east + ' ' + cci_snowc_dst_dateline + self._DEV0

            l.acquire()
            try:
                p = subprocess.Popen(callstr, shell=self._SHELL)
                p.wait()
            except:
                self.logger.warning('Cannot perform reframing step 3, no water bodies a priori information will be used')
                return False
            finally:
                l.release()

            esacciSnowCondition = cci_snowc_dst_dateline
            command = 'gdalwarp '
            arguments = ''

        # step 2: extraction and reprojection into S2 tile geometry:
        hcsCode = tg.HORIZONTAL_CS_CODE.text
        t_srs = '-t_srs ' + hcsCode

        te = ' -te %f %f %f %f' % (xy[0,0], xy[2,1], xy[2,0], xy[0,1])
        tr = ' -tr %d %d' % (self._resolution, self._resolution)
        #t_warp = te + tr + ' -r cubicspline '
        t_warp = te + tr + ' -r near '

        cci_snowc_dst = os.path.join(tmpDir, 'cci_snowc_' + self.config.L2A_TILE_ID + '_{0}m.tif'.format(self._resolution))
        callstr = command + arguments + t_srs + t_warp + esacciSnowCondition + ' ' + cci_snowc_dst + self._DEV0
        l.acquire()
        try:
            p = subprocess.Popen(callstr, shell=self._SHELL)
            p.wait()
        except:
            self.logger.warning('Cannot read esa cci snowc, no snow condition a priori information will be used')
            return False
        finally:
            l.release()

        self.importBandRes(self.SNC, cci_snowc_dst)
        try:
            os.remove(cci_snowc_dst)
            os.remove(cci_snowc_dst_east)
            os.remove(cci_snowc_dst_west)
            os.remove(cci_snowc_dst_dateline)
        except:
            pass

        self.logger.info('ESA CCI Snow Condition map prepared')
        return cci_snowc_dst


    def gdalDEM_srtm(self):
        import urllib
        import zipfile
        isTemporary = False
        demDir = self.config.demDirectory
        if demDir == 'NONE':
            self.logger.info('DEM directory not specified, flat surface is used')
            return False
        self.logger.info('Start DEM alignment for tile')
        sourceDir = os.path.join(self.config.home, demDir)
        tmpDir = self._tmpdir
        l.acquire()
        try:
            if(os.path.exists(sourceDir) == False):
                os.makedirs(sourceDir)
        finally:
            l.release()
 
        xy = self.cornerCoordinates
        xp = L2A_XmlParser(self.config, 'T2A')
        tg = xp.getTree('Geometric_Info', 'Tile_Geocoding')
        hcsName = tg.HORIZONTAL_CS_NAME.text
        zone = hcsName.split()[4]
        zone1 = int(zone[:-1])
        zone2 = zone[-1:].upper()
        lonMin, latMin, dummy = transform_utm_to_wgs84(xy[1,0], xy[1,1], zone1, zone2)
        lonMax, latMax, dummy = transform_utm_to_wgs84(xy[3,0], xy[3,1], zone1, zone2)
        # fix for SIIMPC-611, UMW:
        lonMinId = int((-180-lonMin)/(-360)*72+1)
        lonMaxId = int((-180-lonMax)/(-360)*72+1)
        latMinId = int((60-latMax)/(120)*24+1) # this is inverted by intention
        latMaxId = int((60-latMin)/(120)*24+1) # this is inverted by intention
        # end fix SIIMPC-611

        if(lonMinId < 1) or (lonMaxId > 72) or (latMinId < 1) or (latMaxId > 24):
            self.logger.error('no SRTM dataset available for this tile, flat surface will be used')
            return False

        # temporary fix for SIIMPC-944, JL:         # end temporary fix for SIIMPC-944
        if (lonMinId <= lonMaxId):
            lons = range(lonMinId, lonMaxId+1)
        else:
            lons = [1, 72]
            self.logger.info('This tile is crossing the international date line, a particular processing is performed')
        # end temporary fix for SIIMPC-944

        for i in lons:
            for j in range(latMinId, latMaxId+1):
                tifFn = 'srtm_{:0>2d}_{:0>2d}.tif'.format(i,j)
                zipFn = 'srtm_{:0>2d}_{:0>2d}.zip'.format(i,j)

                try: # does the tiff file already exist?
                    with open (os.path.join(sourceDir, tifFn)) as fp:
                        self.logger.info('Dem exists: %s', tifFn)
                        continue

                except IOError:
                    try:
                        # zipfile needs to be downloaded ...
                        self.logger.info('read zipfile: %s', zipFn)
                        prefix = self.config._demReference
                        self.logger.stream(
                            'Trying to retrieve DEM from URL %s this may take some time ...', prefix)
                        self.logger.info('Trying to retrieve DEM from URL: %s', prefix)
                        url = prefix + zipFn
                        webFile = urllib.urlopen(url)
                        localFile = open(os.path.join(tmpDir, url.split('/')[-1]), 'wb')
                        localFile.write(webFile.read())
                        webFile.close()
                        localFile.close()
                        self.logger.info('zipfile downloaded: %s', zipFn)
                    except Exception as e:
                        self.logger.error(e)
                        self.logger.error('Download error %s, flat surface will be used')
                        return False
                    try:
                        zipf = zipfile.ZipFile(localFile.name, mode='r')
                    except Exception as e:
                        self.logger.error(e)
                        self.logger.error('DEM not available, flat surface will be used')
                        try:
                            os.remove(localFile.name)
                        except:
                            pass
                        return False
                    if (zipf.testzip() == None):
                        try:
                            zipf.extract(tifFn, sourceDir)
                            zipf.close()
                            self.logger.info('DEM unpacked and moved: %s', tifFn)
                            os.remove(localFile.name)
                            self.logger.info('zipfile removed: %s', localFile.name)
                        except Exception as e:
                            self.logger.error(e)
                            self.logger.error('Extraction error for DEM: %', localFile.name)

                        # fix for SIIMPC-577, UMW:
                        self.SIIMPC_577(os.path.join(sourceDir, tifFn))
                        # end fix for SIIMPC-577
                        continue

        # step 1: performing mosaicking, if needed:
        if lonMin <= lonMax: # Fix for SIIMPC-944 VD-JL - International Date Line handling for DEM mosaicking
            command = 'gdalwarp '
        else:
            command = 'gdalwarp -t_srs EPSG:4326 --config CENTER_LONG 180 '
        # end of fix for SIIMPC-944

        arguments = '-ot Int16 '

        if(lonMinId == lonMaxId) & (latMinId == latMaxId):
            srtmf_src = os.path.join(sourceDir,'srtm_{:0>2d}_{:0>2d}.tif'.format(i,j))
        else:
            # more than 1 DEM needs to be concatenated:
            for i in lons:
                for j in range(latMinId, latMaxId+1):
                    tifFn = os.path.join(sourceDir,'srtm_{:0>2d}_{:0>2d}.tif'.format(i,j))
                    arguments += tifFn + ' '

            srtmf_src = os.path.join(tmpDir, 'srtm_' + self.config.L2A_TILE_ID + '_src.tif')
            isTemporary = True
            callstr = command + arguments + srtmf_src + self._DEV0
            l.acquire()
            try:
                p = subprocess.Popen(callstr, shell=self._SHELL)
                p.wait()
            except Exception as e:
                self.logger.fatal(e, exc_info=True)
                self.logger.fatal('shell execution error using gdalwarp')
                os.remove(srtmf_src)
                return False
            finally:
                l.release()

        # The following fix (fix for SIIMPC-550, UMW)
        # needs to be performed on original srtm tiff data
        # i.e. moved before reprojection and resizing (see Jira SIIMPC-550 discussion)
        # done here ...
        # fix for SIIMPC-550, UMW:
        src_ds = gdal.Open(srtmf_src, GA_Update)
        if src_ds is None:
            return False
        src_band = src_ds.GetRasterBand(1)
        rows = src_ds.RasterYSize
        cols = src_ds.RasterXSize
        src_arr = src_band.ReadAsArray(0,0,cols,rows)
        NODATA_DEM = -32768

        # Fix for SIIMPC-944 VD-JL - International Date Line handling for DEM mosaicking
        if (lonMinId > lonMaxId) and (cols == 12000):
            column_west = src_arr[:, 5999].astype(float32)
            column_east = src_arr[:, 6001].astype(float32)
            column_interp = src_arr[:, 6000]
            interp_valid = (column_west != NODATA_DEM) & (column_east != NODATA_DEM)
            column_interp[interp_valid] = ((column_west[interp_valid] + column_east[interp_valid])/2.).astype(int16)
            src_arr[:, 6000] = column_interp
        # end of fix for SIIMPC-944 VD-JL - International Date Line handling for DEM mosaicking

        src_arr[(src_arr == NODATA_DEM)] = 0
        src_band.WriteArray(src_arr, 0, 0)
        src_band.FlushCache()
        src_ds = None
        # end fix for SIIMPC-550

        # step 3: performing the resizing:
        hcsCode = tg.HORIZONTAL_CS_CODE.text
        t_srs = '-t_srs ' + hcsCode
 
        te = ' -te %f %f %f %f' % (xy[0,0], xy[2,1], xy[2,0], xy[0,1])
        tr = ' -tr %d %d' % (self._resolution, self._resolution)
        t_warp = te + tr + ' -r cubicspline '
        # fix for SIIMPC-792, JL:
        arguments = '-ot Float32 '
        # end of fix for SIIMPC-792

        srtmf_dst = os.path.join(tmpDir, 'srtm_' + self.config.L2A_TILE_ID + '_dem.tif')
        callstr = command + arguments + t_srs + t_warp + srtmf_src + ' ' + srtmf_dst + self._DEV0
        l.acquire()
        try:
            p = subprocess.Popen(callstr, shell=self._SHELL)
            p.wait()
        except:
            self.logger.fatal('Error reading DEM, flat surface will be used')
            os.remove(srtmf_src)
            return False
        finally:
            l.release()

        # fix for SIIMPC-792, JL:
        command = 'gdal_translate '
        arguments = '-ot Int16 '
        srtmf_dst_int16 = os.path.join(tmpDir, 'srtm_' + self.config.L2A_TILE_ID + '_dem_int16.tif')
        callstr = command + arguments + srtmf_dst + ' ' + srtmf_dst_int16 + self._DEV0
        l.acquire()
        try:
            p = subprocess.Popen(callstr, shell=self._SHELL)
            p.wait()
        except Exception as e:
            self.logger.fatal(e, exc_info=True)
            self.logger.fatal('Error reading DEM, flat surface will be used')
            os.remove(srtmf_src)
            return False
        finally:
            l.release()
        # end of fix for SIIMPC-792

        self.importBandRes(self.DEM, srtmf_dst_int16)  # fix for SIIMPC-792, JL, srtmf_dst_int16
        if isTemporary:
            os.remove(srtmf_src)
        # fix for SIIMPC-792, JL
        os.remove(srtmf_dst_int16)  # check if needed (JL)
        # end of fix for SIIMPC-792

        self.logger.info('DEM received and prepared')
        return srtmf_dst


    def SIIMPC_577(self, filename):
        # fix for SIIMPC-577, UMW:
        dataset = gdal.Open(filename, gdal.GA_Update)
        if dataset is None:
            return False

        # display current
        self.logger.info('Driver: %s / %s' % (dataset.GetDriver().ShortName, dataset.GetDriver().LongName))
        self.logger.info('Size is: %d x %d x %d' % (dataset.RasterXSize, dataset.RasterYSize, dataset.RasterCount))
        self.logger.info('Projection is: %s' % dataset.GetProjection())
        geotransform = dataset.GetGeoTransform()
        self.logger.info('Origin = (%f, %f)' % (geotransform[0], geotransform[3]))
        self.logger.info('Pixel Size = (%f, %f)' % (geotransform[1], geotransform[5]))
        dataset.SetGeoTransform(
            [geotransform[0] - geotransform[1] / 2, geotransform[1], geotransform[2],
             geotransform[3] + geotransform[5] / 2, geotransform[4], geotransform[5]])

        geotransform = dataset.GetGeoTransform()
        self.logger.info('Origin = (%f, %f)' % (geotransform[0], geotransform[3]))
        self.logger.info('Pixel Size = (%f, %f)' % (geotransform[1], geotransform[5]))
        dataset = None

        return True

    def fileExists(self, filename):
        counter = 0
        while True:
            try:
                with open(filename) as fp:
                    return True # Success
            except Exception as e:
                self.logger.warning(e, exc_info=True)
                if counter > self.config.timeout:
                    self.logger.fatal('File %s cannot be read, Timeout received.' % filename)
                    return False

                sleep(1)
                counter += 1
                continue


    def gdalDEM_Shade(self, demfile):
        sdwfile = demfile.replace('_dem', '_sdw')
        
        altitude = 90.0 - float32(mean(self.config.solze_arr))
        azimuth = float32(mean(self.config.solaz_arr))
        command = 'gdaldem hillshade '
        options = '-compute_edges -az ' + str(azimuth) + ' -alt ' + str(altitude)
        callstr = command + options + ' ' + demfile + ' ' + sdwfile + self._DEV0
        l.acquire()
        try:
            p =subprocess.Popen(callstr, shell=self._SHELL)
            p.wait()
        except Exception as e:
            self.logger.fatal(e, exc_info=True)
            self.logger.fatal('shell execution error using gdaldem shade')
            return False
        finally:
            l.release()

        self.importBandRes(self.SDW, sdwfile)
        os.remove(sdwfile)
        return True


    def gdalDEM_Slope(self, demfile):
        slpfile = demfile.replace('_dem', '_slp')
                
        command = 'gdaldem slope '
        options = '-compute_edges'
        callstr = command + options + ' ' + demfile + ' ' + slpfile + self._DEV0

        l.acquire()
        try:
            p =subprocess.Popen(callstr, shell=self._SHELL)
            p.wait()
        except Exception as e:
            self.logger.fatal(e, exc_info=True)
            self.logger.fatal('shell execution error using gdaldem slope')
            return False
        finally:
            l.release()

        self.importBandRes(self.SLP, slpfile)
        os.remove(slpfile)
        return True


    def gdalDEM_Aspect(self, demfile):
        aspfile = demfile.replace('_dem', '_asp')
        command = 'gdaldem aspect '
        options = '-compute_edges'
        callstr = command + options + ' ' + demfile + ' ' + aspfile + self._DEV0

        l.acquire()
        try:
            p =subprocess.Popen(callstr, shell=self._SHELL)
            p.wait()
        except Exception as e:
            self.logger.fatal(e, exc_info=True)
            self.logger.fatal('shell execution error using gdaldem aspect')
            return False
        finally:
            l.release()

        self.importBandRes(self.ASP, aspfile)
        os.remove(aspfile)
        return True

    def importBandImg(self, index, filename):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        if self.hasBand(index):
            # avoid reread of already existing reflectance bands:
            self.logger.info('L2A_Tables: band ' + bandName + ' already present')
            return True
        if fnmatch.fnmatch(filename,'*.tif'):
            # the new input for JP2 data (or TIFF in raw mode):
            ds = gdal.Open(filename, GA_ReadOnly)
            indataset = ds.GetRasterBand(1).ReadAsArray()
        else:
            warnings.filterwarnings("ignore")
            if self.config.nrThreads == 'AUTO':
                nrThreads = cpu_count()
            else:
                nrThreads = int(self.config.nrThreads)
            try: # to be compatible with OpenJPEG < 2.3:
                glymur.set_option('lib.num_threads', nrThreads)
            except:
                pass
            indataset = glymur.Jp2k(filename)
        if self.config.TESTMODE:
            if (indataset.shape[0] == 183) or (indataset.shape[0] == 1830):
                rowcol = 183
            elif (indataset.shape[0] == 549) or (indataset.shape[0] == 5490):
                rowcol = 549
            elif (indataset.shape[0] == 1098) or (indataset.shape[0] ==  10980):
                rowcol = 1098
            src_nrows = rowcol
            src_ncols = rowcol
            if self._resolution == 60:
                self.config.nrows = 183
                self.config.ncols = 183
            elif self._resolution == 20:
                self.config.nrows = 549
                self.config.ncols = 549
            elif self._resolution == 10:
                self.config.nrows = 1098
                self.config.ncols = 1098
            indataArr = indataset[0:src_nrows,0:src_ncols]
        else:
            src_nrows = indataset.shape[0]
            src_ncols = indataset.shape[1]
            indataArr = indataset[:]

        if (indataArr.max() == 0):
            self.logger.warning('Band ' + bandName + ' does not contain any data')

        # update the geobox according to resolutions:
        if (index == 0):
            self.config.set_geobox(indataset.box[3], 60)
        elif (index == 1):
            self.config.set_geobox(indataset.box[3], 10)
        elif (index == 5):
            self.config.set_geobox(indataset.box[3], 20)

        try:
            h5file = open_file(self._imgdb, mode='a', title =  'input bands')
            arrays = h5file.root.arrays
            filters = Filters(complib='zlib', complevel=self.config.db_compression_level)
            dtOut = self.setDataType(indataArr.dtype)
            eArray = h5file.create_earray(arrays, bandName, dtOut, (0, src_ncols), bandName, filters=filters)
            eArray.append(indataArr)
            table = h5file.root.metadata.META
            particle = table.row
            particle['bandName'] = bandName
            particle['rasterYSize'] = src_nrows
            particle['rasterXSize'] = src_ncols
            particle['rasterCount'] = 1
            particle.append()
            table.flush()
            self.config.timestamp('L2A_Tables: band ' + bandName + ' imported')
            return True
        except Exception as e:
            self.logger.fatal(e, exc_info=True)
            return False
        finally:
            if h5file:
                h5file.close()

    def importBandRes(self, index, filename):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        if ((index in[14,17,18,19]) and (self._resolution == 10)) and self.hasBand(index):
            # resample SCL, AOT, WVP, VIS and DEM related bands:
            self.config.timestamp('L2A_Tables: band ' + bandName + ' needs to be resampled')
            indataset = self.resampleBand(index, self.getBand(index))
        elif fnmatch.fnmatch(filename,'*.tif'):
            # the new input for JP2 data (or TIFF in raw mode):
            ds = gdal.Open(filename, GA_ReadOnly)
            indataset = ds.GetRasterBand(1).ReadAsArray()
        if self.config.TESTMODE:
            if (indataset.shape[0] == 183) or (indataset.shape[0] == 1830):
                rowcol = 183
            elif (indataset.shape[0] == 549) or (indataset.shape[0] == 5490):
                rowcol = 549
            elif (indataset.shape[0] == 1098) or (indataset.shape[0] ==  10980):
                rowcol = 1098
            src_nrows = rowcol
            src_ncols = rowcol
            if self._resolution == 60:
                self.config.nrows = 183
                self.config.ncols = 183
            elif self._resolution == 20:
                self.config.nrows = 549
                self.config.ncols = 549
            elif self._resolution == 10:
                self.config.nrows = 1098
                self.config.ncols = 1098
        else:
            src_nrows = indataset.shape[0]
            src_ncols = indataset.shape[1]

        indataArr = indataset[0:src_nrows,0:src_ncols]
        if (indataArr.max() == 0):
            self.logger.warning('Band ' + bandName + ' does not contain any data')

        # update the geobox according to resolutions:
        if (index == 0):
            self.config.set_geobox(indataset.box[3], 60)
        elif (index == 1):
            self.config.set_geobox(indataset.box[3], 10)
        elif (index == 5):
            self.config.set_geobox(indataset.box[3], 20)
        try:
            if ((index in [14, 17, 18, 19]) and (self._resolution == 10)) and self.hasBand(index):
                h5file = open_file(self._resdb + '_tmp', mode='a', title = 'resampled bands')
            else:
                h5file = open_file(self._resdb, mode='a', title = 'resampled bands')
            arrays = h5file.root.arrays
            filters = Filters(complib='zlib', complevel=self.config.db_compression_level)
            dtOut = self.setDataType(indataArr.dtype)
            eArray = h5file.create_earray(arrays, bandName, dtOut, (0, src_ncols), bandName, filters=filters)
            eArray.append(indataArr)
            table = h5file.root.metadata.META
            particle = table.row
            particle['bandName'] = bandName
            particle['rasterYSize'] = src_nrows
            particle['rasterXSize'] = src_ncols
            particle['rasterCount'] = 1
            particle.append()
            table.flush()
            self.config.timestamp('L2A_Tables: band ' + bandName + ' imported')
            return True
        except Exception as e:
            self.logger.fatal(e, exc_info=True)
            return False
        finally:
            if h5file:
                h5file.close()

    def initTable(self, filename):
        try:
            h5file = open_file(filename, mode='w', title =  str(self._resolution) + 'm bands')
            group = h5file.create_group('/', 'metadata', 'metadata information')
            h5file.create_table(group, 'META', Particle, "Meta Data")
            h5file.create_group('/', 'arrays', 'band arrays')
            h5file.create_group('/', 'tmp', 'temporary arrays')
            return True
        except Exception as e:
            self.logger.fatal(e, exc_info=True)
            return False
        finally:
            if h5file:
                h5file.close()

    def resampleBand(self, index, indataArr):
        src_nrows = indataArr.shape[0]
        tgt_nrows = self.config.nrows
        if (src_nrows / tgt_nrows) == 2:
            # mean per 2x2 block of pixels of 10m band for 20m res
            return uint16(block_reduce(indataArr, block_size=(2, 2), func=mean) + 0.5)
        elif (src_nrows / tgt_nrows) == 3:
            # mean per 3x3 block of pixels of 20m band for 60m res
            return uint16(block_reduce(indataArr, block_size=(3, 3), func=mean) + 0.5)
        elif (src_nrows / tgt_nrows) == 6:
            # mean per 6x6 block of pixels of 10m band for 60m res
            return uint16(block_reduce(indataArr, block_size=(6, 6), func=mean) + 0.5)
        elif tgt_nrows > src_nrows:
            # upsampling is required:
            sizeUp = tgt_nrows
            if index in [14, 17, 18, 19]:
                # order=0 is for nearest neighbor (SCL, AOT, WVP, VIS):
                return (skit_resize(indataArr.astype(uint16), ([sizeUp, sizeUp]), order=0) * 65535.).round().astype(uint16)
            elif (index == 10) | (index == 9):
                # order=1 is for bi-linear interpolation (B10 upsampling from 60 m to 20 m):
                return (skit_resize(indataArr.astype(uint16), ([sizeUp, sizeUp]), order=1) * 65535.).round().astype(uint16)
            else:
                # order=3 is for bi-cubic spline (other bands):
                return (skit_resize(indataArr.astype(uint16), ([sizeUp, sizeUp]), order=3) * 65535.).round().astype(uint16)
        return

    def downsampleBandList_20to60_andExport(self):
        self.config.timestamp('L2A_Tables: preparing downsampled export for 60 m resolution')
        if self._resolution != 20:
            return False

        self._firstInit = False
        R60m = os.path.join(self._L2A_ImgDataDir, 'R60m')
        if not os.path.exists(R60m):
            os.mkdir(R60m)

        self.config.timestamp('L2A_Tables: start export for 60 m resolution')
        if self.config.productVersion < 14:
            bandIndex = [0, 1, 2, 3, 4, 5, 6, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 27]
        else:
            bandIndex = [0, 1, 2, 3, 4, 5, 6, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 27, 33]
        RGB = [3,2,1]

        # Metadata update, create header:
        tileId = self.config.L2A_TILE_ID
        consolidatedTile = False
        if ('L2A_CO_' in tileId):
            consolidatedTile = True
            self.logger.info('consolidated tile ' + tileId + ': no entry in user product metadata generated')
        else:
            if not consolidatedTile and self.config.operationMode == 'TOOLBOX':
                xp = L2A_XmlParser(self.config, 'UP2A')
                pi = xp.getTree('General_Info', 'Product_Info')
                Granule = pi.Product_Organisation.Granule_List.Granule

        for i in bandIndex:
            if self.config.scOnly or (i > 12):
                indataset = self.getBand(i)
                if i < 13:
                    indataset = indataset * self.config.dnScale
            else:
                indataset = self.getTmpBand(i)
            try:
                bandName = self.getBandNameFromIndex(i)
                filename = self._L2A_Tile_BND_File

                if bandName == 'SNW':
                    filename = self._L2A_Tile_SNW_File
                elif bandName == 'CLD':
                    filename = self._L2A_Tile_CLD_File
                elif bandName == 'AOT':
                    filename = self._L2A_Tile_AOT_File
                elif bandName == 'WVP':
                    filename = self._L2A_Tile_WVP_File
                elif bandName == 'TCI':
                    if (self.config.productVersion < 14) or (self.config.tciOutput == False):
                        continue
                    filename = self._L2A_Tile_TCI_File
                elif bandName == 'DEM':
                    if (self.config.demDirectory == 'NONE') or (self.config.demOutput == False):
                        continue
                    filename = self._L2A_Tile_DEM_File
                    indataset = (indataset + 10000).astype(uint16)
                    # SIIMPC-1427, to be activated in a later version:
                    # mask = self.tables.getBand(self.tables.DEM)
                    # band[mask == self.config.noData] = self.config.noData
                    # del mask
                elif bandName == 'DDV':
                    if (self.config.ddvOutput == False):
                        continue
                    filename = self._L2A_Tile_DDV_File
                # special treatment for scene class:
                if bandName == 'SCL':
                    filename = self._L2A_Tile_SCL_File
                    if self.config.TESTMODE:
                        rowcol = 183
                    else:
                        rowcol = 1830
                    band = median_filter(indataset, 3)
                    band = (skit_resize(band.astype(uint8),
                        ([rowcol, rowcol]), order=0) * 255.).round().astype(uint8)
                elif bandName != 'TCI':  # any other band except SCL or TCI:
                    band = uint16(block_reduce(indataset, block_size=(3, 3), func=mean) + 0.5)

                # set to 60m resolution for creation of RGB image:
                self._resolution = 60
                self.config.resolution = 60
                filename = filename.replace('BXX', bandName)
                filename = filename.replace('R20', 'R60')
                filename = filename.replace('20m', '60m')

                if bandName != 'TCI':
                    if self.config.raw or self.config.tif:
                        self.exportRawImage(filename, band)
                    else:
                        self.glymurWrapper(filename, band)
                    self.config.timestamp('L2A_Tables: band ' + bandName + ' exported')
                # for creation of RGB images:
                if i in RGB:
                    self.setTmpBand(i,band)

                #fix GNR: removed extension using split
                #filename = os.path.basename(filename.strip(self._L2A_ImageExtention))
                filename = '.'.join(os.path.basename(filename).split('.')[:-1])
                if (bandName != 'VIS' and not consolidatedTile and self.config.operationMode == 'TOOLBOX'):
                    ifn = 'IMAGE_FILE'
                    imageFile2a = etree.Element(ifn)
                    # by intention os.path.join is not used here, as otherwise validation on windows fails:
                    if (bandName == 'CLD' or bandName == 'SNW' or bandName == 'DDV'):
                        continue
                    elif bandName == 'DEM':
                        imageFile2a.text = 'GRANULE/' + self.config.L2A_TILE_ID + '/AUX_DATA/' + filename
                    else:
                        resolution = 'R' + str(self._resolution) + 'm/'
                        imageFile2a.text = 'GRANULE/' + self.config.L2A_TILE_ID + '/IMG_DATA/' + resolution + filename
                    Granule.append(imageFile2a)

            except Exception as e:
                self.logger.fatal(e, exc_info=True)
                return False

        if self.config.operationMode == 'TOOLBOX':
            xp.export()
            # update on UP level:
            self.updateBandInfo()

        # update on tile level:
        xp = L2A_XmlParser(self.config, 'T2A')
        plqi = xp.getTree('Quality_Indicators_Info', 'Pixel_Level_QI')
        if self.config.operationMode == 'TOOLBOX':
            msk = etree.Element('MASK_FILENAME')
            msk.attrib['type'] = 'MSK_CLDPRB'
            msk.text = 'GRANULE/' + self.config.L2A_TILE_ID + '/QI_DATA/' + os.path.basename(
                self._L2A_Tile_CLD_File.replace('20m', '60m'))
            plqi.append(msk)
            msk = etree.Element('MASK_FILENAME')
            msk.attrib['type'] = 'MSK_SNWPRB'
            msk.text = 'GRANULE/' + self.config.L2A_TILE_ID + '/QI_DATA/' + os.path.basename(
                self._L2A_Tile_SNW_File.replace('20m', '60m'))
            plqi.append(msk)
        else:
            msk = etree.Element('MASK_FILENAME')
            msk.attrib['type'] = 'MSK_CLDPRB'
            msk.text = os.path.basename(self._L2A_Tile_CLD_File.replace('20m', '60m'))
            plqi.append(msk)
            msk = etree.Element('MASK_FILENAME')
            msk.attrib['type'] = 'MSK_SNWPRB'
            msk.text = os.path.basename(self._L2A_Tile_SNW_File.replace('20m', '60m'))
            plqi.append(msk)
        xp.export()

        result = True
        if not self.createRgbImages():
            result = False
        # reset to source resolution:
        self._resolution = 20
        self.config.resolution = 20
        return result

    def exportBandList(self):
        h5file = None
        sourceDir = self._L2A_bandDir
        if(os.path.exists(sourceDir) == False):
            self.logger.fatal('missing directory %s:' % sourceDir)
            return False

        self.config.timestamp('L2A_Tables: start export for %s m resolution' % self._resolution)
        if(self._resolution == 10):
            if self.config.productVersion < 14:
                bandIndex = [1, 2, 3, 7, 13, 17, 18]
            else:
                bandIndex = [1, 2, 3, 7, 13, 17, 18, 33]
        elif(self._resolution == 20):
            if self.config.productVersion < 14:
                bandIndex = [1, 2, 3, 4, 5, 6, 8, 11, 12, 13, 14, 15, 16, 17, 18, 19, 27]
            else:
                bandIndex = [1, 2, 3, 4, 5, 6, 8, 11, 12, 13, 14, 15, 16, 17, 18, 19, 27, 33]
        elif(self._resolution == 60):
            if self.config.productVersion < 14:
                bandIndex = [0, 1, 2, 3, 4, 5, 6, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 27]
            else:
                bandIndex = [0, 1, 2, 3, 4, 5, 6, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 27, 33]

        if self.config.scOnly:
            for index in bandIndex:
                if index < 13:
                    indataset = uint16(self.getBand(index) * self.config.dnScale)
                    self.setTmpBand(index, indataset)
                elif index in [17, 18, 19, 27]:
                    indataset = zeros([self.config.nrows, self.config.ncols], dtype=uint16)
                    self.setBand(index, indataset)

        #prepare the xml export
        tileId = self.config.L2A_TILE_ID
        consolidatedTile = False
        if ('L2A_CO_' in tileId):
            consolidatedTile = True
            self.logger.info('consolidated tile ' + tileId + ': no entry in user product metadata generated')
        else:
            xp = L2A_XmlParser(self.config, 'T2A')
            gi2a = xp.getTree('General_Info', 'TILE_ID')
            ds2a = xp.getTree('General_Info', 'DATASTRIP_ID')
            # PDGS or TOOLBOX, new format:
            gi2a = gi2a.text
            ds2a = ds2a.text
            pbStr = '_N%05.2f' % self._config.processingBaseline
            gi2a = re.sub(r"_N\d\d.\d\d", pbStr, gi2a)
            ds2a = re.sub(r"_N\d\d.\d\d", pbStr, ds2a)
            if not consolidatedTile and self.config.operationMode == 'TOOLBOX':
                xp = L2A_XmlParser(self.config, 'UP2A')
                pi = xp.getTree('General_Info', 'Product_Info')
                gl = pi.Product_Organisation.Granule_List
                try:
                    Granule = gl.Granule
                except:
                    Granule = objectify.Element('Granule')
                    Granule.attrib['datastripIdentifier'] = ds2a
                    Granule.attrib['granuleIdentifier'] = gi2a
                    Granule.attrib['imageFormat'] = 'JPEG2000'
                    gl.append(Granule)
                try:
                    # remove entries for the moment as long as no quality control is implemented for Toolbox mode:
                    qcc = xp.getTree('Quality_Indicators_Info', 'Quality_Control_Checks')
                    del qcc.Failed_Inspections.Datastrip_Report[:]
                    #qcc.Failed_Inspections.Datastrip_Report.attrib['datastripId'] = ds2a
                except:
                    pass
        try:
            h5file = open_file(self._resdb, mode='r')
            for index in bandIndex:
                bandName = self.getBandNameFromIndex(index)
                filename = self._L2A_Tile_BND_File
                filename = filename.replace('BXX', bandName)
                if bandName == 'VIS':
                    filename = self._L2A_Tile_VIS_File
                elif bandName == 'SNW':
                    filename = self._L2A_Tile_SNW_File
                elif bandName == 'CLD':
                    filename = self._L2A_Tile_CLD_File
                elif bandName == 'SCL':
                    filename = self._L2A_Tile_SCL_File
                elif bandName == 'AOT':
                    filename = self._L2A_Tile_AOT_File
                elif bandName == 'WVP':
                    filename = self._L2A_Tile_WVP_File
                elif bandName == 'TCI':
                    if(self.config.productVersion < 14) or (self.config.tciOutput == False):
                        continue
                    filename = self._L2A_Tile_TCI_File
                elif bandName == 'DEM':
                    if(self.config.demDirectory == 'NONE') or (self.config.demOutput == False):
                        continue
                    filename = self._L2A_Tile_DEM_File
                elif bandName == 'DDV':
                    if(self.config.ddvOutput == False):
                        continue
                    filename = self._L2A_Tile_DDV_File
                if index < 13:
                    try:
                        node = h5file.get_node('/tmp', bandName)
                    except:
                        try:
                            node = h5file.get_node('/arrays', bandName)
                        except Exception as e:
                            self.logger.fatal(e, exc_info=True)
                            return False
                elif self._resolution == 10 and (bandName == 'AOT' or bandName == 'WVP'):
                    node = h5file.get_node('/arrays', bandName)
                elif bandName != 'TCI':
                    node = h5file.get_node('/arrays', bandName)
                if (self._resolution == 60):
                    filename = filename.replace('R20', 'R60')
                    filename = filename.replace('20m', '60m')
                if bandName != 'TCI':
                    band = node.read()
                    if self.config.logLevel == 'DEBUG':
                        self.readoutStatistics(bandName)
                    # fix for SIIMPC-551, to avoid negative values where OpenJPEG cannot cope with, UMW,
                    # att offset of 10.000 and convert to uint16
                    if bandName == 'DEM':
                        band = (band + 10000).astype(uint16)
                        # SIIMPC-1427, to be activated in a later version:
                        # mask = self.tables.getBand(self.tables.DEM)
                        # band[mask == self.config.noData] = self.config.noData
                        # del mask

                    elif bandName == 'WVP':
                        band = band.astype(uint16)
                    # end fix for SIIMPC-551
                    if self.config.raw or self.config.tif:
                        self.exportRawImage(filename, band)
                    else:
                        self.glymurWrapper(filename, band)
                        
                    self.config.timestamp('L2A_Tables: band ' + bandName + ' exported')

                if (bandName != 'VIS' and not consolidatedTile and self.config.operationMode == 'TOOLBOX'):
                    filename = '.'.join(os.path.basename(filename).split('.')[:-1])
                    ifn = 'IMAGE_FILE'
                    imageFile2a = etree.Element(ifn)
                    # by intention os.path.join is not used here, as otherwise validation on windows fails:
                    if (bandName == 'CLD' or bandName == 'SNW' or bandName == 'DDV'):
                        continue
                    elif bandName == 'DEM':
                        imageFile2a.text = 'GRANULE/' + self.config.L2A_TILE_ID + '/AUX_DATA/' + filename
                    else:
                        resolution = 'R' + str(self._resolution) + 'm/'
                        imageFile2a.text = 'GRANULE/' + self.config.L2A_TILE_ID + '/IMG_DATA/' + resolution + filename
                    Granule.append(imageFile2a)

        except Exception as e:
            self.logger.fatal(e, exc_info=True)
            return False
        finally:
            if h5file:
                h5file.close()

        if self.config.operationMode == 'TOOLBOX':
            # update on UP level:
            self.updateBandInfo()
            xp.export()
        # update on tile level:
        if(self._resolution > 10):
            xp = L2A_XmlParser(self.config, 'T2A')
            # Fix for SIIMPC-1227 - UMW: change filenames for masks in Tile Metadata to short:
            qii = xp.getRoot('Quality_Indicators_Info')
            for element in qii.getiterator('MASK_FILENAME'):
                dirname, basename = os.path.split(element.text)
                fnIn = basename.replace('__','_')
                fnInS = fnIn.split('_')
                if len(fnInS) > 3:
                    fnOut = fnInS[2] + '_' + fnInS[3] + '_' + fnInS[8] + '.gml'
                # Fix for SIIMPC-1419 - UMW: no directory extension for PDI mode:
                else:
                    fnOut = basename
                if self.config.operationMode == 'TOOLBOX':
                    element._setText(os.path.join(dirname,fnOut))
                else:
                    element._setText(fnOut)
                # end fix: SIIMPC-1419
            # end fix: SIIMPC-1227
            plqi = xp.getTree('Quality_Indicators_Info', 'Pixel_Level_QI')
            if self.config.operationMode == 'TOOLBOX':
                msk = etree.Element('MASK_FILENAME')
                msk.attrib['type'] = 'MSK_CLDPRB'
                msk.text = 'GRANULE/' + self.config.L2A_TILE_ID + '/QI_DATA/' + os.path.basename(self._L2A_Tile_CLD_File)
                plqi.append(msk)
                msk = etree.Element('MASK_FILENAME')
                msk.attrib['type'] = 'MSK_SNWPRB'
                msk.text = 'GRANULE/' + self.config.L2A_TILE_ID + '/QI_DATA/' + os.path.basename(self._L2A_Tile_SNW_File)
                plqi.append(msk)
            else:
                msk = etree.Element('MASK_FILENAME')
                msk.attrib['type'] = 'MSK_CLDPRB'
                msk.text = os.path.basename(self._L2A_Tile_CLD_File)
                plqi.append(msk)
                msk = etree.Element('MASK_FILENAME')
                msk.attrib['type'] = 'MSK_SNWPRB'
                msk.text = os.path.basename(self._L2A_Tile_SNW_File)
                plqi.append(msk)
            if self._firstInit:
                pvi = xp.getTree('Quality_Indicators_Info', 'PVI_FILENAME')
                if self.config.operationMode == 'TOOLBOX':
                    pvi._setText('GRANULE/' + self.config.L2A_TILE_ID + '/QI_DATA/' + os.path.basename(self._L2A_Tile_PVI_File))
                else:
                    pviFn = os.path.basename(self._L2A_Tile_PVI_File).split(self._L2A_ImageExtention)[0]
                    pvi._setText(pviFn)
            xp.export()

        result = True
        if not self.createRgbImages():
            result = False
        # cleanup:
        if (self._resolution == 10) and (os.path.isfile(self._imgdb)):
            self.logger.info("removing hd5 result database (size: %s)" % os.path.getsize(self._resdb))
            os.remove(self._resdb)
            self.logger.info("removing hd5 image database (size: %s)" % os.path.getsize(self._imgdb))
            os.remove(self._imgdb)

        self.config.timestamp('L2A_Tables: stop export')
        return result

    def updateBandInfo(self):
        l.acquire()
        # SIITBX-64: remove unsupported bands 8 and 10:
        try:
            xp = L2A_XmlParser(self.config, 'UP2A')
            pi = xp.getTree('General_Info', 'Product_Info')
            bn = pi.Query_Options.Band_List.BAND_NAME
            # fix for SIIMPC-794: include the 20 m processing, was only active for 60 m up to now
            if self._resolution > 10:
                for i in range(len(bn)):
                    if bn[i].text == 'B8':
                        if not self.checkB2isPresent(10):
                            del bn[i]
                        continue
                    if bn[i].text == 'B10':
                        del bn[i]
                        break
            if self._resolution == 60:
                found = False
                for i in range(len(bn)):
                    if bn[i].text == 'B1':
                        found = True
                        break
                if not found:
                    b1 = etree.Element('BAND_NAME')
                    b1.text = 'B1'
                    bl = pi.Query_Options.Band_List
                    bl.insert(0, b1)
            if self._resolution == 20:
                for i in range(len(bn)):
                    if bn[i].text == 'B1':
                        if not self.checkB2isPresent(60):
                            del bn[i]
                        break
            if self._resolution == 10:
                # SIITBX-64: add info for Band 8, if not already present:
                found = False
                for i in range(len(bn)):
                    if bn[i].text == 'B8':
                        found = True
                        break
                if not found:
                    b8 = etree.Element('BAND_NAME')
                    b8.text = 'B8'
                    bl = pi.Query_Options.Band_List
                    if not self.checkB2isPresent(60):
                        # column is 6 if no 60 m processing was performed:
                        bl.insert(6, b8)
                    else:
                        bl.insert(7, b8)
        except:
            self.logger.info('Unsupported band entries already removed or not found')
        finally:
            l.release()

        # SIIMPC-1390: next lines removed for 2.7.2 to be consistent with DHUS
        return

    def glymurWrapper(self, filename, band):
        # fix for SIIMPC-687, UMW
        # fix for SIIMPC-934, UMW
        if self._resolution == 60:
            kwargs = {"cbsize": (4, 4), "tilesize": (192, 192), "prog": "LRCP", "psizes": ((64, 64), (64, 64), (64, 64), (64, 64), (64, 64), (64, 64))}
        elif self._resolution == 20:
            kwargs = {"cbsize": (8, 8), "tilesize": (640, 640), "prog": "LRCP", "psizes": ((128, 128), (128, 128), (128, 128), (128, 128), (128, 128), (128, 128))}
        elif self._resolution == 10:
            kwargs = {"cbsize": (64, 64), "tilesize": (1024, 1024), "prog": "LRCP", "psizes": ((256, 256), (256, 256), (256, 256), (256, 256), (256, 256), (256, 256))}
        # end fix for SIIMPC-934
        # end fix for SIIMPC-687
        # fix for SIIMPC-558.3, UMW
        glymur.Jp2k(filename, band, **kwargs)
        jp2_L2A = glymur.Jp2k(filename)
        boxes_L2A = jp2_L2A.box
        if 'PVI' in filename:
            # fix wrong resolution in preview image:
            boxes_L2A.insert(3, self.config.geoboxPvi)
        else:
            boxes_L2A.insert(3, self.config.get_geobox())
        boxes_L2A[1] = glymur.jp2box.FileTypeBox(brand='jpx ', compatibility_list=['jpxb', 'jp2 '])
        file_L2A_geo = os.path.splitext(filename)[0] + '_geo.jp2'
        jp2_L2A.wrap(file_L2A_geo, boxes=boxes_L2A)
        os.remove(filename)
        os.rename(file_L2A_geo, filename)
        # end fix for SIIMPC-558.3
        return

    def exportRawImage(self, filename, band):

        Fn = os.path.splitext(filename)[0]
        GMLFn = Fn + '.gml'
        ImgFn = Fn + self._L2A_ImageExtention
        
        isPVI = 'PVI' in os.path.basename(filename)
        isTCI = 'TCI' in os.path.basename(filename)
        isVIS = 'VIS' in os.path.basename(filename)
        isSCL = 'SCL' in os.path.basename(filename)
        isCLD = 'CLD' in os.path.basename(filename)
        isSNW = 'SNW' in os.path.basename(filename)
        isDDV = 'DDV' in os.path.basename(filename)

        if not isVIS:
            self.generateGmlHeader(GMLFn, pvi = isPVI)   

        driver = gdal.GetDriverByName('ENVI')
        if isPVI or isTCI:
            (h, w, numBands) = band.shape
            pixelFormat = gdal.GDT_Byte
        elif isSCL or isSNW or isCLD or isDDV:
            (h,w) = band.shape
            numBands = 1
            pixelFormat = gdal.GDT_Byte
        else:
            (h,w) = band.shape
            numBands = 1
            pixelFormat = gdal.GDT_UInt16

        ds = driver.Create(ImgFn, w, h, numBands, pixelFormat)
        if isTCI or isPVI:
            for b in range(1, numBands+1):
                outBand = ds.GetRasterBand(b)
                outBand.WriteArray(band[:,:,b-1])
        else:
            outBand = ds.GetRasterBand(1)
            outBand.WriteArray(band)

        outBand.FlushCache()
        
        return
    
    def generateGmlHeader(self,GMLFn, pvi = False):

        if pvi:
            geobox = self.config.geoboxPvi
        else:
            geobox = self.config.get_geobox()

        #geobox.write(fptr)
        for box in geobox.box:
            if box.longname == 'Association':
                for subBox in box.box:
                    if subBox.longname == 'XML':
                        xml = subBox.xml
        if xml:                
            fptr = open(GMLFn, 'w')
            xml.write(fptr, encoding='utf-8', xml_declaration=True, standalone=False)
        else:
            self._logger.error('No GML header available in JPEG-2000 L1C Tile image')
    
    def createRgbImages(self):
        # create PVI:
        if self._firstInit:
            pvi = self._L2A_Tile_PVI_File
            try:
                r = self.scalePreview(self.getTmpBand(self.B04))
                g = self.scalePreview(self.getTmpBand(self.B03))
                b = self.scalePreview(self.getTmpBand(self.B02))
            except:
                r = self.scalePreview(int16(self.getBand(self.B04) * self.config.dnScale))
                g = self.scalePreview(int16(self.getBand(self.B03) * self.config.dnScale))
                b = self.scalePreview(int16(self.getBand(self.B02) * self.config.dnScale))
            try:
                # fix for SIIMPC-558.3, UMW
                if self.config.raw or self.config.tif:
                    self.exportRawImage(pvi, dstack((r, g, b)))
                else:
                    self.glymurWrapper(pvi, dstack((r, g, b)))
                # end fix for SIIMPC-558.3
                self.config.timestamp('L2A_Tables: band PVI exported')
            except Exception as e:
                self.logger.fatal(e, exc_info=True)
                self.logger.fatal('PVI image export failed')
                self.config.timestamp('L2A_Tables: PVI image export failed')
                return False

        # create TCI:
        if (self.config.productVersion >= 14) and (self.config.tciOutput == True):
            tci = self._L2A_Tile_TCI_File
            if (self._resolution == 60):
                tci = tci.replace('R20', 'R60')
                tci = tci.replace('20m', '60m')
            try:
                r = self.scaleTci(self.getTmpBand(self.B04))
                g = self.scaleTci(self.getTmpBand(self.B03))
                b = self.scaleTci(self.getTmpBand(self.B02))
            except:
                r = self.scaleTci(int16(self.getBand(self.B04) * self.config.dnScale))
                g = self.scaleTci(int16(self.getBand(self.B03) * self.config.dnScale))
                b = self.scaleTci(int16(self.getBand(self.B02) * self.config.dnScale))
            try:
                if self.config.raw or self.config.tif:
                    self.exportRawImage(tci, dstack((r, g, b)))
                else:
                    self.glymurWrapper(tci, dstack((r, g, b)))
                    
                self.config.timestamp('L2A_Tables: band TCI exported')
            except Exception as e:
                self.logger.fatal('TCI image export failed' + str(e))
                self.config.timestamp('L2A_Tables: TCI image export failed')
                return False

            return True

    def scalePreview(self, arr):
        if(arr.ndim) != 2:
            self.logger.fatal('must be a 2 dimensional array')
            return False

        if self.logger.level != logging.DEBUG:
            src_ncols = self.config.ncols
            src_nrows = self.config.nrows
            tgt_ncols = 343.0
            tgt_nrows = 343.0
            zoomX = float64(tgt_ncols)/float64(src_ncols)
            zoomY = float64(tgt_nrows)/float64(src_nrows)
            arr = zoom(arr, ([zoomX,zoomY]), order=0)

        min_ = 0.0
        max_ = 2500.0
        scale = 254.0
        offset = 1.0

        # SIITBX-50: wrong scale was used:
        # scaling in line with L1C TCI with 0 reserved for No_Data
        scaledArr = uint8(clip(arr, min_, max_) * scale / max_ + offset)
        scaledArr[arr == 0.0] = 0
        return scaledArr


    def scaleTci(self, arr):
        # check if arr could be in native uint16 instead float16
        if (arr.ndim) != 2:
            self.logger.fatal('must be a 2 dimensional array')
            return False

        min_ = float32(0.0)
        max_ = float32(2500.0)
        scale = float32(254.0)
        offset = float32(1.0)

        # SIITBX-50: wrong scale was used:
        # scaling in line with L1C TCI with 0 reserved for No_Data
        scaledArr = uint8(clip(arr, min_, max_) * scale / max_ + offset)
        scaledArr[arr == 0.0] = 0
        return scaledArr

    def testDb(self, filename):
        h5file = None
        try:
            h5file = open_file(filename, mode='r')
            h5file.get_node('/arrays', 'B02')
            self.logger.info('Database ' + filename + ' exists and can be used')
            return True
        except:
            self.logger.info('Database  ' + filename + ' will be removed due to corruption')
            os.remove(filename)
            return False
        finally:
            if h5file:
                h5file.close()

    def hasBand(self, index):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        try:
            if index < 13:
                h5file = open_file(self._imgdb, mode='r')
            else:
                h5file = open_file(self._resdb, mode='r')
            h5file.get_node('/arrays', bandName)
            self.logger.debug('Channel %s is present', self.getBandNameFromIndex(index))
            return True
        except:
            self.logger.debug('Channel %s is not available', self.getBandNameFromIndex(index))
            return False
        finally:
            if h5file:
                h5file.close()

    def getBandSize(self, index, resampled=False):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        try:
            if resampled:
                h5file = open_file(self._resdb, mode='r')
            else:
                h5file = open_file(self._imgdb, mode='r')
            table = h5file.root.metadata.META
            for x in table.iterrows():
                if(x['bandName'] == bandName):
                    if resampled:
                        src_nrows = x['rasterYSize']
                        tgt_nrows = self.config.nrows
                        if src_nrows == tgt_nrows:
                            nrows = x['rasterYSize']
                            ncols = x['rasterXSize']
                            count = x['rasterCount']
                        elif (src_nrows / tgt_nrows) == 2:
                            nrows = x['rasterYSize'] * 2
                            ncols = x['rasterXSize'] * 2
                            count = x['rasterCount'] * 2
                        elif (src_nrows / tgt_nrows) == 3:
                            nrows = x['rasterYSize'] * 3
                            ncols = x['rasterXSize'] * 3
                            count = x['rasterCount'] * 3
                        elif (src_nrows / tgt_nrows) == 6:
                            nrows = x['rasterYSize'] * 6
                            ncols = x['rasterXSize'] * 6
                            count = x['rasterCount'] * 6
                    else:
                        nrows = x['rasterYSize']
                        ncols = x['rasterXSize']
                        count = x['rasterCount']
                    break
            table.flush()
            return(nrows, ncols, count)
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def getBand(self, index):
        # the output is context sensitive
        # it will return TOA_reflectance (0:1) if index < 13
        # it will return the unmodified value if index > 12
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        try:
            if index < 13:
                h5file = open_file(self._imgdb, mode='r')
                node = h5file.get_node('/arrays', bandName)
                array = node.read()
                h5file.close()
                h5file = None
                if self.config.logLevel == 'DEBUG':
                    self.readoutStatistics(bandName)
                src_nrows = array.shape[0]
                tgt_nrows = self.config.nrows
                if src_nrows == tgt_nrows:
                    array = float32(array)
                    return (array / float32(self.config.dnScale))  # scaling from 0:1
            # else:
            resArr = self.getResampledBand(index)
            try:
                if resArr == False:
                    self.config.timestamp('L2A_Tables: band ' + bandName + ' must be resampled')
                    array = self.resampleBand(index, array)
                    if not self.setResampledBand(index, array):
                        return False
            except:
                array = resArr

            if(index > 12):
                return array # no further modification
            else: # return reflectance value:
                array = float32(array)
                return (array / float32(self.config.dnScale))  # scaling from 0:1
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def getDataType(self, index):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        try:
            if index < 13:
                h5file = open_file(self._imgdb, mode='r')
            else:
                h5file = open_file(self._resdb, mode='r')
            node = h5file.get_node('/arrays', bandName)
            dt = node.dtype
            return(dt)
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def setBand(self, index, array):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        if self.config.logLevel == 'DEBUG':
            self.readoutStatistics(bandName, read = False)
        try:
            h5file = open_file(self._resdb, mode='a')
            if(h5file.__contains__('/arrays/' + bandName)):
                node = h5file.get_node('/arrays', bandName)
                node.remove()

            arr = h5file.root.arrays
            dtIn = self.setDataType(array.dtype)
            filters = Filters(complib='zlib', complevel=self.config.db_compression_level)
            node = h5file.create_earray(arr, bandName, dtIn, (0,array.shape[1]), bandName, filters=filters)
            self.logger.debug('Channel %02d %s added to table', index, self.getBandNameFromIndex(index))
            node.append(array)

            table = h5file.root.metadata.META
            update = False
            # if row exists, change it:
            for row in table.iterrows():
                if(row['bandName'] == bandName):
                    row['rasterYSize'] = array.shape[0]
                    row['rasterXSize'] = array.shape[1]
                    row['rasterCount'] = 1
                    row.update()
                    update = True
            # else append it:
            if(update == False):
                row = table.row
                row['bandName'] = bandName
                row['rasterYSize'] = array.shape[0]
                row['rasterXSize'] = array.shape[1]
                row['rasterCount'] = 1
                row.append()
            table.flush()
            return True
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def removeBandImg(self, index):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        try:
            h5file = open_file(self._imgdb, mode='a')
            table = h5file.root.metadata.META
            if(h5file.__contains__('/arrays/' + bandName)):
                node = h5file.get_node('/arrays', bandName)
                node.remove()
                table.flush()
                self.logger.debug('Channel %02d %s removed from table', index, self.getBandNameFromIndex(index))
            return True
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def removeBandRes(self, index):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        try:
            h5file = open_file(self._resdb, mode='a')
            table = h5file.root.metadata.META
            if(h5file.__contains__('/arrays/' + bandName)):
                node = h5file.get_node('/arrays', bandName)
                node.remove()
                table.flush()
                self.logger.debug('Channel %02d %s removed from table', index, self.getBandNameFromIndex(index))
            return True
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def removeAllBands(self):
        h5file = None
        try:
            h5file = open_file(self._resdb, mode='a')
            table = h5file.root.metadata.META
            for index in range(0,37):
                bandName = self.getBandNameFromIndex(index)
                if(h5file.__contains__('/arrays/' + bandName)):
                    node = h5file.get_node('/arrays', bandName)
                    node.remove()
            table.flush()
            self.logger.debug('All channels removed from table')
            result = self.removeAllTmpBands()
            if result == False:
                return False
            return self.removeAllResampledBands()
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def getTmpBand(self, index):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        try:
            h5file = open_file(self._resdb, mode='r')
            node = h5file.get_node('/tmp', bandName)
            array = node.read()
            if self.config.logLevel == 'DEBUG':
                self.readoutStatistics(bandName)
            return array
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def setTmpBand(self, index, array):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        try:
            h5file = open_file(self._resdb, mode='a')
            table = h5file.root.metadata.META
            if(h5file.__contains__('/tmp/' + bandName)):
                node = h5file.get_node('/tmp', bandName)
                node.remove()
                table.flush()
            tmp = h5file.root.tmp
            dtIn = self.setDataType(array.dtype)
            filters = Filters(complib='zlib', complevel=self.config.db_compression_level)
            node = h5file.create_earray(tmp, bandName, dtIn, (0,array.shape[1]), bandName, filters=filters)
            self.logger.debug('Temporary channel ' + str(index) + ' added to table')
            node.append(array)
            if self.config.logLevel == 'DEBUG':
                self.readoutStatistics(bandName, read = False)
            table.flush()
            return True
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def removeTmpBand(self, index):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        try:
            h5file = open_file(self._resdb, mode='a')
            table = h5file.root.metadata.META
            if(h5file.__contains__('/tmp/' + bandName)):
                node = h5file.get_node('/tmp', bandName)
                node.remove()
                table.flush()
                self.logger.debug('Temporary channel ' + str(index) + ' removed from table')
            return True
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def removeAllTmpBands(self):
        h5file = None
        try:
            h5file = open_file(self._resdb, mode='a')
            table = h5file.root.metadata.META
            for index in range(0,37):
                bandName = self.getBandNameFromIndex(index)
                if(h5file.__contains__('/tmp/' + bandName)):
                    node = h5file.get_node('/tmp', bandName)
                    node.remove()
            table.flush()
            self.logger.debug('All temporary bands removed from table')
            return True
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def getResampledBand(self, index):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        try:
            h5file = open_file(self._resdb, mode='r')
            node = h5file.get_node('/arrays', bandName)
            array = node.read()
            if self.config.logLevel == 'DEBUG':
                self.readoutStatistics(bandName)
            return array
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def setResampledBand(self, index, array):
        h5file = None
        bandName = self.getBandNameFromIndex(index)
        try:
            h5file = open_file(self._resdb, mode='a')
            table = h5file.root.metadata.META
            if(h5file.__contains__('/arrays/' + bandName)):
                node = h5file.get_node('/arrays', bandName)
                node.remove()
                table.flush()
            arrays = h5file.root.arrays
            dtIn = self.setDataType(array.dtype)
            filters = Filters(complib='zlib', complevel=self.config.db_compression_level)
            node = h5file.create_earray(arrays, bandName, dtIn, (0,array.shape[1]), bandName, filters=filters)
            self.logger.debug('Resampled band ' + str(index) + ' added to table')
            node.append(array)
            if self.config.logLevel == 'DEBUG':
                self.readoutStatistics(bandName, read = False)
            table.flush()
            return True
        except:
            return False
        finally:
            if h5file:
                h5file.close()

    def removeAllResampledBands(self):
        h5file = None
        try:
            h5file = open_file(self._resdb, mode='a')
            table = h5file.root.metadata.META
            for index in range(0,37):
                bandName = self.getBandNameFromIndex(index)
                if(h5file.__contains__('/arrays/' + bandName)):
                    node = h5file.get_node('/arrays', bandName)
                    node.remove()
            table.flush()
            self.logger.debug('All resampled bands removed from table')
            return True
        except:
            return False
        finally:
            if h5file:
                h5file.close()


    def setDataType(self, dtIn):
        if(dtIn == uint8):
            dtOut = UInt8Atom()
        elif(dtIn == uint16):
            dtOut = UInt16Atom()
        elif(dtIn == int16):
            dtOut = Int16Atom()
        elif(dtIn == uint32):
            dtOut = UInt32Atom()
        elif(dtIn == int32):
            dtOut = Int32Atom()
        elif(dtIn == float32):
            dtOut = Float32Atom()
        elif(dtIn == float64):
            dtOut = Float64Atom()
        elif(dtIn == GDT_Byte):
            dtOut = UInt8Atom()
        elif(dtIn == GDT_UInt16):
            dtOut = UInt16Atom()
        elif(dtIn == GDT_Int16):
            dtOut = Int16Atom()
        elif(dtIn == GDT_UInt32):
            dtOut = UInt32Atom()
        elif(dtIn == GDT_Int32):
            dtOut = Int32Atom()
        elif(dtIn == GDT_Float32):
            dtOut = Float32Atom()
        elif(dtIn == GDT_Float64):
            dtOut = Float64Atom()

        return dtOut

    def getArray(self, filename):
        filename = self._testdir + filename + '.npy'
        if((os.path.isfile(filename)) == False):
            self.logger.critical('File ' + filename + ' not present')
            return False

        return load(filename)

    def getDiffFromArrays(self, filename1, filename2):
        filename1 = self._testdir + filename1 + '.npy'
        filename2 = self._testdir + filename2 + '.npy'
        if((os.path.isfile(filename1)) == False):
            self.logger.critical('File ' + filename1 + ' not present')
            return False

        if((os.path.isfile(filename2)) == False):
            self.logger.critical('File ' + filename2 + ' not present')
            return False

        arr1 = load(filename1)
        arr2 = load(filename2)
        return (arr1-arr2)

    def saveArray(self, filename, arr):
        filename = self._testdir + filename + '.npy'
        save(filename, arr)

        if(os.path.exists(self._L2A_bandDir) == False):
            os.makedirs(self._L2A_bandDir)
            self.logger.info('File ' + filename + ' saved to disk')
        return

    def readoutStatistics(self, bandname, read = True):
        if read:
            key = 'read'
        else:
            key ='write'

        if bandname not in self.config._stat[key]:
            self.config._stat[key][bandname] = 1
        else:
            self.config._stat[key][bandname] += 1

    def appendTile(self):
        l.acquire()
        try:
            xp = L2A_XmlParser(self.config, 'DS2A')
            ti = xp.getTree('Image_Data_Info', 'Tiles_Information')
            tl = ti.Tile_List
            L2A_TILE_ID = self.config.L2A_TILE_ID
            Tile = objectify.Element('Tile', tileId=L2A_TILE_ID)

            for i in range(len(tl)):
                if tl[i].Tile.attrib['tileId'] == L2A_TILE_ID:
                    break  # else:
                ti.Tile_List.append(Tile)
                xp.export()
                xp.validate()
        except:  # tile list is empty:
            ti.Tile_List.append(Tile)
            xp.export()
            xp.validate()
        finally:
            l.release()
        return len(ti.Tile_List.Tile)

    def sceneCouldHaveSnow(self):
        
        globalSnowMapFn = self.config.snowMapReference
        globalSnowMapFn = os.path.join(self.config.auxDir, globalSnowMapFn)
        if((os.path.isfile(globalSnowMapFn)) == False):
            self.logger.error('global snow map not present, snow detection will be performed')
            return True
        
        l.acquire()
        try:
            img = Image.open(globalSnowMapFn)
        finally:
            l.release()
            
        globalSnowMap = array(img)
        xy = self.cornerCoordinates
        xp = L2A_XmlParser(self.config, 'T2A')
        tg = xp.getTree('Geometric_Info', 'Tile_Geocoding')
        hcsName = tg.HORIZONTAL_CS_NAME.text
        zone = hcsName.split()[4]
        zone1 = int(zone[:-1])
        zone2 = zone[-1:].upper()
        lonMin, latMin, dummy = transform_utm_to_wgs84(xy[1,0], xy[1,1], zone1, zone2)
        lonMax, latMax, dummy = transform_utm_to_wgs84(xy[3,0], xy[3,1], zone1, zone2)
        # lat_cen = int((latMax + latMin) / 2)

        # Snow map should have a dimension of 7200 x 3600, 20 pixels per degree:
        xMin = int((lonMin + 180.0) * 20.0 + 0.5)
        xMax = int((lonMax + 180.0) * 20.0 + 0.5)
        yMin = 3600 - int((latMax + 90.0) * 20.0 + 0.5) # Inverted by intention
        yMax = 3600 - int((latMin + 90.0) * 20.0 + 0.5) # Inverted by intention

        # fix for SIIMPC-944 JL - handling case of international date line (Longitude +180/-180 line)
        if xMin < xMax:
            aoi = globalSnowMap[yMin:yMax, xMin:xMax]
        else:
            aoi_east = globalSnowMap[yMin:yMax, xMin:]
            aoi_west = globalSnowMap[yMin:yMax, 0:xMax]
            aoi = concatenate((aoi_east, aoi_west), axis=1)
        # end fix SIIMPC-944 JL

        if(aoi.max() > 0):
            return True
        
        return False

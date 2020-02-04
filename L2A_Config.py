#!/usr/bin/env python

from numpy import *
import fnmatch

import cPickle as pickle
import sys, os, logging, inspect
import ConfigParser
from L2A_XmlParser import L2A_XmlParser
from L2A_Library import *
from lxml import etree, objectify
from time import time, strftime
from datetime import datetime
from multiprocessing import Lock
from shutil import copyfile, copytree
from psutil import cpu_count
from L2A_ProcessDataStrip import L2A_ProcessDataStrip


try:
    from osgeo import gdal, osr
    from osgeo.gdalconst import *

    gdal.TermProgress = gdal.TermProgress_nocb
except ImportError:
    import gdal, osr
    from gdalconst import *

l = Lock()


def getScriptDir(follow_symlinks=True):
    if getattr(sys, 'frozen', False):  # py2exe, PyInstaller, cx_Freeze
        path = os.path.abspath(sys.executable)
    else:
        path = inspect.getabsfile(getScriptDir)
    if follow_symlinks:
        path = os.path.realpath(path)
    return os.path.dirname(path)


class L2A_Config(object):
    def __init__(self, logger, input_dir=False):
        # set TESTMODE to 1 if you want to have a higher processing speed for test purposes.
        # processes only the upper left area of 183 x 183 pixels: must contain some data pixels!
        # leave to 0 if normal processing is wanted (default).
        self._TESTMODE = 0
        self._processorName = 'Sentinel-2 Level 2A Processor (Sen2Cor)'
        self._processorVersion = '2.8.0'
        self._processorDate = '2019.02.20'
        self._productVersion = '14.5'
        self._processing_centre = None
        self._archiving_centre = None
        self._datastrip = None
        self._datastrip_root_folder = None
        self._tile = None
        self._raw = None
        self._tif = None
        self._geobox_10 = None
        self._geobox_20 = None
        self._geobox_60 = None
        self._geoboxPvi = None
        self._user_product = None
        self._input_dir = None
        self._work_dir = None
        self._output_dir = None
        self._img_database_dir = None
        self._res_database_dir = None
        self._operationMode = 'INVALID'
        self._processingBaseline = None
        self._configPB = None
        self._logger = logger
        self._logLevel = 'INFO'
        self._sc_lp_blu = 1.0
        self._tEstimation = 0.0
        self._L2A_DS_MTD_LST = []
        self._L2A_DS_MTD_XML = None
        self._dsScheme1c = None
        self._dsScheme2a = None
        self._timestamp = datetime.utcnow()
        self._localTimestamp = time()
        self._UPgenerationTimestamp = None
        self._spacecraftName = None
        
        if (input_dir):

            self._atmDataFn = None
            self._tEst60 = 150.0
            self._tEst20 = self._tEst60 * 8.0
            self._tEst10 = self._tEst60 * 8.0
            self._tStart = None
            self._nrTiles = None
            self._nrThreads = None
            self._input_dir = input_dir

            self._ncols = -1
            self._nrows = -1
            self._nbnds = -1
            self._sza = -1
            self._saa = -1
            self._GIPP = ''
            self._ECMWF = ''
            self._DEM = ''
            self._L2A_BOA_QUANTIFICATION_VALUE = None
            self._L2A_WVP_QUANTIFICATION_VALUE = 1000.0
            self._L2A_AOT_QUANTIFICATION_VALUE = 1000.0
            self._dnScale = 10000.0
            self._adj_km = 1.0
            self._ch940 = array([8, 8, 9, 9, 0, 0])
            self._cellsize = 0  # pixelsize (m), cellsize (km)
            self._dem_terrain_correction = True
            self._ibrdf = 0  # brdf correction
            self._thr_g = 0.25  # lower bound for brdf correction
            self._ibrdf_dark = 0  # flag for cast shadow dark pixel processing
            self._icl_shadow = 0  # no cloud shadow calculation
            self._iclshad_mask = 3  # 1=small, 2=medium, 3=large mask with default values of thr_shad
            self._ihaze = 0
            self._ihcw = 1
            self._ihot_dynr = 2
            self._ihot_mask = 2
            self._intpol760 = 1  # always 1 for Sentinel2
            self._intpol725_825 = 1  # always 1 for Sentinel2
            self._intpol1400 = 1  # always 1 for Sentinel2
            self._intpol940_1130 = 1  # always 1 for Sentinel2
            self._istretch_type = 1  # linear
            self._iwat_shd = 0
            self._iwaterwv = 1
            self._iwv_ndvi = 0
            self._iwv_watermask = 1  # (1=average wv is used for water pixels)
            self._ksolflux = 0
            self._altit = 0.1
            self._npref = 0
            self._phi_scl_min = 0.05  # lower bound of shadow fraction, default = 0.05
            self._phi_unscl_max = -999.0  # initialize in Common/atcor_main
            self._pixelsize = 0
            self._resolution = 0
            self._rel_saturation = 1.00
            self._thr_shad = -999.0
            self._thv = 0.0  # tilt view angle, tilt azimuth angle
            self._phiv = 90.0  # sensor view zenith and azimuth angle
            self._smooth_wvmap = 100.0
            self._entityId = ''
            self._acquisitionDate = ''
            self._orbitPath = None
            self._orbitRow = None
            self._targetPath = None
            self._targetRow = None
            self._stationSgs = ''
            self._sceneStartTime = None
            self._sceneStopTime = None
            self._solaz = None
            self._solaz_arr = None
            self._solze = None
            self._solze_arr = None
            self._vaa_arr = None
            self._vza_arr = None
            self._visibility = 40.0
            self._wl940a = array([0.895, 1.000])  # range of moderate wv absorption region around  940 nm
            self._wl1130a = array([1.079, 1.180])  # range of moderate wv absorption region around 1130 nm
            self._wl1400a = array([1.330, 1.490])  # range for interpolation
            self._wl1900a = array([1.780, 1.970])  # range for interpolation
            self._wv_thr_cirrus = 0.60
            self._icirrus = 0
            self._d2 = 1.0  # this is a constant, see explanation in line 2061
            self._c0 = None
            self._c1 = None
            self._e0 = None
            self._wvlsen = None
            self._fwhm = None
            self._acOnly = False
            self._L2A_INSPIRE_XML = None
            self._L2A_MANIFEST_SAFE = None
            self._L1C_UP_MTD_XML = None
            self._L1C_DS_MTD_XML = None
            self._L1C_TILE_MTD_XML = None
            self._L1C_UP_ID = None
            self._L1C_DS_ID = None
            self._L1C_TILE_ID = None
            self._L2A_UP_MTD_XML = None
            self._L2A_DS_MTD_XML = None
            self._L2A_DS_MTD_LST = []
            self._L2A_TILE_MTD_XML = None
            self._L2A_UP_ID = None
            self._L2A_DS_LST = []
            self._L2A_DS_ID = None
            self._L2A_TILE_ID = None
            self._creationDate = None
            self._scOnly = False
            self._processed60 = False
            self._processed20 = False
            self._processed10 = False
            self._selectedTile = None
            self._aerosolType = None
            self._midLatitude = None
            self._ozoneContent = None
            self._waterVapour = None
            self._ozoneSetpoint = None
            self._timeout = None
            # fix for SIIMPC-552 and SIIMPC-557, UMW - making options configurable:
            self._scaling_disabler = None
            self._scaling_limiter = None
            self._rho_retrieval_step2 = None
            self._min_sc_blu = 0.9
            self._max_sc_blu = 1.1
            self._namingConvention = None
            self.datatakeSensingTime = None
            self._upScheme1c = None
            self._upScheme2a = None
            self._tileScheme1c = None
            self._tileScheme2a = None
            self._dsScheme1c = None
            self._dsScheme2a = None
            self._gippScheme2a = None
            self._gippSchemeSc = None
            self._gippSchemeAc = None
            self._gippSchemePb = None
            self._manifestScheme = None
            self._satelliteId = 'S2A'
            self._demOutput = False
            self._tciOutput = False
            self._ddvOutput = False
            self._downsample20to60 = True
            self._processingStartTimestamp = datetime.utcnow()
            self._AC_Min_Ddv_Area = None
            self._demType = 'NONE'
            self._stat = {'read':{},'write':{}}
            self._db_compression_level = 0
            self._UP_INDEX_HTML = None
            self._INSPIRE_XML = None

    def get_processing_centre(self):
        return self._processing_centre

    def get_archiving_centre(self):
        return self._archiving_centre

    def get_datastrip(self):
        return self._datastrip

    def get_datastrip_root_folder(self):
        return self._datastrip_root_folder

    def get_tile(self):
        return self._tile
    
    def get_raw(self):
        return self._raw

    def get_tif(self):
        return self._tif    

    def get_geoboxPvi(self):
        return self._geoboxPvi

    def get_user_product(self):
        return self._user_product

    def get_input_dir(self):
        return self._input_dir

    def get_work_dir(self):
        return self._work_dir

    def get_img_database_dir(self):
        return self._img_database_dir

    def get_res_database_dir(self):
        return self._res_database_dir

    def get_output_dir(self):
        return self._output_dir

    def set_processing_centre(self, value):
        self._processing_centre = value

    def set_archiving_centre(self, value):
        self._archiving_centre = value

    def set_datastrip(self, value):
        self._datastrip = value

    def set_datastrip_root_folder(self, value):
        self._datastrip_root_folder = value

    def set_tile(self, value):
        self._tile = value

    def set_raw(self, value):
        self._raw = value
        
    def set_tif(self, value):
        self._tif = value
        
    def set_geoboxPvi(self, value):
        self._geoboxPvi = value

    def set_user_product(self, value):
        self._user_product = value

    def set_input_dir(self, value):
        self._input_dir = value

    def set_work_dir(self, value):
        self._work_dir = value

    def set_img_database_dir(self, value):
        self._img_database_dir = value

    def set_res_database_dir(self, value):
        self._res_database_dir = value

    def set_output_dir(self, value):
        self._output_dir = value

    def del_processing_centre(self):
        del self._processing_centre

    def del_archiving_centre(self):
        del self._archiving_centre

    def del_datastrip(self):
        del self._datastrip

    def del_datastrip_root_folder(self):
        del self._datastrip_root_folder

    def del_tile(self):
        del self._tile
        
    def del_raw(self):
        del self._raw
        
    def del_tif(self):
        del self._tif

    def del_geoboxPvi(self):
        del self._geoboxPvi

    def del_user_product(self):
        del self._user_product

    def del_input_dir(self):
        del self._input_dir

    def del_work_dir(self):
        del self._work_dir

    def del_img_database_dir(self):
        del self._img_database_dir

    def del_res_database_dir(self):
        del self._res_database_dir

    def del_output_dir(self):
        del self._output_dir

    def get_dem_type(self):
        return self._demType

    def set_dem_type(self, value):
        self._demType = value

    def del_dem_type(self):
        del self._demType

    def get_ac_min_ddv_area(self):
        return self._AC_Min_Ddv_Area

    def get_up_scheme_1c(self):
        return self._upScheme1c

    def get_ac_swir_refl_lower_th(self):
        return self._AC_Swir_Refl_Lower_Th

    def get_up_scheme_2a(self):
        return self._upScheme2a

    def get_ac_ddv_16um_refl_th_3(self):
        return self._AC_Ddv_16um_Refl_Th3

    def get_tile_scheme_1c(self):
        return self._tileScheme1c

    def get_tile_scheme_2a(self):
        return self._tileScheme2a

    def get_ac_swir_22um_red_refl_ratio(self):
        return self._AC_Swir_22um_Red_Refl_Ratio

    def get_ds_scheme_1c(self):
        return self._dsScheme1c

    def get_ac_red_blue_refl_ratio(self):
        return self._AC_Red_Blue_Refl_Ratio

    def get_ds_scheme_2a(self):
        return self._dsScheme2a

    def get_ac_cut_off_aot_iter_vegetation(self):
        return self._AC_Cut_Off_Aot_Iter_Vegetation

    def get_gipp_scheme_2a(self):
        return self._gippScheme2a

    def get_gipp_scheme_pb(self):
        return self._gippSchemePb

    def get_ac_cut_off_aot_iter_water(self):
        return self._AC_Cut_Off_Aot_Iter_Water

    def get_gipp_scheme_sc(self):
        return self._gippSchemeSc

    def get_ac_aerosol_type_ratio_th(self):
        return self._AC_Aerosol_Type_Ratio_Th

    def get_gipp_scheme_ac(self):
        return self._gippSchemeAc

    def get_ac_topo_corr_th(self):
        return self._AC_Topo_Corr_Th

    def get_manifest_scheme(self):
        return self._manifestScheme

    def get_ac_slope_th(self):
        return self._AC_Slope_Th

    def get_ac_dem_p2p_val(self):
        return self._AC_Dem_P2p_Val

    def set_up_scheme_1c(self, value):
        self._upScheme1c = value

    def get_ac_swir_refl_ndvi_th(self):
        return self._AC_Swir_Refl_Ndvi_Th

    def set_up_scheme_2a(self, value):
        self._upScheme2a = value

    def get_ac_ddv_swir_refl_th_1(self):
        return self._AC_Ddv_Swir_Refl_Th1

    def set_tile_scheme_1c(self, value):
        self._tileScheme1c = value

    def get_ac_ddv_swir_refl_th_2(self):
        return self._AC_Ddv_Swir_Refl_Th2

    def set_tile_scheme_2a(self, value):
        self._tileScheme2a = value

    def get_ac_ddv_swir_refl_th_3(self):
        return self._AC_Ddv_Swir_Refl_Th3

    def set_ds_scheme_1c(self, value):
        self._dsScheme1c = value

    def get_ac_ddv_16um_refl_th_1(self):
        return self._AC_Ddv_16um_Refl_Th1

    def set_ds_scheme_2a(self, value):
        self._dsScheme2a = value

    def get_ac_ddv_16um_refl_th_2(self):
        return self._AC_Ddv_16um_Refl_Th2

    def set_gipp_scheme_2a(self, value):
        self._gippScheme2a = value

    def set_gipp_scheme_pb(self, value):
        self._gippSchemePb = value

    def get_ac_dbv_nir_refl_th(self):
        return self._AC_Dbv_Nir_Refl_Th

    def set_gipp_scheme_sc(self, value):
        self._gippSchemeSc = value

    def get_ac_dbv_ndvi_th(self):
        return self._AC_Dbv_Ndvi_Th

    def set_gipp_scheme_ac(self, value):
        self._gippSchemeAc = value

    def get_ac_red_ref_refl_th(self):
        return self._AC_Red_Ref_Refl_Th

    def set_manifest_scheme(self, value):
        self._manifestScheme = value

    def get_ac_dbv_red_veget_tst_ndvi_th(self):
        return self._AC_Dbv_Red_Veget_Tst_Ndvi_Th

    def del_up_scheme_1c(self):
        del self._upScheme1c

    def get_ac_dbv_red_veget_refl_th(self):
        return self._AC_Dbv_Red_Veget_Refl_Th

    def del_up_scheme_2a(self):
        del self._upScheme2a

    def get_ac_wv_iter_start_summer(self):
        return self._AC_Wv_Iter_Start_Summer

    def get_ac_wv_iter_start_winter(self):
        return self._AC_Wv_Iter_Start_Winter

    def del_tile_scheme_1c(self):
        del self._tileScheme1c

    def del_tile_scheme_2a(self):
        del self._tileScheme2a

    def del_ds_scheme_1c(self):
        del self._dsScheme1c

    def del_ds_scheme_2a(self):
        del self._dsScheme2a

    def get_ac_rng_nbhd_terrain_corr(self):
        return self._AC_Rng_Nbhd_Terrain_Corr

    def del_gipp_scheme_2a(self):
        del self._gippScheme2a

    def del_gipp_scheme_pb(self):
        del self._gippSchemePb

    def get_ac_max_nr_topo_iter(self):
        return self._AC_Max_Nr_Topo_Iter

    def del_gipp_scheme_sc(self):
        del self._gippSchemeSc

    def get_ac_topo_corr_cutoff(self):
        return self._AC_Topo_Corr_Cutoff

    def del_gipp_scheme_ac(self):
        del self._gippSchemeAc

    def del_manifest_scheme(self):
        del self._manifestScheme

    def get_ac_vegetation_index_th(self):
        return self._AC_Vegetation_Index_Th

    def get_datatake_sensing_time(self):
        return self._datatakeSensingTime

    def get_ac_limit_area_path_rad_scale(self):
        return self._AC_Limit_Area_Path_Rad_Scale

    def set_datatake_sensing_time(self, value):
        self._datatakeSensingTime = value

    def get_ac_ddv_smooting_window(self):
        return self._AC_Ddv_Smooting_Window

    def del_datatake_sensing_time(self):
        del self._datatakeSensingTime

    def get_ac_terrain_refl_start(self):
        return self._AC_Terrain_Refl_Start

    def get_ac_spr_refl_percentage(self):
        return self._AC_Spr_Refl_Percentage

    def get_ac_spr_refl_promille(self):
        return self._AC_Spr_Refl_Promille

    def get_naming_convention(self):
        return self._namingConvention

    def set_naming_convention(self, value):
        self._namingConvention = value

    def del_naming_convention(self):
        del self._namingConvention

    def get_dem_output(self):
        return self._demOutput

    def set_dem_output(self, value):
        self._demOutput = value

    def del_dem_output(self):
        del self._demOutput

    def get_tci_output(self):
        return self._tciOutput

    def set_tci_output(self, value):
        self._tciOutput = value

    def del_tci_output(self):
        del self._tciOutput

    def get_ddv_output(self):
        return self._ddvOutput

    def set_ddv_output(self, value):
        self._ddvOutput = value

    def del_ddv_output(self):
        del self._ddvOutput

    def get_downsample_20to60(self):
        return self._downsample20to60

    def set_downsample_20to60(self, value):
        self._downsample20to60 = value

    def del_downsample_20to60(self):
        del self._downsample20to60

    def get_min_sc_blu(self):
        return self._min_sc_blu

    def set_min_sc_blu(self, value):
        self._min_sc_blu = value

    def get_ac_max_vis_iter(self):
        return self._AC_Max_Vis_Iter

    def del_min_sc_blu(self):
        del self._min_sc_blu

    def set_ac_min_ddv_area(self, value):
        pass

    def get_max_sc_blu(self):
        return self._max_sc_blu

    def get_db_compression_level(self):
        return self._db_compression_level

    def set_ac_swir_refl_lower_th(self, value):
        self._AC_Swir_Refl_Lower_Th = value

    def set_max_sc_blu(self, value):
        self._max_sc_blu = value

    def set_db_compression_level(self, value):
        self._db_compression_level = value

    def set_ac_ddv_16um_refl_th_3(self, value):
        self._AC_Ddv_16um_Refl_Th3 = value

    def del_max_sc_blu(self):
        del self._max_sc_blu

    def del_db_compression_level(self):
        del self._db_compression_level

    # fix for SIIMPC-552, UMW - making option configurable:
    def get_scaling_disabler(self):
        return self._scaling_disabler

    def set_scaling_disabler(self, value):
        self._scaling_disabler = value

    def del_scaling_disabler(self):
        del self._scaling_disabler

    def get_scaling_limiter(self):
        return self._scaling_limiter

    def set_scaling_limiter(self, value):
        self._scaling_limiter = value

    def del_scaling_limiter(self):
        del self._scaling_limiter

    def set_ac_swir_16um_red_refl_ratio(self, value):
        self._AC_Swir_16um_Red_Refl_Ratio = value

    def set_ac_swir_22um_red_refl_ratio(self, value):
        self._AC_Swir_22um_Red_Refl_Ratio = value

    def set_ac_red_blue_refl_ratio(self, value):
        self._AC_Red_Blue_Refl_Ratio = value

    def set_ac_cut_off_aot_iter_vegetation(self, value):
        self._AC_Cut_Off_Aot_Iter_Vegetation = value

    def set_ac_cut_off_aot_iter_water(self, value):
        self._AC_Cut_Off_Aot_Iter_Water = value

    def set_ac_aerosol_type_ratio_th(self, value):
        self._AC_Aerosol_Type_Ratio_Th = value

    # implementation of SIIMPC-557, UMW:
    def get_rho_retrieval_step2(self):
        return self._rho_retrieval_step2

    def set_ac_topo_corr_th(self, value):
        self._AC_Topo_Corr_Th = value

    def set_rho_retrieval_step2(self, value):
        self._rho_retrieval_step2 = value

    def set_ac_slope_th(self, value):
        self._AC_Slope_Th = value

    def set_ac_dem_p2p_val(self, value):
        self._AC_Dem_P2p_Val = value

    def del_rho_retrieval_step2(self):
        del self._rho_retrieval_step2

    def set_ac_swir_refl_ndvi_th(self, value):
        self._AC_Swir_Refl_Ndvi_Th = value

    def get_timeout(self):
        return self._timeout

    def set_ac_ddv_swir_refl_th_1(self, value):
        self._AC_Ddv_Swir_Refl_Th1 = value

    def set_timeout(self, value):
        self._timeout = value

    def set_ac_ddv_swir_refl_th_2(self, value):
        self._AC_Ddv_Swir_Refl_Th2 = value

    def del_timeout(self):
        del self._timeout

    def set_ac_ddv_swir_refl_th_3(self, value):
        self._AC_Ddv_Swir_Refl_Th3 = value

    def set_ac_ddv_16um_refl_th_1(self, value):
        self._AC_Ddv_16um_Refl_Th1 = value

    def set_ac_ddv_16um_refl_th_2(self, value):
        self._AC_Ddv_16um_Refl_Th2 = value

    def set_ac_dbv_nir_refl_th(self, value):
        self._AC_Dbv_Nir_Refl_Th = value

    def set_ac_dbv_ndvi_th(self, value):
        self._AC_Dbv_Ndvi_Th = value

    def set_ac_red_ref_refl_th(self, value):
        self._AC_Red_Ref_Refl_Th = value

    def set_ac_dbv_red_veget_tst_ndvi_th(self, value):
        self._AC_Dbv_Red_Veget_Tst_Ndvi_Th = value

    def set_ac_dbv_red_veget_refl_th(self, value):
        self._AC_Dbv_Red_Veget_Refl_Th = value

    def set_ac_wv_iter_start_summer(self, value):
        self._AC_Wv_Iter_Start_Summer = value

    def set_ac_wv_iter_start_winter(self, value):
        self._AC_Wv_Iter_Start_Winter = value

    def set_ac_rng_nbhd_terrain_corr(self, value):
        self._AC_Rng_Nbhd_Terrain_Corr = value

    def set_ac_max_nr_topo_iter(self, value):
        self._AC_Max_Nr_Topo_Iter = value

    def set_ac_topo_corr_cutoff(self, value):
        self._AC_Topo_Corr_Cutoff = value

    def set_ac_vegetation_index_th(self, value):
        self._AC_Vegetation_Index_Th = value

    def set_ac_limit_area_path_rad_scale(self, value):
        self._AC_Limit_Area_Path_Rad_Scale = value

    def set_ac_ddv_smooting_window(self, value):
        self._AC_Ddv_Smooting_Window = value

    def set_ac_terrain_refl_start(self, value):
        self._AC_Terrain_Refl_Start = value

    def set_ac_spr_refl_percentage(self, value):
        self._AC_Spr_Refl_Percentage = value

    def set_ac_spr_refl_promille(self, value):
        self._AC_Spr_Refl_Promille = value

    def set_ac_max_vis_iter(self, value):
        self._AC_Max_Vis_Iter = value

    def del_ac_min_ddv_area(self):
        del self._AC_Min_Ddv_Area

    def del_ac_swir_refl_lower_th(self):
        del self._AC_Swir_Refl_Lower_Th

    def del_ac_ddv_16um_refl_th_3(self):
        del self._AC_Ddv_16um_Refl_Th3

    def del_ac_swir_22um_red_refl_ratio(self):
        del self._AC_Swir_22um_Red_Refl_Ratio

    def del_ac_red_blue_refl_ratio(self):
        del self._AC_Red_Blue_Refl_Ratio

    def del_ac_cut_off_aot_iter_vegetation(self):
        del self._AC_Cut_Off_Aot_Iter_Vegetation

    def del_ac_cut_off_aot_iter_water(self):
        del self._AC_Cut_Off_Aot_Iter_Water

    def del_ac_aerosol_type_ratio_th(self):
        del self._AC_Aerosol_Type_Ratio_Th

    def del_ac_topo_corr_th(self):
        del self._AC_Topo_Corr_Th

    def del_ac_slope_th(self):
        del self._AC_Slope_Th

    def del_ac_dem_p2p_val(self):
        del self._AC_Dem_P2p_Val

    def del_ac_swir_refl_ndvi_th(self):
        del self._AC_Swir_Refl_Ndvi_Th

    def del_ac_ddv_swir_refl_th_1(self):
        del self._AC_Ddv_Swir_Refl_Th1

    def del_ac_ddv_swir_refl_th_2(self):
        del self._AC_Ddv_Swir_Refl_Th2

    def del_ac_ddv_swir_refl_th_3(self):
        del self._AC_Ddv_Swir_Refl_Th3

    def del_ac_ddv_16um_refl_th_1(self):
        del self._AC_Ddv_16um_Refl_Th1

    def del_ac_ddv_16um_refl_th_2(self):
        del self._AC_Ddv_16um_Refl_Th2

    def del_ac_dbv_nir_refl_th(self):
        del self._AC_Dbv_Nir_Refl_Th

    def del_ac_dbv_ndvi_th(self):
        del self._AC_Dbv_Ndvi_Th

    def del_ac_red_ref_refl_th(self):
        del self._AC_Red_Ref_Refl_Th

    def del_ac_dbv_red_veget_tst_ndvi_th(self):
        del self._AC_Dbv_Red_Veget_Tst_Ndvi_Th

    def del_ac_dbv_red_veget_refl_th(self):
        del self._AC_Dbv_Red_Veget_Refl_Th

    def del_ac_wv_iter_start_summer(self):
        del self._AC_Wv_Iter_Start_Summer

    def del_ac_wv_iter_start_winter(self):
        del self._AC_Wv_Iter_Start_Winter

    def del_ac_rng_nbhd_terrain_corr(self):
        del self._AC_Rng_Nbhd_Terrain_Corr

    def del_ac_max_nr_topo_iter(self):
        del self._AC_Max_Nr_Topo_Iter

    def del_ac_topo_corr_cutoff(self):
        del self._AC_Topo_Corr_Cutoff

    def del_ac_vegetation_index_th(self):
        del self._AC_Vegetation_Index_Th

    def del_ac_limit_area_path_rad_scale(self):
        del self._AC_Limit_Area_Path_Rad_Scale

    def del_ac_ddv_smooting_window(self):
        del self._AC_Ddv_Smooting_Window

    def del_ac_terrain_refl_start(self):
        del self._AC_Terrain_Refl_Start

    def del_ac_spr_refl_percentage(self):
        del self._AC_Spr_Refl_Percentage

    def del_ac_spr_refl_promille(self):
        del self._AC_Spr_Refl_Promille

    def del_ac_max_vis_iter(self):
        del self._AC_Max_Vis_Iter

    def get_ozone_setpoint(self):
        return self._ozoneSetpoint

    def set_ozone_setpoint(self, value):
        self._ozoneSetpoint = value

    def del_ozone_setpoint(self):
        del self._ozoneSetpoint

    def get_aerosol_type(self):
        return self._aerosolType

    def get_mid_latitude(self):
        return self._midLatitude

    def get_ozone_content(self):
        return self._ozoneContent

    def get_water_vapour(self):
        return self._waterVapour

    def set_aerosol_type(self, value):
        self._aerosolType = value

    def set_mid_latitude(self, value):
        self._midLatitude = value

    def set_ozone_content(self, value):
        self._ozoneContent = value

    def set_water_vapour(self, value):
        self._waterVapour = value

    def del_aerosol_type(self):
        del self._aerosolType

    def del_mid_latitude(self):
        del self._midLatitude

    def del_ozone_content(self):
        del self._ozoneContent

    def del_water_vapour(self):
        del self._waterVapour

    def get_processing_status_fn(self):
        return self._processingStatusFn

    def get_processing_estimation_fn(self):
        return self._processingEstimationFn

    def set_processing_status_fn(self, value):
        self._processingStatusFn = value

    def set_processing_estimation_fn(self, value):
        self._processingEstimationFn = value

    def del_processing_status_fn(self):
        del self._processingStatusFn

    def del_processing_estimation_fn(self):
        del self._processingEstimationFn

    def get_nr_tiles(self):
        if self._nrTiles == 1:
            return self._nrTiles
        self._nrTiles = 0

        if self.namingConvention == 'SAFE_STANDARD':
            filemask = 'S2*_*_L1C_*'
        else:
            filemask = 'L1C_*'

        GRANULE = 'GRANULE'
        granuleDir = os.path.join(self.work_dir, GRANULE)
        filelist = sorted(os.listdir(granuleDir))
        for tile in filelist:
            if not fnmatch.fnmatch(tile, filemask):
                continue
            self._nrTiles += 1
        return self._nrTiles

    def get_nr_threads(self):
        nrThreads = self._nrThreads
        if nrThreads == 'AUTO':
            self._nrThreads = cpu_count()
        else:
            self._nrThreads = int(nrThreads)
        return self._nrThreads

    def set_nr_tiles(self, value):
        self._nrTiles = value

    def set_nr_threads(self, value):
        self._nrThreads = value

    def del_nr_tiles(self):
        del self._nrTiles

    def del_nr_threads(self):
        del self._nrThreads

    def get_t_start(self):
        return self._tStart

    def set_t_start(self, value):
        self._tStart = value

    def del_t_start(self):
        del self._tStart

    def get_testmode(self):
        return self._TESTMODE

    def set_testmode(self, value):
        self._TESTMODE = value

    def del_testmode(self):
        del self._TESTMODE

    def get_t_estimation(self):
        return self._tEstimation

    def set_t_estimation(self, value):
        self._tEstimation = value

    def del_t_estimation(self):
        del self._tEstimation

    def get_selected_tile(self):
        return self._selectedTile

    def set_selected_tile(self, value):
        self._selectedTile = value

    def del_selected_tile(self):
        del self._selectedTile

    def get_log_level(self):
        return self._logLevel

    def set_log_level(self, value):
        self._logLevel = value

    def del_log_level(self):
        del self._logLevel

    def get_fn_log(self):
        return self._fnLog

    def set_fn_log(self, value):
        self._fnLog = value

    def del_fn_log(self):
        del self._fnLog

    def get_operation_mode(self):
        return self._operationMode

    def set_operation_mode(self, value):
        self.init_home_directory(value)
        self._operationMode = value

    def del_operation_mode(self):
        del self._operationMode

    def get_logger(self):
        return self._logger

    def set_logger(self, value):
        self._logger = value

    def del_logger(self):
        del self._logger

    def get_sc_lp_blu(self):
        return self._sc_lp_blu

    def set_sc_lp_blu(self, value):
        self._sc_lp_blu = value

    def del_sc_lp_blu(self):
        del self._sc_lp_blu

    def get_processed_60(self):
        return self._processed60

    def get_processed_20(self):
        return self._processed20

    def get_processed_10(self):
        return self._processed10

    def set_processed_60(self, value):
        self._processed60 = value

    def set_processed_20(self, value):
        self._processed20 = value

    def set_processed_10(self, value):
        self._processed10 = value

    def del_processed_60(self):
        del self._processed60

    def del_processed_20(self):
        del self._processed20

    def del_processed_10(self):
        del self._processed10

    def get_sc_only(self):
        return self._scOnly

    def set_sc_only(self, value):
        self._scOnly = value

    def del_sc_only(self):
        del self._scOnly

    def get_config_sc(self):
        return self._configSC

    def get_config_ac(self):
        return self._configAC

    def get_config_pb(self):
        return self._configPB

    def set_config_sc(self, value):
        self._configSC = value

    def set_config_ac(self, value):
        self._configAC = value

    def set_config_pb(self, value):
        self._configPB = value

    def del_config_sc(self):
        del self._configSC

    def del_config_ac(self):
        del self._configAC

    def del_config_pb(self):
        del self._configPB

    def get_dem_directory(self):
        return self._demDirectory

    def get_dem_reference(self):
        return self._demReference

    def get_snow_map_reference(self):
        return self._snowMapReference


    def get_esacci_wb_map_reference(self):
        return self._esacciWaterBodiesReference


    def get_esacci_lccs_map_reference(self):
        return self._esacciLandCoverReference


    def get_esacci_snowc_map_reference_directory(self):
        return self._esacciSnowConditionReferenceDir


    def set_dem_directory(self, value):
        self._demDirectory = value

    def set_dem_reference(self, value):
        self._demReference = value

    def set_snow_map_reference(self, value):
        self._snowMapReference = value


    def set_esacci_wb_map_reference(self, value):
        self._esacciWaterBodiesReference = value


    def set_esacci_lccs_map_reference(self, value):
        self._esacciLandCoverReference = value


    def set_esacci_snowc_map_reference_directory(self, value):
        self._esacciSnowConditionReferenceDir = value


    def del_dem_directory(self):
        del self._demDirectory

    def del_dem_reference(self):
        del self._demReference

    def del_snow_map_reference(self):
        del self._snowMapReference


    def del_esacci_wb_map_reference(self):
        del self._esacciWaterBodiesReference


    def del_esacci_lccs_map_reference(self):
        del self._esacciLandCoverReference


    def del_esacci_snowc_map_reference_directory(self):
        del self._esacciSnowConditionReferenceDir


    def get_median_filter(self):
        return self._medianFilter

    def set_median_filter(self, value):
        self._medianFilter = value

    def del_median_filter(self):
        del self._medianFilter

    def get_t_sdw(self):
        return self._T_SDW

    def get_t_b_02_b_12(self):
        return self._T_B02_B12

    def set_t_sdw(self, value):
        self._T_SDW = value

    def set_t_b_02_b_12(self, value):
        self._T_B02_B12 = value

    def del_t_sdw(self):
        del self._T_SDW

    def del_t_b_02_b_12(self):
        del self._T_B02_B12

    def get_no_data(self):
        return self._noData

    def get_icirrus(self):
        return self._icirrus

    def get_saturated_defective(self):
        return self._saturatedDefective

    def get_dark_features(self):
        return self._darkFeatures

    def get_cloud_shadows(self):
        return self._cloudShadows

    def get_vegetation(self):
        return self._vegetation

    def get_bare_soils(self):
        return self._bareSoils

    def get_water(self):
        return self._water

    def get_low_proba_clouds(self):
        return self._lowProbaClouds

    def get_med_proba_clouds(self):
        return self._medProbaClouds

    def get_high_proba_clouds(self):
        return self._highProbaClouds

    def get_thin_cirrus(self):
        return self._thinCirrus

    def get_snow_ice(self):
        return self._snowIce

    def set_no_data(self, value):
        self._noData = value

    def set_icirrus(self, value):
        self._icirrus = value

    def set_saturated_defective(self, value):
        self._saturatedDefective = value

    def set_dark_features(self, value):
        self._darkFeatures = value

    def set_cloud_shadows(self, value):
        self._cloudShadows = value

    def set_vegetation(self, value):
        self._vegetation = value

    def set_bare_soils(self, value):
        self._bareSoils = value

    def set_water(self, value):
        self._water = value

    def set_low_proba_clouds(self, value):
        self._lowProbaClouds = value

    def set_med_proba_clouds(self, value):
        self._medProbaClouds = value

    def set_high_proba_clouds(self, value):
        self._highProbaClouds = value

    def set_thin_cirrus(self, value):
        self._thinCirrus = value

    def set_snow_ice(self, value):
        self._snowIce = value

    def del_no_data(self):
        del self._no_data

    def del_icirrus(self):
        del self._icirrus

    def del_saturated_defective(self):
        del self._saturatedDefective

    def del_dark_features(self):
        del self._darkFeatures

    def del_cloud_shadows(self):
        del self._cloudShadows

    def del_vegetation(self):
        del self._vegetation

    def del_bare_soils(self):
        del self._bareSoils

    def del_water(self):
        del self._water

    def del_low_proba_clouds(self):
        del self._lowProbaClouds

    def del_med_proba_clouds(self):
        del self._medProbaClouds

    def del_high_proba_clouds(self):
        del self._highProbaClouds

    def del_thin_cirrus(self):
        del self._thinCirrus

    def del_snow_ice(self):
        del self._snowIce

    def get_t_21_b_12(self):
        return self._T21_B12

    def get_t_22_b_12(self):
        return self._T22_B12

    def get_t_21_r_b_02_b_11(self):
        return self._T21_R_B02_B11

    def get_t_22_r_b_02_b_11(self):
        return self._T22_R_B02_B11

    def set_t_21_b_12(self, value):
        self._T21_B12 = value

    def set_t_22_b_12(self, value):
        self._T22_B12 = value

    def set_t_21_r_b_02_b_11(self, value):
        self._T21_R_B02_B11 = value

    def set_t_22_r_b_02_b_11(self, value):
        self._T22_R_B02_B11 = value

    def del_t_21_b_12(self):
        del self._T21_B12

    def del_t_22_b_12(self):
        del self._T22_B12

    def del_t_21_r_b_02_b_11(self):
        del self._T21_R_B02_B11

    def del_t_22_r_b_02_b_11(self):
        del self._T22_R_B02_B11

    def get_t_11_b_02(self):
        return self._T11_B02

    def get_t_12_b_02(self):
        return self._T12_B02

    def get_t_11_r_b_02_b_11(self):
        return self._T11_R_B02_B11

    def get_t_12_r_b_02_b_11(self):
        return self._T12_R_B02_B11

    def set_t_11_b_02(self, value):
        self._T11_B02 = value

    def set_t_12_b_02(self, value):
        self._T12_B02 = value

    def set_t_11_r_b_02_b_11(self, value):
        self._T11_R_B02_B11 = value

    def set_t_12_r_b_02_b_11(self, value):
        self._T12_R_B02_B11 = value

    def del_t_11_b_02(self):
        del self._T11_B02

    def del_t_12_b_02(self):
        del self._T12_B02

    def del_t_11_r_b_02_b_11(self):
        del self._T11_R_B02_B11

    def del_t_12_r_b_02_b_11(self):
        del self._T12_R_B02_B11

    def get_t_1_ndvi(self):
        return self._T1_NDVI

    def get_t_2_ndvi(self):
        return self._T2_NDVI

    def set_t_1_ndvi(self, value):
        self._T1_NDVI = value

    def set_t_2_ndvi(self, value):
        self._T2_NDVI = value

    def del_t_1_ndvi(self):
        del self._T1_NDVI

    def del_t_2_ndvi(self):
        del self._T2_NDVI

    def get_t_1_snow(self):
        return self._T1_SNOW

    def get_t_2_snow(self):
        return self._T2_SNOW

    def set_t_1_snow(self, value):
        self._T1_SNOW = value

    def set_t_2_snow(self, value):
        self._T2_SNOW = value

    def del_t_1_snow(self):
        del self._T1_SNOW

    def del_t_2_snow(self):
        del self._T2_SNOW

    def get_t_1_r_b_02_b_04(self):
        return self._T1_R_B02_B04

    def get_t_2_r_b_02_b_04(self):
        return self._T2_R_B02_B04

    def get_t_1_r_b_8_a_b_03(self):
        return self._T1_R_B8A_B03

    def get_t_2_r_b_8_a_b_03(self):
        return self._T2_R_B8A_B03

    def get_t_1_r_b_8_a_b_11(self):
        return self._T1_R_B8A_B11

    def get_t_2_r_b_8_a_b_11(self):
        return self._T2_R_B8A_B11

    def set_t_1_r_b_02_b_04(self, value):
        self._T1_R_B02_B04 = value

    def set_t_2_r_b_02_b_04(self, value):
        self._T2_R_B02_B04 = value

    def set_t_1_r_b_8_a_b_03(self, value):
        self._T1_R_B8A_B03 = value

    def set_t_2_r_b_8_a_b_03(self, value):
        self._T2_R_B8A_B03 = value

    def set_t_1_r_b_8_a_b_11(self, value):
        self._T1_R_B8A_B11 = value

    def set_t_2_r_b_8_a_b_11(self, value):
        self._T2_R_B8A_B11 = value

    def del_t_1_r_b_02_b_04(self):
        del self._T1_R_B02_B04

    def del_t_2_r_b_02_b_04(self):
        del self._T2_R_B02_B04

    def del_t_1_r_b_8_a_b_03(self):
        del self._T1_R_B8A_B03

    def del_t_2_r_b_8_a_b_03(self):
        del self._T2_R_B8A_B03

    def del_t_1_r_b_8_a_b_11(self):
        del self._T1_R_B8A_B11

    def del_t_2_r_b_8_a_b_11(self):
        del self._T2_R_B8A_B11

    def get_t_1_b_02(self):
        return self._T1_B02

    def get_t_2_b_02(self):
        return self._T2_B02

    def get_t_1_b_8_a(self):
        return self._T1_B8A

    def get_t_2_b_8_a(self):
        return self._T2_B8A

    def get_t_1_b_10(self):
        return self._T1_B10

    def get_t_2_b_10(self):
        return self._T2_B10

    def get_t_1_b_12(self):
        return self._T1_B12

    def get_t_2_b_12(self):
        return self._T2_B12

    def set_t_1_b_02(self, value):
        self._T1_B02 = value

    def set_t_2_b_02(self, value):
        self._T2_B02 = value

    def set_t_1_b_8_a(self, value):
        self._T1_B8A = value

    def set_t_2_b_8_a(self, value):
        self._T2_B8A = value

    def set_t_1_b_10(self, value):
        self._T1_B10 = value

    def set_t_2_b_10(self, value):
        self._T2_B10 = value

    def set_t_1_b_12(self, value):
        self._T1_B12 = value

    def set_t_2_b_12(self, value):
        self._T2_B12 = value

    def del_t_1_b_02(self):
        del self._T1_B02

    def del_t_2_b_02(self):
        del self._T2_B02

    def del_t_1_b_8_a(self):
        del self._T1_B8A

    def del_t_2_b_8_a(self):
        del self._T2_B8A

    def del_t_1_b_10(self):
        del self._T1_B10

    def del_t_2_b_10(self):
        del self._T2_B10

    def del_t_1_b_12(self):
        del self._T1_B12

    def del_t_2_b_12(self):
        del self._T2_B12

    def get_t_1_ndsi_snw(self):
        return self._T1_NDSI_SNW

    def get_t_2_ndsi_snw(self):
        return self._T2_NDSI_SNW

    def set_t_1_ndsi_snw(self, value):
        self._T1_NDSI_SNW = value

    def set_t_2_ndsi_snw(self, value):
        self._T2_NDSI_SNW = value

    def del_t_1_ndsi_snw(self):
        del self._T1_NDSI_SNW

    def del_t_2_ndsi_snw(self):
        del self._T2_NDSI_SNW

    def get_t_1_ndsi_cld(self):
        return self._T1_NDSI_CLD

    def get_t_2_ndsi_cld(self):
        return self._T2_NDSI_CLD

    def set_t_1_ndsi_cld(self, value):
        self._T1_NDSI_CLD = value

    def set_t_2_ndsi_cld(self, value):
        self._T2_NDSI_CLD = value

    def del_t_1_ndsi_cld(self):
        del self._T1_NDSI_CLD

    def del_t_2_ndsi_cld(self):
        del self._T2_NDSI_CLD

    def get_t_1_b_04(self):
        return self._T1_B04

    def get_t_2_b_04(self):
        return self._T2_B04

    def set_t_1_b_04(self, value):
        self._T1_B04 = value

    def set_t_2_b_04(self, value):
        self._T2_B04 = value

    def del_t_1_b_04(self):
        del self._T1_B04

    def del_t_2_b_04(self):
        del self._T2_B04

    def get_l_2_a_wvp_quantification_value(self):
        return self._L2A_WVP_QUANTIFICATION_VALUE

    def set_l_2_a_wvp_quantification_value(self, value):
        self._L2A_WVP_QUANTIFICATION_VALUE = value

    def del_l_2_a_wvp_quantification_value(self):
        del self._L2A_WVP_QUANTIFICATION_VALUE

    def get_product_version(self):
        return self._productVersion

    def set_product_version(self, value):
        self._productVersion = value

    def del_product_version(self):
        del self._productVersion

    def get_spacecraft_name(self):
        return self._spacecraftName

    def set_spacecraft_name(self, value):
        self._spacecraftName = value

    def del_spacecraft_name(self):
        del self._spacecraftName

    def get_l1c_up_mtd_xml(self):
        return self._L1C_UP_MTD_XML

    def get_l1c_ds_mtd_xml(self):
        return self._L1C_DS_MTD_XML

    def get_l1c_up_id(self):
        return self._L1C_UP_ID

    def get_l1c_ds_id(self):
        return self._L1C_DS_ID

    def get_l1c_tile_id(self):
        return self._L1C_TILE_ID

    def set_l1c_up_mtd_xml(self, value):
        self._L1C_UP_MTD_XML = value

    def set_l1c_ds_mtd_xml(self, value):
        self._L1C_DS_MTD_XML = value

    def set_l1c_up_id(self, value):
        self._L1C_UP_ID = value

    def set_l1c_ds_id(self, value):
        self._L1C_DS_ID = value

    def set_l1c_tile_id(self, value):
        self._L1C_TILE_ID = value

    def del_l1c_up_mtd_xml(self):
        del self._L1C_UP_MTD_XML

    def del_l1c_ds_mtd_xml(self):
        del self._L1C_DS_MTD_XML

    def del_l1c_up_id(self):
        del self._L1C_UP_ID

    def del_l1c_ds_id(self):
        del self._L1C_DS_ID

    def del_l1c_tile_id(self):
        del self._L1C_TILE_ID

    def get_creation_date(self):
        return self._creationDate

    def set_creation_date(self, value):
        self._creationDate = value

    def del_creation_date(self):
        del self._creationDate

    def get_wvlsen(self):
        return self._wvlsen

    def get_fwhm(self):
        return self._fwhm

    def set_wvlsen(self, value):
        self._wvlsen = value

    def set_fwhm(self, value):
        self._fwhm = value

    def del_wvlsen(self):
        del self._wvlsen

    def del_fwhm(self):
        del self._fwhm

    def get_l2a_boa_quantification_value(self):
        return self._L2A_BOA_QUANTIFICATION_VALUE

    def get_l2a_wvp_quantification_value(self):
        return self._L2A_WVP_QUANTIFICATION_VALUE

    def get_l2a_aot_quantification_value(self):
        return self._L2A_AOT_QUANTIFICATION_VALUE

    def set_l2a_boa_quantification_value(self, value):
        self._L2A_BOA_QUANTIFICATION_VALUE = value

    def set_l2a_wvp_quantification_value(self, value):
        self._L2A_WVP_QUANTIFICATION_VALUE = value

    def set_l2a_aot_quantification_value(self, value):
        self._L2A_AOT_QUANTIFICATION_VALUE = value

    def del_l2a_boa_quantification_value(self):
        del self._L2A_BOA_QUANTIFICATION_VALUE

    def del_l2a_wvp_quantification_value(self):
        del self._L2A_WVP_QUANTIFICATION_VALUE

    def del_l2a_aot_quantification_value(self):
        del self._L2A_AOT_QUANTIFICATION_VALUE

    def get_l2a_up_dir(self):
        return self._L2A_UP_DIR

    def set_l2a_up_dir(self, value):
        self._L2A_UP_DIR = value

    def del_l2a_up_dir(self):
        del self._L2A_UP_DIR

    def get_l2a_up_id(self):
        return self._L2A_UP_ID

    def set_l2a_up_id(self, value):
        self._L2A_UP_ID = value

    def del_l2a_up_id(self):
        del self._L2A_UP_ID

    def get_l2a_tile_id(self):
        return self._L2A_TILE_ID

    def get_l1c_tile_mtd_xml(self):
        return self._L1C_TILE_MTD_XML

    def get_l2a_inspire_xml(self):
        return self._L2A_INSPIRE_XML

    def get_l2a_manifest_safe(self):
        return self._L2A_MANIFEST_SAFE

    def get_l2a_up_mtd_xml(self):
        return self._L2A_UP_MTD_XML

    def get_l2a_tile_mtd_xml(self):
        return self._L2A_TILE_MTD_XML

    def set_l1c_tile_mtd_xml(self, value):
        self._L1C_TILE_MTD_XML = value

    def set_l2a_tile_id(self, value):
        self._L2A_TILE_ID = value

    def set_l2a_inspire_xml(self, value):
        self._L2A_INSPIRE_XML = value

    def set_l2a_manifest_safe(self, value):
        self._L2A_MANIFEST_SAFE = value

    def set_l2a_up_mtd_xml(self, value):
        self._L2A_UP_MTD_XML = value

    def set_l2a_tile_mtd_xml(self, value):
        self._L2A_TILE_MTD_XML = value

    def del_l2a_ds_id(self):
        del self._L2A_DS_ID

    def del_l2a_tile_id(self):
        del self._L2A_TILE_ID

    def del_l1c_tile_mtd_xml(self):
        del self._L1C_TILE_MTD_XML

    def del_l2a_inspire_xml(self):
        del self._L2A_INSPIRE_XML

    def del_l2a_manifest_safe(self):
        del self._L2A_MANIFEST_SAFE

    def del_l2a_up_mtd_xml(self):
        del self._L2A_UP_MTD_XML

    def del_l2a_ds_mtd_xml(self):
        del self._L2A_DS_MTD_XML

    def del_l2a_tile_mtd_xml(self):
        del self._L2A_TILE_MTD_XML

    def get_entity_id(self):
        return self._entityId

    def get_acquisition_date(self):
        return self._acquisitionDate

    def get_orbit_path(self):
        return self._orbitPath

    def get_orbit_row(self):
        return self._orbitRow

    def get_target_path(self):
        return self._targetPath

    def get_target_row(self):
        return self._targetRow

    def get_station_sgs(self):
        return self._stationSgs

    def get_scene_start_time(self):
        return self._sceneStartTime

    def get_scene_stop_time(self):
        return self._sceneStopTime

    def set_entity_id(self, value):
        self._entityId = value

    def set_acquisition_date(self, value):
        self._acquisitionDate = value

    def set_orbit_path(self, value):
        self._orbitPath = value

    def set_orbit_row(self, value):
        self._orbitRow = value

    def set_target_path(self, value):
        self._targetPath = value

    def set_target_row(self, value):
        self._targetRow = value

    def set_station_sgs(self, value):
        self._stationSgs = value

    def set_scene_start_time(self, value):
        self._sceneStartTime = value

    def set_scene_stop_time(self, value):
        self._sceneStopTime = value

    def del_entity_id(self):
        del self._entityId

    def del_acquisition_date(self):
        del self._acquisitionDate

    def del_orbit_path(self):
        del self._orbitPath

    def del_orbit_row(self):
        del self._orbitRow

    def del_target_path(self):
        del self._targetPath

    def del_target_row(self):
        del self._targetRow

    def del_station_sgs(self):
        del self._stationSgs

    def del_scene_start_time(self):
        del self._sceneStartTime

    def del_scene_stop_time(self):
        del self._sceneStopTime

    def get_dn_scale(self):
        return self._dnScale

    def set_dn_scale(self, value):
        self._dnScale = value

    def del_dn_scale(self):
        del self._dnScale

    def get_d_2(self):
        return self._d2

    def get_c_0(self):
        return self._c0

    def get_c_1(self):
        return self._c1

    def get_e_0(self):
        return self._e0

    def set_d_2(self, value):
        self._d2 = value

    def set_c_0(self, value):
        self._c0 = value

    def set_c_1(self, value):
        self._c1 = value

    def set_e_0(self, value):
        self._e0 = value

    def del_d_2(self):
        del self._d2

    def del_c_0(self):
        del self._c0

    def del_c_1(self):
        del self._c1

    def del_e_0(self):
        del self._e0

    def get_processor_name(self):
        return self._processorName

    def get_processor_version(self):
        return self._processorVersion

    def get_processing_baseline(self):
        return self._processingBaseline

    def get_processor_date(self):
        return self._processorDate

    def set_processor_name(self, value):
        self._processorName = value

    def set_processor_version(self, value):
        self._processorVersion = value

    def set_processing_baseline(self, value):
        self._processingBaseline = value

    def set_processor_date(self, value):
        self._processorDate = value

    def del_processor_name(self):
        del self._processorName

    def del_processor_version(self):
        del self._processorVersion

    def del_processing_baseline(self):
        del self._processingBaseline

    def del_processor_date(self):
        del self._processorDate

    def get_ncols(self):
        return self._ncols

    def get_nrows(self):
        return self._nrows

    def get_nbnds(self):
        return self._nbnds

    def get_zenith_angle(self):
        return self._sza

    def get_azimuth_angle(self):
        return self._saa

    def get_gipp(self):
        return self._GIPP

    def get_ecmwf(self):
        return self._ECMWF

    def set_ncols(self, value):
        self._ncols = value

    def set_nrows(self, value):
        self._nrows = value

    def set_nbnds(self, value):
        self._nbnds = value

    def set_zenith_angle(self, value):
        self._sza = value

    def set_azimuth_angle(self, value):
        self._saa = value

    def set_gipp(self, value):
        self._GIPP = value

    def set_ecmwf(self, value):
        self._ECMWF = value

    def del_ncols(self):
        del self._ncols

    def del_nrows(self):
        del self._nrows

    def del_nbnds(self):
        del self._nbnds

    def del_zenith_angle(self):
        del self._sza

    def del_azimuth_angle(self):
        del self._saa

    def del_gipp(self):
        del self._GIPP

    def del_ecmwf(self):
        del self._ECMWF

    def get_home(self):
        return self._home

    def get_data_dir(self):
        return self._dataDir

    def get_config_dir(self):
        return self._configDir

    def get_bin_dir(self):
        return self._binDir

    def get_lib_dir(self):
        return self._libDir

    def get_aux_dir(self):
        return self._auxDir

    def get_log_dir(self):
        return self._logDir

    def get_config_fn(self):
        return self._configFn

    def get_input_fn(self):
        return self._inputFn

    def get_aot_fn(self):
        return self._aotFn

    def get_aspect_fn(self):
        return self._aspectFn

    def get_atm_data_fn(self):
        return self._atmDataFn

    def get_class_map_fn(self):
        return self._classMapFn

    def get_cloud_qi_map_fn(self):
        return self._cloudQiMapFn

    def get_ddv_fn(self):
        return self._ddvFn

    def get_hcw_fn(self):
        return self._hcwFn

    def get_ilumination_fn(self):
        return self._iluminationFn

    def get_sky_view_fn(self):
        return self._skyViewFn

    def get_slope_fn(self):
        return self._slopeFn

    def get_snow_qi_map_fn(self):
        return self._snowQiMapFn

    def get_vis_index_fn(self):
        return self._visIndexFn

    def get_water_vapor_fn(self):
        return self._waterVaporFn

    def get_adj_km(self):
        return self._adj_km

    def get_beta_thr(self):
        return self._beta_thr

    def get_ch_940(self):
        return self._ch940

    def get_cellsize(self):
        return self._cellsize

    def get_cloud_refl_thr_blu(self):
        return self._cloud_refl_thr_blu

    def get_dem_terrain_correction(self):
        return self._dem_terrain_correction

    def get_ibrdf(self):
        return self._ibrdf

    def get_ibrdf_dark(self):
        return self._ibrdf_dark

    def get_icl_shadow(self):
        return self._icl_shadow

    def get_iclshad_mask(self):
        return self._iclshad_mask

    def get_ihaze(self):
        return self._ihaze

    def get_ihcw(self):
        return self._ihcw

    def get_ihot_dynr(self):
        return self._ihot_dynr

    def get_ihot_mask(self):
        return self._ihot_mask

    def get_intpol_1400(self):
        return self._intpol1400

    def get_intpol_725_825(self):
        return self._intpol725_825

    def get_intpol_760(self):
        return self._intpol760

    def get_intpol_940_1130(self):
        return self._intpol940_1130

    def get_istretch_type(self):
        return self._istretch_type

    def get_iwat_shd(self):
        return self._iwat_shd

    def get_iwaterwv(self):
        return self._iwaterwv

    def get_iwv_watermask(self):
        return self._iwv_watermask

    def get_ksolflux(self):
        return self._ksolflux

    def get_altit(self):
        return self._altit

    def get_npref(self):
        return self._npref

    def get_phi_scl_min(self):
        return self._phi_scl_min

    def get_phi_unscl_max(self):
        return self._phi_unscl_max

    def get_pixelsize(self):
        return self._pixelsize

    def get_resolution(self):
        return self._resolution

    def get_ratio_blu_red(self):
        return self._ratio_blu_red

    def get_ratio_red_swir(self):
        return self._ratio_red_swir

    def get_refl_cutoff(self):
        return self._refl_cutoff

    def get_rel_saturation(self):
        return self._rel_saturation

    def get_thr_shad(self):
        return self._thr_shad

    def get_thv(self):
        return self._thv

    def get_phiv(self):
        return self._phiv

    def get_smooth_wvmap(self):
        return self._smooth_wvmap

    def get_solaz(self):
        return self._solaz

    def get_solaz_arr(self):
        return self._solaz_arr

    def get_solze(self):
        return self._solze

    def get_solze_arr(self):
        return self._solze_arr

    def get_thr_g(self):
        return self._thr_g

    def get_vaa_arr(self):
        return self._vaa_arr

    def get_visibility(self):
        return self._visibility

    def get_vza_arr(self):
        return self._vza_arr

    def get_water_refl_thr_nir(self):
        return self._water_refl_thr_nir

    def get_water_refl_thr_swir_1(self):
        return self._water_refl_thr_swir1

    def get_wl_1130a(self):
        return self._wl1130a

    def get_wl_1400a(self):
        return self._wl1400a

    def get_wl_1900a(self):
        return self._wl1900a

    def get_wl_940a(self):
        return self._wl940a

    def get_wv_thr_cirrus(self):
        return self._wv_thr_cirrus
    
    def get_pic_fn(self):
        return self._pic_fn

    def set_home(self, value):
        self._home = value

    def set_data_dir(self, value):
        self._dataDir = value

    def set_config_dir(self, value):
        self._configDir = value

    def set_bin_dir(self, value):
        self._binDir = value

    def set_lib_dir(self, value):
        self._libDir = value

    def set_aux_dir(self, value):
        self._auxDir = value

    def set_log_dir(self, value):
        self._logDir = value

    def set_config_fn(self, value):
        self._configFn = value

    def set_input_fn(self, value):
        self._inputFn = os.path.join(self._dataDir, value)

    def set_aot_fn(self, value):
        self._aotFn = os.path.join(self._dataDir, value)

    def set_aspect_fn(self, value):
        self._aspectFn = os.path.join(self._dataDir, value)

    def set_atm_data_fn(self, value):
        self._atmDataFn = value

    def set_class_map_fn(self, value):
        self._classMapFn = os.path.join(self._dataDir, value)

    def set_cloud_qi_map_fn(self, value):
        self._cloudQiMapFn = os.path.join(self._dataDir, value)

    def set_ddv_fn(self, value):
        self._ddvFn = os.path.join(self._dataDir, value)

    def set_hcw_fn(self, value):
        self._hcwFn = os.path.join(self._dataDir, value)

    def set_ilumination_fn(self, value):
        self._iluminationFn = os.path.join(self._dataDir, value)

    def set_sky_view_fn(self, value):
        self._skyViewFn = os.path.join(self._dataDir, value)

    def set_slope_fn(self, value):
        self._slopeFn = os.path.join(self._dataDir, value)

    def set_snow_qi_map_fn(self, value):
        self._snowQiMapFn = os.path.join(self._dataDir, value)

    def set_vis_index_fn(self, value):
        self._visIndexFn = os.path.join(self._dataDir, value)

    def set_water_vapor_fn(self, value):
        self._waterVaporFn = os.path.join(self._dataDir, value)

    def set_adj_km(self, value):
        self._adj_km = value

    def set_beta_thr(self, value):
        self._beta_thr = value

    def set_ch_940(self, value):
        self._ch940 = value

    def set_cellsize(self, value):
        self._cellsize = value

    def set_cloud_refl_thr_blu(self, value):
        self._cloud_refl_thr_blu = value

    def set_date(self, value):
        self._date = value

    def set_dem_terrain_correction(self, value):
        self._dem_terrain_correction = value

    def set_ibrdf(self, value):
        self._ibrdf = value

    def set_ibrdf_dark(self, value):
        self._ibrdf_dark = value

    def set_icl_shadow(self, value):
        self._icl_shadow = value

    def set_iclshad_mask(self, value):
        self._iclshad_mask = value

    def set_ihaze(self, value):
        self._ihaze = value

    def set_ihcw(self, value):
        self._ihcw = value

    def set_ihot_dynr(self, value):
        self._ihot_dynr = value

    def set_ihot_mask(self, value):
        self._ihot_mask = value

    def set_intpol_1400(self, value):
        self._intpol1400 = value

    def set_intpol_725_825(self, value):
        self._intpol725_825 = value

    def set_intpol_760(self, value):
        self._intpol760 = value

    def set_intpol_940_1130(self, value):
        self._intpol940_1130 = value

    def set_istretch_type(self, value):
        self._istretch_type = value

    def set_iwat_shd(self, value):
        self._iwat_shd = value

    def set_iwaterwv(self, value):
        self._iwaterwv = value

    def set_iwv_watermask(self, value):
        self._iwv_watermask = value

    def set_ksolflux(self, value):
        self._ksolflux = value

    def set_altit(self, value):
        self._altit = value

    def set_npref(self, value):
        self._npref = value

    def set_phi_scl_min(self, value):
        self._phi_scl_min = value

    def set_phi_unscl_max(self, value):
        self._phi_unscl_max = value

    def set_pixelsize(self, value):
        self._pixelsize = value

    def set_resolution(self, value):
        self._resolution = value
        self._pixelsize = value
        self._cellsize = value

    def set_ratio_blu_red(self, value):
        self._ratio_blu_red = value

    def set_ratio_red_swir(self, value):
        self._ratio_red_swir = value

    def set_refl_cutoff(self, value):
        self._refl_cutoff = value

    def set_rel_saturation(self, value):
        self._rel_saturation = value

    def set_thr_shad(self, value):
        self._thr_shad = value

    def set_thv(self, value):
        self._thv = value

    def set_phiv(self, value):
        self._phiv = value

    def set_smooth_wvmap(self, value):
        self._smooth_wvmap = value

    def set_solaz(self, value):
        self._solaz = value

    def set_solaz_arr(self, value):
        self._solaz_arr = value

    def set_solze(self, value):
        self._solze = value

    def set_solze_arr(self, value):
        self._solze_arr = value

    def set_thr_clear_water(self, value):
        self._thr_clear_water = value

    def set_thr_haze_water(self, value):
        self._thr_haze_water = value

    def set_thr_g(self, value):
        self._thr_g = value

    def set_vaa_arr(self, value):
        self._vaa_arr = value

    def set_visibility(self, value):
        self._visibility = value

    def set_vza_arr(self, value):
        self._vza_arr = value

    def set_water_refl_thr_nir(self, value):
        self._water_refl_thr_nir = value

    def set_water_refl_thr_swir_1(self, value):
        self._water_refl_thr_swir1 = value

    def set_wl_1130a(self, value):
        self._wl1130a = value

    def set_wl_1400a(self, value):
        self._wl1400a = value

    def set_wl_1900a(self, value):
        self._wl1900a = value

    def set_wl_940a(self, value):
        self._wl940a = value

    def set_wv_thr_cirrus(self, value):
        self._wv_thr_cirrus = value
    
    def set_pic_fn(self,value):
        self._pic_fn = value

    def del_home(self):
        del self._home

    def del_data_dir(self):
        del self._dataDir

    def del_config_dir(self):
        del self._configDir

    def del_bin_dir(self):
        del self._binDir

    def del_lib_dir(self):
        del self._libDir

    def del_aux_dir(self):
        del self._auxDir

    def del_log_dir(self):
        del self._logDir

    def del_config_fn(self):
        del self._configFn

    def del_input_fn(self):
        del self._inputFn

    def del_aot_fn(self):
        del self._aotFn

    def del_aspect_fn(self):
        del self._aspectFn

    def del_atm_data_fn(self):
        del self._atmDataFn

    def del_class_map_fn(self):
        del self._classMapFn

    def del_cloud_qi_map_fn(self):
        del self._cloudQiMapFn

    def del_ddv_fn(self):
        del self._ddvFn

    def del_hcw_fn(self):
        del self._hcwFn

    def del_ilumination_fn(self):
        del self._iluminationFn

    def del_sky_view_fn(self):
        del self._skyViewFn

    def del_slope_fn(self):
        del self._slopeFn

    def del_snow_qi_map_fn(self):
        del self._snowQiMapFn

    def del_vis_index_fn(self):
        del self._visIndexFn

    def del_water_vapor_fn(self):
        del self._waterVaporFn

    def del_adj_km(self):
        del self._adj_km

    def del_beta_thr(self):
        del self._beta_thr

    def del_ch_940(self):
        del self._ch940

    def del_cellsize(self):
        del self._cellsize

    def del_cloud_refl_thr_blu(self):
        del self._cloud_refl_thr_blu

    def del_date(self):
        del self._date

    def del_dem_terrain_correction(self):
        del self._dem_terrain_correction

    def del_ibrdf(self):
        del self._ibrdf

    def del_ibrdf_dark(self):
        del self._ibrdf_dark

    def del_icl_shadow(self):
        del self._icl_shadow

    def del_iclshad_mask(self):
        del self._iclshad_mask

    def del_ihaze(self):
        del self._ihaze

    def del_ihcw(self):
        del self._ihcw

    def del_ihot_dynr(self):
        del self._ihot_dynr

    def del_ihot_mask(self):
        del self._ihot_mask

    def del_intpol_1400(self):
        del self._intpol1400

    def del_intpol_725_825(self):
        del self._intpol725_825

    def del_intpol_760(self):
        del self._intpol760

    def del_intpol_940_1130(self):
        del self._intpol940_1130

    def del_istretch_type(self):
        del self._istretch_type

    def del_iwat_shd(self):
        del self._iwat_shd

    def del_iwaterwv(self):
        del self._iwaterwv

    def del_iwv_watermask(self):
        del self._iwv_watermask

    def del_ksolflux(self):
        del self._ksolflux

    def del_altit(self):
        del self._altit

    def del_npref(self):
        del self._npref

    def del_phi_scl_min(self):
        del self._phi_scl_min

    def del_phi_unscl_max(self):
        del self._phi_unscl_max

    def del_pixelsize(self):
        del self._pixelsize

    def del_resolution(self):
        del self._resolution

    def del_ratio_blu_red(self):
        del self._ratio_blu_red

    def del_ratio_red_swir(self):
        del self._ratio_red_swir

    def del_refl_cutoff(self):
        del self._refl_cutoff

    def del_rel_saturation(self):
        del self._rel_saturation

    def del_thr_shad(self):
        del self._thr_shad

    def del_thv(self):
        del self._thv

    def del_phiv(self):
        del self._phiv

    def del_smooth_wvmap(self):
        del self._smooth_wvmap

    def del_solaz(self):
        del self._solaz

    def del_solaz_arr(self):
        del self._solaz_arr

    def del_solze(self):
        del self._solze

    def del_solze_arr(self):
        del self._solze_arr

    def del_thr_clear_water(self):
        del self._thr_clear_water

    def del_thr_haze_water(self):
        del self._thr_haze_water

    def del_thr_g(self):
        del self._thr_g

    def del_vaa_arr(self):
        del self._vaa_arr

    def del_visibility(self):
        del self._visibility

    def del_vza_arr(self):
        del self._vza_arr

    def del_water_refl_thr_nir(self):
        del self._water_refl_thr_nir

    def del_water_refl_thr_swir_1(self):
        del self._water_refl_thr_swir1

    def del_wl_1130a(self):
        del self._wl1130a

    def del_wl_1400a(self):
        del self._wl1400a

    def del_wl_1900a(self):
        del self._wl1900a

    def del_wl_940a(self):
        del self._wl940a

    def del_wv_thr_cirrus(self):
        del self._wv_thr_cirrus
    
    def del_pic_fn(self):
        del self._pic_fn

    def set_l2a_ds_id(self, value):
        self._L2A_DS_ID = value
        if value in self._L2A_DS_LST:
            return
        self._L2A_DS_LST.append(value)
        return

    def get_l2a_ds_id(self):
        try:
            tileId = self._L2A_TILE_ID[25:40]
            for ref in self._L2A_DS_LST:
                if tileId in ref:
                    return ref
        except:
            return self._L2A_DS_ID

    def set_l2a_ds_mtd_xml(self, value):
        self._L2A_DS_MTD_XML = value
        if value in self._L2A_DS_MTD_LST:
            return
        self._L2A_DS_MTD_LST.append(value)
        return

    def get_l2a_ds_mtd_xml(self):
        return self._L2A_DS_MTD_XML
        try:
            tileId = self._L2A_TILE_ID[25:40]
            for ref in self._L2A_DS_MTD_LST:
                if tileId in ref:
                    return ref
        except:
            return self._L2A_DS_MTD_XML

    def get_UP_INDEX_HTML(self):
        return self._UP_INDEX_HTML

    def set_UP_INDEX_HTML(self, value):
        self._UP_INDEX_HTML = value

    def del_UP_INDEX_HTML(self):
        del self._UP_INDEX_HTML

    def get_INSPIRE_XML(self):
        return self._INSPIRE_XML

    def set_INSPIRE_XML(self, value):
        self._INSPIRE_XML = value

    def del_INSPIRE_XML(self):
        del self._INSPIRE_XML

    # Properties:
    processorName = property(get_processor_name, set_processor_name, del_processor_name, "processorName's docstring")
    processorVersion = property(get_processor_version, set_processor_version, del_processor_version,
                                "processorVersion's docstring")
    processingBaseline = property(get_processing_baseline, set_processing_baseline, del_processing_baseline,
                                "processingBaseline's docstring")
    processorDate = property(get_processor_date, set_processor_date, del_processor_date, "processorDate's docstring")
    home = property(get_home, set_home, del_home, "home's docstring")
    processing_centre = property(get_processing_centre, set_processing_centre, del_processing_centre, "processing_centre's docstring")
    archiving_centre = property(get_archiving_centre, set_archiving_centre, del_archiving_centre, "archiving_centre's docstring")
    datastrip = property(get_datastrip, set_datastrip, del_datastrip, "datastrip's docstring")
    datastrip_root_folder = property(get_datastrip_root_folder, set_datastrip_root_folder, del_datastrip_root_folder, "datastrip_root_folder's docstring")
    UP_INDEX_HTML = property(get_UP_INDEX_HTML, set_UP_INDEX_HTML, del_UP_INDEX_HTML, "UP_INDEX_HTML's docstring")
    INSPIRE_XML = property(get_INSPIRE_XML, set_INSPIRE_XML, del_INSPIRE_XML, "INSPIRE_XML's docstring")
    tile = property(get_tile, set_tile, del_tile, "tile's docstring")
    raw = property(get_raw, set_raw, del_raw, "RAW mode's docstring")
    tif = property(get_tif, set_tif, del_tif, "RAW mode Tiff's docstring")
    geoboxPvi = property(get_geoboxPvi, set_geoboxPvi, del_geoboxPvi, "geobox PVI docstring")
    user_product = property(get_user_product, set_user_product, del_user_product, "user_product's docstring")
    input_dir = property(get_input_dir, set_input_dir, del_input_dir, "input_dir's docstring")
    work_dir = property(get_work_dir, set_work_dir, del_work_dir, "work_dir's docstring")
    img_database_dir = property(get_img_database_dir, set_img_database_dir, del_img_database_dir, "img_database_dir's docstring")
    res_database_dir = property(get_res_database_dir, set_res_database_dir, del_res_database_dir, "res_database_dir's docstring")
    output_dir = property(get_output_dir, set_output_dir, del_output_dir, "output_dir's docstring")
    dataDir = property(get_data_dir, set_data_dir, del_data_dir, "dataDir's docstring")
    configDir = property(get_config_dir, set_config_dir, del_config_dir, "configDir's docstring")
    binDir = property(get_bin_dir, set_bin_dir, del_bin_dir, "binDir's docstring")
    libDir = property(get_lib_dir, set_lib_dir, del_lib_dir, "libDir's docstring")
    auxDir = property(get_aux_dir, set_aux_dir, del_aux_dir, "auxDir's docstring")
    logDir = property(get_log_dir, set_log_dir, del_log_dir, "logDir's docstring")
    configFn = property(get_config_fn, set_config_fn, del_config_fn, "configFn's docstring")
    configSC = property(get_config_sc, set_config_sc, del_config_sc, "configSC's docstring")
    configAC = property(get_config_ac, set_config_ac, del_config_ac, "configAC's docstring")
    configPB = property(get_config_pb, set_config_pb, del_config_pb, "configPB's docstring")
    atmDataFn = property(get_atm_data_fn, set_atm_data_fn, del_atm_data_fn, "atmDataFn's docstring")
    adj_km = property(get_adj_km, set_adj_km, del_adj_km, "adj_km's docstring")
    beta_thr = property(get_beta_thr, set_beta_thr, del_beta_thr, "beta_thr's docstring")
    ch940 = property(get_ch_940, set_ch_940, del_ch_940, "ch940's docstring")
    cellsize = property(get_cellsize, set_cellsize, del_cellsize, "cellsize's docstring")
    cloud_refl_thr_blu = property(get_cloud_refl_thr_blu, set_cloud_refl_thr_blu, del_cloud_refl_thr_blu,
                                  "cloud_refl_thr_blu's docstring")
    dem_terrain_correction = property(get_dem_terrain_correction, set_dem_terrain_correction, del_dem_terrain_correction,
                                      "dem_terrain_correction's docstring")
    demOutput = property(get_dem_output, set_dem_output, del_dem_output, "dem_output's docstring")
    tciOutput = property(get_tci_output, set_tci_output, del_tci_output, "tci_output's docstring")
    ddvOutput = property(get_ddv_output, set_ddv_output, del_ddv_output, "ddv_output's docstring")
    downsample20to60 = property(get_downsample_20to60, set_downsample_20to60, del_downsample_20to60, "downsample_20to60's docstring")
    ibrdf = property(get_ibrdf, set_ibrdf, del_ibrdf, "ibrdf's docstring")
    thr_g = property(get_thr_g, set_thr_g, del_thr_g, "thr_g's docstring")
    ibrdf_dark = property(get_ibrdf_dark, set_ibrdf_dark, del_ibrdf_dark, "ibrdf_dark's docstring")
    icl_shadow = property(get_icl_shadow, set_icl_shadow, del_icl_shadow, "icl_shadow's docstring")
    iclshad_mask = property(get_iclshad_mask, set_iclshad_mask, del_iclshad_mask, "iclshad_mask's docstring")
    ihaze = property(get_ihaze, set_ihaze, del_ihaze, "ihaze's docstring")
    ihcw = property(get_ihcw, set_ihcw, del_ihcw, "ihcw's docstring")
    ihot_dynr = property(get_ihot_dynr, set_ihot_dynr, del_ihot_dynr, "ihot_dynr's docstring")
    ihot_mask = property(get_ihot_mask, set_ihot_mask, del_ihot_mask, "ihot_mask's docstring")
    intpol1400 = property(get_intpol_1400, set_intpol_1400, del_intpol_1400, "intpol1400's docstring")
    intpol725_825 = property(get_intpol_725_825, set_intpol_725_825, del_intpol_725_825, "intpol725_825's docstring")
    intpol760 = property(get_intpol_760, set_intpol_760, del_intpol_760, "intpol760's docstring")
    intpol940_1130 = property(get_intpol_940_1130, set_intpol_940_1130, del_intpol_940_1130,
                              "intpol940_1130's docstring")
    istretch_type = property(get_istretch_type, set_istretch_type, del_istretch_type, "istretch_type's docstring")
    iwat_shd = property(get_iwat_shd, set_iwat_shd, del_iwat_shd, "iwat_shd's docstring")
    iwaterwv = property(get_iwaterwv, set_iwaterwv, del_iwaterwv, "iwaterwv's docstring")
    iwv_watermask = property(get_iwv_watermask, set_iwv_watermask, del_iwv_watermask, "iwv_watermask's docstring")
    ksolflux = property(get_ksolflux, set_ksolflux, del_ksolflux, "ksolflux's docstring")
    altit = property(get_altit, set_altit, del_altit, "altit's docstring")
    npref = property(get_npref, set_npref, del_npref, "npref's docstring")
    phi_scl_min = property(get_phi_scl_min, set_phi_scl_min, del_phi_scl_min, "phi_scl_min's docstring")
    phi_unscl_max = property(get_phi_unscl_max, set_phi_unscl_max, del_phi_unscl_max, "phi_unscl_max's docstring")
    pixelsize = property(get_pixelsize, set_pixelsize, del_pixelsize, "pixelsize's docstring")
    resolution = property(get_resolution, set_resolution, del_resolution, "resolution's docstring")
    ratio_blu_red = property(get_ratio_blu_red, set_ratio_blu_red, del_ratio_blu_red, "ratio_blu_red's docstring")
    ratio_red_swir = property(get_ratio_red_swir, set_ratio_red_swir, del_ratio_red_swir, "ratio_red_swir's docstring")
    refl_cutoff = property(get_refl_cutoff, set_refl_cutoff, del_refl_cutoff, "refl_cutoff's docstring")
    rel_saturation = property(get_rel_saturation, set_rel_saturation, del_rel_saturation, "rel_saturation's docstring")
    thr_shad = property(get_thr_shad, set_thr_shad, del_thr_shad, "thr_shad's docstring")
    thv = property(get_thv, set_thv, del_thv, "thv's docstring")
    phiv = property(get_phiv, set_phiv, del_phiv, "phiv's docstring")
    smooth_wvmap = property(get_smooth_wvmap, set_smooth_wvmap, del_smooth_wvmap, "smooth_wvmap's docstring")
    solaz = property(get_solaz, set_solaz, del_solaz, "solaz's docstring")
    solaz_arr = property(get_solaz_arr, set_solaz_arr, del_solaz_arr, "solaz_arr's docstring")
    solze = property(get_solze, set_solze, del_solze, "solze's docstring")
    solze_arr = property(get_solze_arr, set_solze_arr, del_solze_arr, "solze_arr's docstring")
    vaa_arr = property(get_vaa_arr, set_vaa_arr, del_vaa_arr, "vaa_arr's docstring")
    visibility = property(get_visibility, set_visibility, del_visibility, "visibility's docstring")
    vza_arr = property(get_vza_arr, set_vza_arr, del_vza_arr, "vza_arr's docstring")
    water_refl_thr_nir = property(get_water_refl_thr_nir, set_water_refl_thr_nir, del_water_refl_thr_nir,
                                  "water_refl_thr_nir's docstring")
    water_refl_thr_swir1 = property(get_water_refl_thr_swir_1, set_water_refl_thr_swir_1, del_water_refl_thr_swir_1,
                                    "water_refl_thr_swir1's docstring")
    wl1130a = property(get_wl_1130a, set_wl_1130a, del_wl_1130a, "wl1130a's docstring")
    wl1400a = property(get_wl_1400a, set_wl_1400a, del_wl_1400a, "wl1400a's docstring")
    wl1900a = property(get_wl_1900a, set_wl_1900a, del_wl_1900a, "wl1900a's docstring")
    wl940a = property(get_wl_940a, set_wl_940a, del_wl_940a, "wl940a's docstring")
    wv_thr_cirrus = property(get_wv_thr_cirrus, set_wv_thr_cirrus, del_wv_thr_cirrus, "wv_thr_cirrus's docstring")
    ncols = property(get_ncols, set_ncols, del_ncols, "ncols's docstring")
    nrows = property(get_nrows, set_nrows, del_nrows, "nrows's docstring")
    nbnds = property(get_nbnds, set_nbnds, del_nbnds, "nbnds's docstring")
    zenith_angle = property(get_zenith_angle, set_zenith_angle, del_zenith_angle, "zenith_angle's docstring")
    azimuth_angle = property(get_azimuth_angle, set_azimuth_angle, del_azimuth_angle, "azimuth_angle's docstring")
    GIPP = property(get_gipp, set_gipp, del_gipp, "GIPP's docstring")
    ECMWF = property(get_ecmwf, set_ecmwf, del_ecmwf, "ECMWF's docstring")
    d2 = property(get_d_2, set_d_2, del_d_2, "d2's docstring")
    c0 = property(get_c_0, set_c_0, del_c_0, "c0's docstring")
    c1 = property(get_c_1, set_c_1, del_c_1, "c1's docstring")
    e0 = property(get_e_0, set_e_0, del_e_0, "e0's docstring")
    wvlsen = property(get_wvlsen, set_wvlsen, del_wvlsen, "wvlsen's docstring")
    fwhm = property(get_fwhm, set_fwhm, del_fwhm, "fwhm's docstring")
    dnScale = property(get_dn_scale, set_dn_scale, del_dn_scale, "dnScale's docstring")
    entityId = property(get_entity_id, set_entity_id, del_entity_id, "entityId's docstring")
    acquisitionDate = property(get_acquisition_date, set_acquisition_date, del_acquisition_date,
                               "acquisitionDate's docstring")
    orbitPath = property(get_orbit_path, set_orbit_path, del_orbit_path, "orbitPath's docstring")
    orbitRow = property(get_orbit_row, set_orbit_row, del_orbit_row, "orbitRow's docstring")
    targetPath = property(get_target_path, set_target_path, del_target_path, "targetPath's docstring")
    targetRow = property(get_target_row, set_target_row, del_target_row, "targetRow's docstring")
    stationSgs = property(get_station_sgs, set_station_sgs, del_station_sgs, "stationSgs's docstring")
    sceneStartTime = property(get_scene_start_time, set_scene_start_time, del_scene_start_time,
                              "sceneStartTime's docstring")
    sceneStopTime = property(get_scene_stop_time, set_scene_stop_time, del_scene_stop_time, "sceneStopTime's docstring")
    L2A_INSPIRE_XML = property(get_l2a_inspire_xml, set_l2a_inspire_xml, del_l2a_inspire_xml,
                               "L2A_INSPIRE_XML's docstring")
    L2A_MANIFEST_SAFE = property(get_l2a_manifest_safe, set_l2a_manifest_safe, del_l2a_manifest_safe,
                                 "L2A_MANIFEST_SAFE's docstring")
    L1C_UP_MTD_XML = property(get_l1c_up_mtd_xml, set_l1c_up_mtd_xml, del_l1c_up_mtd_xml, "L1C_UP_MTD_XML's docstring")
    L1C_DS_MTD_XML = property(get_l1c_ds_mtd_xml, set_l1c_ds_mtd_xml, del_l1c_ds_mtd_xml, "L1C_DS_MTD_XML's docstring")
    L1C_TILE_MTD_XML = property(get_l1c_tile_mtd_xml, set_l1c_tile_mtd_xml, del_l1c_tile_mtd_xml,
                                "L1C_TILE_MTD_XML's docstring")
    L1C_UP_ID = property(get_l1c_up_id, set_l1c_up_id, del_l1c_up_id, "L1C_UP_ID's docstring")
    L1C_DS_ID = property(get_l1c_ds_id, set_l1c_ds_id, del_l1c_ds_id, "L1C_DS_ID's docstring")
    L1C_TILE_ID = property(get_l1c_tile_id, set_l1c_tile_id, del_l1c_tile_id, "L1C_TILE_ID's docstring")
    L2A_UP_MTD_XML = property(get_l2a_up_mtd_xml, set_l2a_up_mtd_xml, del_l2a_up_mtd_xml,
                              "L2A_OPER_PRODUCT_MTD_XML's docstring")
    L2A_DS_MTD_XML = property(get_l2a_ds_mtd_xml, set_l2a_ds_mtd_xml, del_l2a_ds_mtd_xml, "L2A_DS_MTD_XML's docstring")
    L2A_TILE_MTD_XML = property(get_l2a_tile_mtd_xml, set_l2a_tile_mtd_xml, del_l2a_tile_mtd_xml,
                                "L2A_TILE_MTD_XML's docstring")
    L2A_TILE_ID = property(get_l2a_tile_id, set_l2a_tile_id, del_l2a_tile_id, "L2A_TILE_ID's docstring")
    L2A_DS_ID = property(get_l2a_ds_id, set_l2a_ds_id, del_l2a_ds_id, "L2A_TILE_ID's docstring")
    L2A_UP_ID = property(get_l2a_up_id, set_l2a_up_id, del_l2a_up_id, "L2A_UP_ID's docstring")
    L2A_UP_DIR = property(get_l2a_up_dir, set_l2a_up_dir, del_l2a_up_dir, "L2A_UP_DIR's docstring")
    L2A_BOA_QUANTIFICATION_VALUE = property(get_l2a_boa_quantification_value, set_l2a_boa_quantification_value,
                                            del_l2a_boa_quantification_value,
                                            "L2A_BOA_QUANTIFICATION_VALUE's docstring")
    L2A_WVP_QUANTIFICATION_VALUE = property(get_l2a_wvp_quantification_value, set_l2a_wvp_quantification_value,
                                            del_l2a_wvp_quantification_value,
                                            "L2A_WVP_QUANTIFICATION_VALUE's docstring")
    L2A_AOT_QUANTIFICATION_VALUE = property(get_l2a_aot_quantification_value, set_l2a_aot_quantification_value,
                                            del_l2a_aot_quantification_value,
                                            "L2A_AOT_QUANTIFICATION_VALUE's docstring")
    creationDate = property(get_creation_date, set_creation_date, del_creation_date, "creationDate's docstring")
    productVersion = property(get_product_version, set_product_version, del_product_version,
                              "productVersion's docstring")
    spacecraftName = property(get_spacecraft_name, set_spacecraft_name, del_spacecraft_name,
                              "spacecraft name's docstring")
    #     tTotal = property(get_t_total, set_t_total, del_t_total, "tTotal's docstring")
    T1_B04 = property(get_t_1_b_04, set_t_1_b_04, del_t_1_b_04, "T1_B04's docstring")
    T2_B04 = property(get_t_2_b_04, set_t_2_b_04, del_t_2_b_04, "T2_B04's docstring")
    T1_NDSI_CLD = property(get_t_1_ndsi_cld, set_t_1_ndsi_cld, del_t_1_ndsi_cld, "T1_NDSI_CLD's docstring")
    T2_NDSI_CLD = property(get_t_2_ndsi_cld, set_t_2_ndsi_cld, del_t_2_ndsi_cld, "T2_NDSI_CLD's docstring")
    T1_NDSI_SNW = property(get_t_1_ndsi_snw, set_t_1_ndsi_snw, del_t_1_ndsi_snw, "T1_NDSI_SNW's docstring")
    T2_NDSI_SNW = property(get_t_2_ndsi_snw, set_t_2_ndsi_snw, del_t_2_ndsi_snw, "T2_NDSI_SNW's docstring")
    T1_B02 = property(get_t_1_b_02, set_t_1_b_02, del_t_1_b_02, "T1_B02's docstring")
    T2_B02 = property(get_t_2_b_02, set_t_2_b_02, del_t_2_b_02, "T2_B02's docstring")
    T1_B8A = property(get_t_1_b_8_a, set_t_1_b_8_a, del_t_1_b_8_a, "T1_B8A's docstring")
    T2_B8A = property(get_t_2_b_8_a, set_t_2_b_8_a, del_t_2_b_8_a, "T2_B8A's docstring")
    T1_B10 = property(get_t_1_b_10, set_t_1_b_10, del_t_1_b_10, "T1_B10's docstring")
    T2_B10 = property(get_t_2_b_10, set_t_2_b_10, del_t_2_b_10, "T2_B10's docstring")
    T1_B12 = property(get_t_1_b_12, set_t_1_b_12, del_t_1_b_12, "T1_B12's docstring")
    T2_B12 = property(get_t_2_b_12, set_t_2_b_12, del_t_2_b_12, "T2_B12's docstring")
    T1_R_B02_B04 = property(get_t_1_r_b_02_b_04, set_t_1_r_b_02_b_04, del_t_1_r_b_02_b_04, "T1_R_B02_B04's docstring")
    T2_R_B02_B04 = property(get_t_2_r_b_02_b_04, set_t_2_r_b_02_b_04, del_t_2_r_b_02_b_04, "T2_R_B02_B04's docstring")
    T1_R_B8A_B03 = property(get_t_1_r_b_8_a_b_03, set_t_1_r_b_8_a_b_03, del_t_1_r_b_8_a_b_03,
                            "T1_R_B8A_B03's docstring")
    T2_R_B8A_B03 = property(get_t_2_r_b_8_a_b_03, set_t_2_r_b_8_a_b_03, del_t_2_r_b_8_a_b_03,
                            "T2_R_B8A_B03's docstring")
    T1_R_B8A_B11 = property(get_t_1_r_b_8_a_b_11, set_t_1_r_b_8_a_b_11, del_t_1_r_b_8_a_b_11,
                            "T1_R_B8A_B11's docstring")
    T2_R_B8A_B11 = property(get_t_2_r_b_8_a_b_11, set_t_2_r_b_8_a_b_11, del_t_2_r_b_8_a_b_11,
                            "T2_R_B8A_B11's docstring")
    T1_SNOW = property(get_t_1_snow, set_t_1_snow, del_t_1_snow, "T1_SNOW's docstring")
    T2_SNOW = property(get_t_2_snow, set_t_2_snow, del_t_2_snow, "T2_SNOW's docstring")
    T1_NDVI = property(get_t_1_ndvi, set_t_1_ndvi, del_t_1_ndvi, "T1_NDVI's docstring")
    T2_NDVI = property(get_t_2_ndvi, set_t_2_ndvi, del_t_2_ndvi, "T2_NDVI's docstring")
    T11_B02 = property(get_t_11_b_02, set_t_11_b_02, del_t_11_b_02, "T11_B02's docstring")
    T12_B02 = property(get_t_12_b_02, set_t_12_b_02, del_t_12_b_02, "T12_B02's docstring")
    T11_R_B02_B11 = property(get_t_11_r_b_02_b_11, set_t_11_r_b_02_b_11, del_t_11_r_b_02_b_11,
                             "T11_R_B02_B11's docstring")
    T12_R_B02_B11 = property(get_t_12_r_b_02_b_11, set_t_12_r_b_02_b_11, del_t_12_r_b_02_b_11,
                             "T12_R_B02_B11's docstring")
    T21_B12 = property(get_t_21_b_12, set_t_21_b_12, del_t_21_b_12, "T21_B12's docstring")
    T22_B12 = property(get_t_22_b_12, set_t_22_b_12, del_t_22_b_12, "T22_B12's docstring")
    T21_R_B02_B11 = property(get_t_21_r_b_02_b_11, set_t_21_r_b_02_b_11, del_t_21_r_b_02_b_11,
                             "T21_R_B02_B11's docstring")
    T22_R_B02_B11 = property(get_t_22_r_b_02_b_11, set_t_22_r_b_02_b_11, del_t_22_r_b_02_b_11,
                             "T22_R_B02_B11's docstring")
    noData = property(get_no_data, set_no_data, del_no_data, "noData's docstring")
    icirrus = property(get_icirrus, set_icirrus, del_icirrus, "icirrus' docstring")
    saturatedDefective = property(get_saturated_defective, set_saturated_defective, del_saturated_defective,
                                  "saturatedDefective's docstring")
    darkFeatures = property(get_dark_features, set_dark_features, del_dark_features, "darkFeatures's docstring")
    cloudShadows = property(get_cloud_shadows, set_cloud_shadows, del_cloud_shadows, "cloudShadows's docstring")
    vegetation = property(get_vegetation, set_vegetation, del_vegetation, "vegetation's docstring")
    bareSoils = property(get_bare_soils, set_bare_soils, del_bare_soils, "bareSoils's docstring")
    water = property(get_water, set_water, del_water, "water's docstring")
    lowProbaClouds = property(get_low_proba_clouds, set_low_proba_clouds, del_low_proba_clouds,
                              "lowProbaClouds's docstring")
    medProbaClouds = property(get_med_proba_clouds, set_med_proba_clouds, del_med_proba_clouds,
                              "medProbaClouds's docstring")
    highProbaClouds = property(get_high_proba_clouds, set_high_proba_clouds, del_high_proba_clouds,
                               "highProbaClouds's docstring")
    thinCirrus = property(get_thin_cirrus, set_thin_cirrus, del_thin_cirrus, "thinCirrus's docstring")
    snowIce = property(get_snow_ice, set_snow_ice, del_snow_ice, "snowIce's docstring")
    T_SDW = property(get_t_sdw, set_t_sdw, del_t_sdw, "T_SDW's docstring")
    T_B02_B12 = property(get_t_b_02_b_12, set_t_b_02_b_12, del_t_b_02_b_12, "T_B02_B12's docstring")
    medianFilter = property(get_median_filter, set_median_filter, del_median_filter, "medianFilter's docstring")
    demDirectory = property(get_dem_directory, set_dem_directory, del_dem_directory, "demDirectory's docstring")
    demReference = property(get_dem_reference, set_dem_reference, del_dem_reference, "demReference's docstring")
    snowMapReference = property(get_snow_map_reference, set_snow_map_reference, del_snow_map_reference,
                                "snowMapReference's docstring")
    esacciWaterBodiesReference = property(get_esacci_wb_map_reference, set_esacci_wb_map_reference,
                                          del_esacci_wb_map_reference,
                                          "esacciWaterBodiesReference's docstring")
    esacciLandCoverReference = property(get_esacci_lccs_map_reference, set_esacci_lccs_map_reference,
                                        del_esacci_lccs_map_reference,
                                        "esacciLandCoverReference's docstring")
    esacciSnowConditionDirReference = property(get_esacci_snowc_map_reference_directory,
                                               set_esacci_snowc_map_reference_directory,
                                               del_esacci_snowc_map_reference_directory,
                                               "esacciSnowConditionDirReference's docstring")
    scOnly = property(get_sc_only, set_sc_only, del_sc_only, "scOnly's docstring")
    processed60 = property(get_processed_60, set_processed_60, del_processed_60, "processed60's docstring")
    processed20 = property(get_processed_20, set_processed_20, del_processed_20, "processed20's docstring")
    processed10 = property(get_processed_10, set_processed_10, del_processed_10, "processed10's docstring")
    logger = property(get_logger, set_logger, del_logger, "log's docstring")
    logLevel = property(get_log_level, set_log_level, del_log_level, "logLevel's docstring")
    operationMode = property(get_operation_mode, set_operation_mode, del_operation_mode, "operationModes's docstring")
    fnLog = property(get_fn_log, set_fn_log, del_fn_log, "fnLog's docstring")
    selectedTile = property(get_selected_tile, set_selected_tile, del_selected_tile, "selectedTile's docstring")
    tEstimation = property(get_t_estimation, set_t_estimation, del_t_estimation, "tEstimation's docstring")
    tStart = property(get_t_start, set_t_start, del_t_start, "tStart's docstring")
    TESTMODE = property(get_testmode, set_testmode, del_testmode, "TESTMODE's docstring")
    nrTiles = property(get_nr_tiles, set_nr_tiles, del_nr_tiles, "nrTiles's docstring")
    nrThreads = property(get_nr_threads, set_nr_threads, del_nr_threads, "nrThreads's docstring")
    processingStatusFn = property(get_processing_status_fn, set_processing_status_fn, del_processing_status_fn,
                                  "processingStatusFn's docstring")
    processingEstimationFn = property(get_processing_estimation_fn, set_processing_estimation_fn,
                                      del_processing_estimation_fn, "processingEstimationFn's docstring")
    aerosolType = property(get_aerosol_type, set_aerosol_type, del_aerosol_type, "aerosolType's docstring")
    midLatitude = property(get_mid_latitude, set_mid_latitude, del_mid_latitude, "midLatitude's docstring")
    ozoneContent = property(get_ozone_content, set_ozone_content, del_ozone_content, "ozoneContent's docstring")
    waterVapour = property(get_water_vapour, set_water_vapour, del_water_vapour, "waterVapour's docstring")
    ozoneSetpoint = property(get_ozone_setpoint, set_ozone_setpoint, del_ozone_setpoint, "ozoneSetpoint's docstring")
    timeout = property(get_timeout, set_timeout, del_timeout, "timeout's docstring")
    # fix for SIIMPC-552, UMW - making options configurable:
    scaling_disabler = property(get_scaling_disabler, set_scaling_disabler, del_scaling_disabler,
                                "scaling_disabler's docstring")
    scaling_limiter = property(get_scaling_limiter, set_scaling_limiter, del_scaling_limiter,
                               "scaling_limiter's docstring")
    # implementation of SIIMPC-557, UMW:
    rho_retrieval_step2 = property(get_rho_retrieval_step2, set_rho_retrieval_step2, del_rho_retrieval_step2,
                                   "rho_retrieval_step2's docstring")
    min_sc_blu = property(get_min_sc_blu, set_min_sc_blu, del_min_sc_blu, "min_sc_blu's docstring")
    max_sc_blu = property(get_max_sc_blu, set_max_sc_blu, del_max_sc_blu, "max_sc_blu's docstring")
    db_compression_level = property(get_db_compression_level, set_db_compression_level, del_db_compression_level, "db_compression_level's docstring")
    namingConvention = property(get_naming_convention, set_naming_convention, del_naming_convention,
                                "naming_convention's docstring")
    datatakeSensingTime = property(get_datatake_sensing_time, set_datatake_sensing_time, del_datatake_sensing_time,
                                   "datatake_sensing_time's docstring")
    demType = property(get_dem_type, set_dem_type, del_dem_type, "demType's docstring")
    upScheme1c = property(get_up_scheme_1c, set_up_scheme_1c, del_up_scheme_1c, "upScheme1c's docstring")
    upScheme2a = property(get_up_scheme_2a, set_up_scheme_2a, del_up_scheme_2a, "upScheme2a's docstring")
    tileScheme1c = property(get_tile_scheme_1c, set_tile_scheme_1c, del_tile_scheme_1c, "tileScheme1c's docstring")
    tileScheme2a = property(get_tile_scheme_2a, set_tile_scheme_2a, del_tile_scheme_2a, "tileScheme2a's docstring")
    dsScheme1c = property(get_ds_scheme_1c, set_ds_scheme_1c, del_ds_scheme_1c, "dsScheme1c's docstring")
    dsScheme2a = property(get_ds_scheme_2a, set_ds_scheme_2a, del_ds_scheme_2a, "dsScheme2a's docstring")
    gippScheme2a = property(get_gipp_scheme_2a, set_gipp_scheme_2a, del_gipp_scheme_2a, "gippScheme2a's docstring")
    gippSchemePb = property(get_gipp_scheme_pb, set_gipp_scheme_pb, del_gipp_scheme_pb, "gippSchemePb's docstring")
    gippSchemeSc = property(get_gipp_scheme_sc, set_gipp_scheme_sc, del_gipp_scheme_sc, "gippSchemeSc's docstring")
    gippSchemeAc = property(get_gipp_scheme_ac, set_gipp_scheme_ac, del_gipp_scheme_ac, "gippSchemeAc's docstring")
    manifestScheme = property(get_manifest_scheme, set_manifest_scheme, del_manifest_scheme,
                              "manifestScheme's docstring")

    AC_Min_Ddv_Area = property(get_ac_min_ddv_area, set_ac_min_ddv_area, del_ac_min_ddv_area,
                               "AC_Min_Ddv_Area's docstring")
    AC_Swir_Refl_Lower_Th = property(get_ac_swir_refl_lower_th, set_ac_swir_refl_lower_th, del_ac_swir_refl_lower_th,
                                     "AC_Swir_Refl_Lower_Th's docstring")
    AC_Swir_22um_Red_Refl_Ratio = property(get_ac_swir_22um_red_refl_ratio, set_ac_swir_22um_red_refl_ratio,
                                           del_ac_swir_22um_red_refl_ratio, "AC_Swir_22um_Red_Refl_Ratio's docstring")
    AC_Red_Blue_Refl_Ratio = property(get_ac_red_blue_refl_ratio, set_ac_red_blue_refl_ratio,
                                      del_ac_red_blue_refl_ratio, "AC_Red_Blue_Refl_Ratio's docstring")
    AC_Cut_Off_Aot_Iter_Vegetation = property(get_ac_cut_off_aot_iter_vegetation, set_ac_cut_off_aot_iter_vegetation,
                                              del_ac_cut_off_aot_iter_vegetation,
                                              "AC_Cut_Off_Aot_Iter_Vegetation's docstring")
    AC_Cut_Off_Aot_Iter_Water = property(get_ac_cut_off_aot_iter_water, set_ac_cut_off_aot_iter_water,
                                         del_ac_cut_off_aot_iter_water, "AC_Cut_Off_Aot_Iter_Water's docstring")
    AC_Aerosol_Type_Ratio_Th = property(get_ac_aerosol_type_ratio_th, set_ac_aerosol_type_ratio_th,
                                        del_ac_aerosol_type_ratio_th, "AC_Aerosol_Type_Ratio_Th's docstring")
    AC_Topo_Corr_Th = property(get_ac_topo_corr_th, set_ac_topo_corr_th, del_ac_topo_corr_th,
                               "AC_Topo_Corr_Th's docstring")
    AC_Slope_Th = property(get_ac_slope_th, set_ac_slope_th, del_ac_slope_th, "AC_Slope_Th's docstring")
    AC_Dem_P2p_Val = property(get_ac_dem_p2p_val, set_ac_dem_p2p_val, del_ac_dem_p2p_val, "AC_Dem_P2p_Val's docstring")
    AC_Swir_Refl_Ndvi_Th = property(get_ac_swir_refl_ndvi_th, set_ac_swir_refl_ndvi_th, del_ac_swir_refl_ndvi_th,
                                    "AC_Swir_Refl_Ndvi_Th's docstring")
    AC_Ddv_Swir_Refl_Th1 = property(get_ac_ddv_swir_refl_th_1, set_ac_ddv_swir_refl_th_1, del_ac_ddv_swir_refl_th_1,
                                    "AC_Ddv_Swir_Refl_Th1's docstring")
    AC_Ddv_Swir_Refl_Th2 = property(get_ac_ddv_swir_refl_th_2, set_ac_ddv_swir_refl_th_2, del_ac_ddv_swir_refl_th_2,
                                    "AC_Ddv_Swir_Refl_Th2's docstring")
    AC_Ddv_Swir_Refl_Th3 = property(get_ac_ddv_swir_refl_th_3, set_ac_ddv_swir_refl_th_3, del_ac_ddv_swir_refl_th_3,
                                    "AC_Ddv_Swir_Refl_Th3's docstring")
    AC_Ddv_16um_Refl_Th1 = property(get_ac_ddv_16um_refl_th_1, set_ac_ddv_16um_refl_th_1, del_ac_ddv_16um_refl_th_1,
                                    "AC_Ddv_16um_Refl_Th1's docstring")
    AC_Ddv_16um_Refl_Th2 = property(get_ac_ddv_16um_refl_th_2, set_ac_ddv_16um_refl_th_2, del_ac_ddv_16um_refl_th_2,
                                    "AC_Ddv_16um_Refl_Th2's docstring")
    AC_Ddv_16um_Refl_Th3 = property(get_ac_ddv_16um_refl_th_3, set_ac_ddv_16um_refl_th_3, del_ac_ddv_16um_refl_th_3,
                                    "AC_Ddv_16um_Refl_Th3's docstring")
    AC_Dbv_Nir_Refl_Th = property(get_ac_dbv_nir_refl_th, set_ac_dbv_nir_refl_th, del_ac_dbv_nir_refl_th,
                                  "AC_Dbv_Nir_Refl_Th's docstring")
    AC_Dbv_Ndvi_Th = property(get_ac_dbv_ndvi_th, set_ac_dbv_ndvi_th, del_ac_dbv_ndvi_th, "AC_Dbv_Ndvi_Th's docstring")
    AC_Red_Ref_Refl_Th = property(get_ac_red_ref_refl_th, set_ac_red_ref_refl_th, del_ac_red_ref_refl_th,
                                  "AC_Red_Ref_Refl_Th's docstring")
    AC_Dbv_Red_Veget_Tst_Ndvi_Th = property(get_ac_dbv_red_veget_tst_ndvi_th, set_ac_dbv_red_veget_tst_ndvi_th,
                                            del_ac_dbv_red_veget_tst_ndvi_th,
                                            "AC_Dbv_Red_Veget_Tst_Ndvi_Th's docstring")
    AC_Dbv_Red_Veget_Refl_Th = property(get_ac_dbv_red_veget_refl_th, set_ac_dbv_red_veget_refl_th,
                                        del_ac_dbv_red_veget_refl_th, "AC_Dbv_Red_Veget_Refl_Th's docstring")
    AC_Wv_Iter_Start_Summer = property(get_ac_wv_iter_start_summer, set_ac_wv_iter_start_summer,
                                       del_ac_wv_iter_start_summer, "AC_Wv_Iter_Start_Summer's docstring")
    AC_Wv_Iter_Start_Winter = property(get_ac_wv_iter_start_winter, set_ac_wv_iter_start_winter,
                                       del_ac_wv_iter_start_winter, "AC_Wv_Iter_Start_Winter's docstring")
    AC_Rng_Nbhd_Terrain_Corr = property(get_ac_rng_nbhd_terrain_corr, set_ac_rng_nbhd_terrain_corr,
                                        del_ac_rng_nbhd_terrain_corr, "AC_Rng_Nbhd_Terrain_Corr's docstring")
    AC_Max_Nr_Topo_Iter = property(get_ac_max_nr_topo_iter, set_ac_max_nr_topo_iter, del_ac_max_nr_topo_iter,
                                   "AC_Max_Nr_Topo_Iter's docstring")
    AC_Topo_Corr_Cutoff = property(get_ac_topo_corr_cutoff, set_ac_topo_corr_cutoff, del_ac_topo_corr_cutoff,
                                   "AC_Topo_Corr_Cutoff's docstring")
    AC_Vegetation_Index_Th = property(get_ac_vegetation_index_th, set_ac_vegetation_index_th,
                                      del_ac_vegetation_index_th, "AC_Vegetation_Index_Th's docstring")
    AC_Limit_Area_Path_Rad_Scale = property(get_ac_limit_area_path_rad_scale, set_ac_limit_area_path_rad_scale,
                                            del_ac_limit_area_path_rad_scale,
                                            "AC_Limit_Area_Path_Rad_Scale's docstring")
    AC_Ddv_Smooting_Window = property(get_ac_ddv_smooting_window, set_ac_ddv_smooting_window,
                                      del_ac_ddv_smooting_window, "AC_Ddv_Smooting_Window's docstring")
    AC_Terrain_Refl_Start = property(get_ac_terrain_refl_start, set_ac_terrain_refl_start, del_ac_terrain_refl_start,
                                     "AC_Terrain_Refl_Start's docstring")
    AC_Spr_Refl_Percentage = property(get_ac_spr_refl_percentage, set_ac_spr_refl_percentage,
                                      del_ac_spr_refl_percentage, "AC_Spr_Refl_Percentage's docstring")
    AC_Spr_Refl_Promille = property(get_ac_spr_refl_promille, set_ac_spr_refl_promille, del_ac_spr_refl_promille,
                                    "AC_Spr_Refl_Promille's docstring")
    sc_lp_blu =  property(get_sc_lp_blu, set_sc_lp_blu, del_sc_lp_blu, "sc_lp_blu's docstring")
    sc_lp_blu =  property(get_sc_lp_blu, set_sc_lp_blu, del_sc_lp_blu, "sc_lp_blu's docstring")
    picFn =  property(get_pic_fn, set_pic_fn, del_pic_fn, "config pic filename docstring")


    def init_home_directory(self,operationMode):
        if operationMode == 'TOOLBOX':
            try:
                self._home = os.environ['SEN2COR_HOME']
            except:
                self._home = getScriptDir()
            self._configDir = os.path.join(getScriptDir(), 'cfg')
            self.configPB = os.path.join(self._configDir, 'L2A_PB_GIPP.xml')
        else:
            try:
                self._home = os.environ['SEN2COR_BIN']
            except:
                self._home = getScriptDir()
            self._configDir = os.path.join(self._home, 'cfg')

        if not os.path.exists(self._configDir) and operationMode == 'TOOLBOX':
            os.mkdir(self._configDir)

        if self._work_dir:
            self._logDir = self._work_dir
        else:
            self._logDir = os.path.join(self._home, 'log')
            if not os.path.exists(self._logDir):
                os.mkdir(self._logDir)

        self.configFn = os.path.join(self._home, 'cfg', 'L2A_GIPP.xml')
        self.configSC = os.path.join(self._configDir, 'L2A_CAL_SC_GIPP.xml')
        self.configAC = os.path.join(self._configDir, 'L2A_CAL_AC_GIPP.xml')

        self._processingStatusFn = os.path.join(self._logDir, '.progress')
        self._processingEstimationFn = os.path.join(self._logDir, '.estimation')

        if not os.path.isfile(self._processingEstimationFn):
            # init processing estimation file:
            config = ConfigParser.RawConfigParser()
            config.add_section('time estimation')
            config.set('time estimation', 't_est_60', self._tEst60)
            config.set('time estimation', 't_est_20', self._tEst20)
            config.set('time estimation', 't_est_10', self._tEst10)
            configFile = open(self._processingEstimationFn, 'w')
            config.write(configFile)
            configFile.close() 
            
    def preprocess(self, resolution):
        self.resolution = resolution
        try:
            self.readPreferences()
            self.set_L2A_DS_and_Tile_metadata()
        except:
            self.logger.error('Preprocessing failed')
            return False
        return True

    def setSchemes(self):
        try:
            doc = objectify.parse(self.configFn)
            root = doc.getroot()
            psd = root.Common_Section.PSD_Scheme
            psdLen = len(psd)
            for i in range(psdLen):
                # this implements the version dependency for the PSD:
                if (psd[i].attrib['PSD_Version']) == str(self.productVersion):
                    prefix = psd[i].attrib['PSD_Reference']
                    self._upScheme1c = os.path.join(prefix, psd[i].UP_Scheme_1C.text + '.xsd')
                    upScheme2a = os.path.join(prefix, psd[i].UP_Scheme_2A.text)
                    self._tileScheme1c = os.path.join(prefix, psd[i].Tile_Scheme_1C.text + '.xsd')
                    tileScheme2a = os.path.join(prefix, psd[i].Tile_Scheme_2A.text)
                    self._dsScheme1c = os.path.join(prefix, psd[i].DS_Scheme_1C.text + '.xsd')
                    dsScheme2a = os.path.join(prefix, psd[i].DS_Scheme_2A.text)

                    if self.productVersion == float32(14.2):
                        # SIIMPC-1390: force L2A Product to convert to PSD Version 14.5
                        upScheme2a = upScheme2a.replace('14.2','14.5')
                        tileScheme2a = tileScheme2a.replace('14.2','14.5')
                        dsScheme2a = dsScheme2a.replace('14.2','14.5')

                    self._upScheme2a = upScheme2a + '.xsd'
                    self._tileScheme2a = tileScheme2a + '.xsd'
                    self._dsScheme2a = dsScheme2a + '.xsd'
                    break

            self._gippScheme2a = root.Common_Section.GIPP_Scheme.text + '.xsd'
            self._gippSchemeSc = root.Common_Section.SC_Scheme.text + '.xsd'
            self._gippSchemeAc = root.Common_Section.AC_Scheme.text + '.xsd'
            self._gippSchemePb = root.Common_Section.PB_Scheme.text + '.xsd'
            self._manifestScheme = os.path.join(prefix[:-6] + 'SAFE', 'resources', \
                                                'xsd', 'int', 'esa', 'safe', 'sentinel', '1.1', \
                                                'sentinel-2', 'msi', 'archive_l2a_user_product', 'xfdu.xsd')
        except:

            if not self.logger:
                from L2A_Logger import L2A_Logger
                self.logger = L2A_Logger('sen2cor')
            self.logger.fatal('wrong identifier for xml structure.')

        return

    def create_L2A_UserProduct(self):
        # initialize variables:
        firstInit = False
        if self.namingConvention == 'SAFE_STANDARD':
            L1C_UP_MASK = 'S2?_OPER_PRD_MSIL1C*.SAFE'
        else:
            L1C_UP_MASK = 'S2?_MSIL1C*.SAFE'

        L1C_UP_DIR = self.input_dir
        if not os.path.exists(L1C_UP_DIR):
            self.logger.fatal('directory "%s" does not exist.' % L1C_UP_DIR)
            return False

        # detect the filename for the datastrip metadata:
        L1C_DS_DIR = os.path.join(L1C_UP_DIR, 'DATASTRIP')
        if not os.path.exists(L1C_DS_DIR):
            self.logger.fatal('directory "%s" does not exist.' % L1C_DS_DIR)
            return False

        if self.namingConvention == 'SAFE_STANDARD':
            L1C_DS_MASK = 'S2?_OPER_MSI_L1C_DS_*'
        else:
            L1C_DS_MASK = 'DS_*'

        dirlist = sorted(os.listdir(L1C_DS_DIR))
        found = False

        for dirname in dirlist:
            if (fnmatch.fnmatch(dirname, L1C_DS_MASK) == True):
                found = True
                break

        if not found:
            self.logger.fatal('No metadata in datastrip.')
            return False

        L1C_DS_DIR = os.path.join(L1C_DS_DIR, dirname)
        if self.namingConvention == 'SAFE_STANDARD':
            L1C_DS_MTD_XML = (dirname[:-7] + '.xml').replace('_MSI_', '_MTD_')
        else:
            L1C_DS_MTD_XML = 'MTD_DS.xml'

        self.L1C_DS_MTD_XML = os.path.join(L1C_DS_DIR, L1C_DS_MTD_XML)

        dirname, basename = os.path.split(L1C_UP_DIR)
        if (fnmatch.fnmatch(basename, L1C_UP_MASK) == False):
            self.logger.fatal('%s: identifier %s is missing.' % (basename, L1C_UP_MASK))
            return False

        GRANULE = os.path.join(L1C_UP_DIR, 'GRANULE')
        if not os.path.exists(GRANULE):
            self.logger.fatal('directory "%s" does not exist.' % GRANULE)
            return False

        #
        # the product (directory) structure:
        # -------------------------------------------------------
        L1C_UP_ID = basename
        l1cUpS = L1C_UP_ID.split('_')
        baseline = self.processingBaseline
        pbStr = 'N%05.2f' % baseline
        self._UPgenerationTimestamp = datetime.utcnow()
        generationTimeStr = strftime('%Y%m%dT%H%M%S', self._UPgenerationTimestamp .timetuple())
        gtsSafe = generationTimeStr + '.SAFE'
        if self.namingConvention == 'SAFE_STANDARD':
            L1C_TILE_ID = self.getTileId1c()
            l1cTileS = L1C_TILE_ID.split('_')
            L2A_UP_ID = '_'.join([l1cUpS[0], 'MSIL2A', l1cUpS[5], pbStr[:-3] + pbStr[4:], gtsSafe, l1cTileS[-2], l1cTileS[-4]])
            L2A_UP_DIR = os.path.join(self.output_dir, L2A_UP_ID + '.SAFE')
            self.datatakeSensingTime = l1cUpS[5]
        else:
            MSIL2A = 'MSIL2A'
            if len(l1cUpS) == 7:
                L2A_UP_ID = '_'.join([l1cUpS[0], MSIL2A, l1cUpS[2], pbStr[:-3] + pbStr[4:], l1cUpS[4], l1cUpS[5], gtsSafe])
            else:
                L2A_UP_ID = '_'.join([l1cUpS[0], MSIL2A, l1cUpS[2], pbStr[:-3] + pbStr[4:], l1cUpS[4], gtsSafe])
            L2A_UP_DIR = os.path.join(self.output_dir, L2A_UP_ID)
            self.datatakeSensingTime = l1cUpS[2]
        L1C_INSPIRE_XML = os.path.join(L1C_UP_DIR, 'INSPIRE.xml')
        L1C_MANIFEST_SAFE = os.path.join(L1C_UP_DIR, 'manifest.safe')
        L2A_INSPIRE_XML = os.path.join(L2A_UP_DIR, 'INSPIRE.xml')
        L2A_MANIFEST_SAFE = os.path.join(L2A_UP_DIR, 'manifest.safe')
        self.L2A_MANIFEST_SAFE = L2A_MANIFEST_SAFE

        AUX_DATA = 'AUX_DATA'
        DATASTRIP = 'DATASTRIP'
        GRANULE = 'GRANULE'
        HTML = 'HTML'
        REP_INFO = 'rep_info'

        if (os.name == 'nt') and (self.namingConvention == 'SAFE_STANDARD'):
            # special treatment for windows for long pathnames:
            L1C_UP_DIR_ = u'\\'.join([u'\\\\?', L1C_UP_DIR])
            L1C_INSPIRE_XML_ = u'\\'.join([u'\\\\?', L1C_INSPIRE_XML])
            L1C_MANIFEST_SAFE_ = u'\\'.join([u'\\\\?', L1C_MANIFEST_SAFE])
            L2A_UP_DIR_ = u'\\'.join([u'\\\\?', L2A_UP_DIR])
            L2A_INSPIRE_XML_ = u'\\'.join([u'\\\\?', L2A_INSPIRE_XML])
            L2A_MANIFEST_SAFE_ = u'\\'.join([u'\\\\?', L2A_MANIFEST_SAFE])
        else:
            L1C_UP_DIR_ = L1C_UP_DIR
            L1C_INSPIRE_XML_ = L1C_INSPIRE_XML
            L1C_MANIFEST_SAFE_ = L1C_MANIFEST_SAFE
            L2A_UP_DIR_ = L2A_UP_DIR
            L2A_INSPIRE_XML_ = L2A_INSPIRE_XML
            L2A_MANIFEST_SAFE_ = L2A_MANIFEST_SAFE

        self.L1C_UP_ID = L1C_UP_ID
        self.L2A_UP_ID = L2A_UP_ID
        self.L1C_UP_DIR = L1C_UP_DIR_
        self.L2A_UP_DIR = L2A_UP_DIR_
        fn_L2A = 'MTD_MSIL2A.xml'
        fn_L2A = os.path.join(L2A_UP_DIR_, fn_L2A)
        self.L2A_UP_MTD_XML = fn_L2A
        self.INSPIRE_XML = os.path.join(L2A_UP_DIR_, 'INSPIRE.xml')
        self.UP_INDEX_HTML = os.path.join(L2A_UP_DIR_, 'HTML', 'UserProduct_index.html')

        # now create:
        if not os.path.exists(os.path.join(L2A_UP_DIR, GRANULE)):
            firstInit = True
            try:
                copytree(os.path.join(L1C_UP_DIR_, AUX_DATA), os.path.join(L2A_UP_DIR_, AUX_DATA))
                copytree(os.path.join(L1C_UP_DIR_, DATASTRIP), os.path.join(L2A_UP_DIR_, DATASTRIP))
                #copytree(os.path.join(L1C_UP_DIR_, HTML), os.path.join(L2A_UP_DIR_, HTML))
                copytree(os.path.join(L1C_UP_DIR_, REP_INFO), os.path.join(L2A_UP_DIR_, REP_INFO))
                # fix for SIIMPC-598.1 UMW:
                chmod_recursive(L2A_UP_DIR_, 0755)
                # end fix for SIIMPC-598.1
                # remove the L1C xsds:
                S2_mask = 'S2_*.xsd'
                repdir = os.path.join(L2A_UP_DIR_, REP_INFO)
                filelist = os.listdir(repdir)

                for filename in filelist:
                    if (fnmatch.fnmatch(filename, S2_mask) == True):
                        os.remove(os.path.join(repdir, filename))
                try:
                    copyfile(L1C_MANIFEST_SAFE_, L2A_MANIFEST_SAFE_)
                    os.chmod(L2A_MANIFEST_SAFE_, 0755)
                except:
                    self.L2A_MANIFEST_SAFE = None
                try:
                    copyfile(L1C_INSPIRE_XML_, L2A_INSPIRE_XML_)
                    os.chmod(L2A_INSPIRE_XML_, 0755)
                except:
                    pass
                os.mkdir(os.path.join(L2A_UP_DIR_, GRANULE))
                # end fix for SIIMPC-598.1

                # this is to set the version info from the L1C User Product:
                if not self.setProductVersion():
                    return False
                self.setSchemes()
            except:
                self.logger.fatal('Error in creating L2A User Product')
                return False

            # copy L2A schemes from config_dir into rep_info:
            REP_INFO = 'rep_info'
            try:
                copyfile(os.path.join(self.configDir, self._upScheme2a),
                         os.path.join(L2A_UP_DIR, REP_INFO, os.path.basename(self._upScheme2a)))
                copyfile(os.path.join(self.configDir, self._tileScheme2a),
                         os.path.join(L2A_UP_DIR, REP_INFO, os.path.basename(self._tileScheme2a)))
                copyfile(os.path.join(self.configDir, self._dsScheme2a),
                         os.path.join(L2A_UP_DIR, REP_INFO, os.path.basename(self._dsScheme2a)))
            except:
                self._logger.fatal('error in copying metadata files to target product')
                return False

            # create user product:
            if self.namingConvention == 'SAFE_STANDARD':
                S2A_mask = 'S2?_OPER_MTD_SAFL1C*.xml'
            else:
                S2A_mask = 'MTD_MSIL1C.xml'
            try:
                filelist = sorted(os.listdir(L1C_UP_DIR))
                found = False
            except:
                self.logger.fatal('L1C input is not accessible')
                return False

            for filename in filelist:
                if fnmatch.fnmatch(filename, S2A_mask):
                    found = True
                    break
            if not found:
                self.logger.fatal('No metadata for user product')
                return False

            # prepare L2A User Product metadata file
            fn_L1C = os.path.join(L1C_UP_DIR_, filename)
            # copy L2A User Product metadata file:
            copyfile(fn_L1C, fn_L2A)
            self.L1C_UP_MTD_XML = fn_L1C

        return firstInit

    def configure_L2A_UP_metadata(self):
        # remove old L1C entries from L1C_UP_MTD_XML:
        xp = L2A_XmlParser(self, 'UP1C')
        pi1c = xp.getTree('General_Info', 'Product_Info')
        # SIIMPC-1152: get the Datatake Identifier from L1C:
        di2a = pi1c.Datatake
        datatakeIdentifier = di2a.attrib['datatakeIdentifier'].split('_N')
        baseline = self.processingBaseline
        datatakeIdentifier[-1] = '{:05.2f}'.format(float(baseline))
        xp = L2A_XmlParser(self, 'UP2A')
        if (xp.convert() == False):
            self.logger.fatal('error in converting user product metadata to level 2A')

        productInfo = 'Product_Info'
        xp = L2A_XmlParser(self, 'UP2A')
        pi2a = xp.getTree('General_Info', productInfo)
        # SIIMPC-1152: keep the Datatake Identifier from L1C:
        dataTake = pi2a.Datatake
        dataTake.attrib['datatakeIdentifier'] = '_N'.join(datatakeIdentifier)
        del pi2a.Product_Organisation.Granule_List[:]
        gl = objectify.Element('Granule_List')
        pi2a.Product_Organisation.append(gl)
        pi2a.PRODUCT_URI = self.L2A_UP_ID
        pi2a.PROCESSING_LEVEL = 'Level-2A'
        pi2a.PRODUCT_TYPE = 'S2MSI2A'
        pi2a.GENERATION_TIME = strftime('%Y-%m-%dT%H:%M:%S.',
            self._UPgenerationTimestamp.timetuple()) + \
            str(self._UPgenerationTimestamp.microsecond)[:3] + 'Z'
        qo = pi2a.Query_Options
        try:
            qo.Aux_List.attrib['productLevel'] = 'Level-2Ap'
        except:
            pass

        sceneClassificationText = ['SC_NODATA',
                                    'SC_SATURATED_DEFECTIVE',
                                    'SC_DARK_FEATURE_SHADOW',
                                    'SC_CLOUD_SHADOW',
                                    'SC_VEGETATION',
                                    'SC_NOT_VEGETATED',
                                    'SC_WATER',
                                    'SC_UNCLASSIFIED',
                                    'SC_CLOUD_MEDIUM_PROBA',
                                    'SC_CLOUD_HIGH_PROBA',
                                    'SC_THIN_CIRRUS',
                                    'SC_SNOW_ICE']

        pic = xp.getTree('General_Info', 'Product_Image_Characteristics')
        qvl = objectify.Element('QUANTIFICATION_VALUES_LIST')
        qvl.BOA_QUANTIFICATION_VALUE = str(int(self._dnScale))
        qvl.BOA_QUANTIFICATION_VALUE.attrib['unit'] = 'none'
        qvl.AOT_QUANTIFICATION_VALUE = str(self.L2A_AOT_QUANTIFICATION_VALUE)
        qvl.AOT_QUANTIFICATION_VALUE.attrib['unit'] = 'none'
        qvl.WVP_QUANTIFICATION_VALUE = str(self.L2A_WVP_QUANTIFICATION_VALUE)
        qvl.WVP_QUANTIFICATION_VALUE.attrib['unit'] = 'cm'
        pic.QUANTIFICATION_VALUES_LIST = qvl
        scl = objectify.Element('Scene_Classification_List')
        for i in range(0, len(sceneClassificationText)):
            scid = objectify.Element('Scene_Classification_ID')
            scid.SCENE_CLASSIFICATION_TEXT = sceneClassificationText[i]
            scid.SCENE_CLASSIFICATION_INDEX = str(i)
            scl.append(scid)
        pic.append(scl)
        #SIIMPC-1152 RBS Patch: GNR Fix
        lfn = etree.Element('LUT_FILENAME')
        lfn.text = 'None'
        auxinfo = xp.getNode('Auxiliary_Data_Info')

        if (xp.getTree('Auxiliary_Data_Info', 'GRI_List')) == False:
            gfn = xp.getTree('Auxiliary_Data_Info', 'GRI_FILENAME')
            del gfn[:]
            gl = objectify.Element('GRI_List')
            gl.append(gfn)
            auxinfo.insert(3, gl)
        try:
            ll = xp.getTree('Auxiliary_Data_Info', 'LUT_List')
            ll.append(lfn)
        except:
            ll = objectify.Element('LUT_List')
            ll.append(lfn)
            auxinfo.insert(5, ll)
        try:
            esacciWaterBodies = self.esacciWaterBodiesReference
            esacciWaterBodies = os.path.join(self.auxDir, esacciWaterBodies)
            esacciLandCover = self.esacciLandCoverReference
            esacciLandCover = os.path.join(self.auxDir, esacciLandCover)
            esacciSnowConditionDir = self.esacciSnowConditionDirReference
            esacciSnowConditionDir = os.path.join(self.auxDir, esacciSnowConditionDir)
            item = xp.getTree('Auxiliary_Data_Info', 'SNOW_CLIMATOLOGY_MAP')
            item._setText(self.snowMapReference)
            item = xp.getTree('Auxiliary_Data_Info', 'ESACCI_WaterBodies_Map')
            if ((os.path.isfile(esacciWaterBodies)) == True):
                item._setText(self.esacciWaterBodiesReference)
            else:
                item._setText('None')
            item = xp.getTree('Auxiliary_Data_Info', 'ESACCI_LandCover_Map')
            if ((os.path.isfile(esacciLandCover)) == True):
                item._setText(self.esacciLandCoverReference)
            else:
                item._setText('None')
            item = xp.getTree('Auxiliary_Data_Info', 'ESACCI_SnowCondition_Map_Dir')
            if (os.path.isdir(esacciSnowConditionDir)) == True:
                item._setText(self.esacciSnowConditionDirReference)
            else:
                item._setText('None')
        except:
            auxinfo = xp.getNode('Auxiliary_Data_Info')
            item = etree.Element('SNOW_CLIMATOLOGY_MAP')
            item.text = self.snowMapReference
            auxinfo.insert(6, item)
            item = etree.Element('ESACCI_WaterBodies_Map')
            if ((os.path.isfile(esacciWaterBodies)) == True):
                item.text = self.esacciWaterBodiesReference
            else:
                item.text = 'None'
            auxinfo.insert(7, item)
            item = etree.Element('ESACCI_LandCover_Map')
            if ((os.path.isfile(esacciLandCover)) == True):
                item.text = self.esacciLandCoverReference
            else:
                item.text = 'None'
            auxinfo.insert(8, item)
            item = etree.Element('ESACCI_SnowCondition_Map_Dir')
            if (os.path.isdir(esacciSnowConditionDir)) == True:
                item.text = self.esacciSnowConditionDirReference
            else:
                item.text = 'None'
            auxinfo.insert(9, item)

        xp.export()
        return

    def create_L2A_Datastrip(self):
        # Generate Datastrip
        if self.L2A_DS_ID:
            self.logger.info('datastrip already exists, no action required')
        else:
            datastrip = L2A_ProcessDataStrip(self)
            datastrip.generate()
            self.L2A_DS_MTD_XML = datastrip.L2A_DS_MTD_XML
            self.L2A_DS_ID = datastrip.L2A_DS_ID
            self.L2A_DS_DIR = datastrip.L2A_DS_DIR
            self._processing_centre = datastrip.processing_centre
            self._archiving_centre = datastrip.archiving_centre
        return

    def read_L2A_Datastrip(self):
        # Read Datastrip Location:
        DATASTRIP = 'DATASTRIP'
        DS_mask = 'DS_*'
        MTD_DS = 'MTD_DS.xml'
        dsdir = os.path.join(self.L2A_UP_DIR, DATASTRIP)
        filelist = os.listdir(dsdir)

        for filename in filelist:
            if (fnmatch.fnmatch(filename, DS_mask) == True):
                self.L2A_DS_ID = filename
                self.L2A_DS_DIR = os.path.join(self.L2A_UP_DIR, DATASTRIP, filename)
                self.L2A_DS_MTD_XML = os.path.join(self.L2A_DS_DIR, MTD_DS)
                return True

        return False

    def getEntriesFromDatastrip(self):
        # get the bandIndex
        if self._resolution == 10:
            bandIndex = [1, 2, 3, 7]
            self._ch940 = [0, 0, 0, 0, 0, 0]
        else:
            bandIndex = [0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12]
            self._ch940 = [8, 8, 9, 9, 0, 0]
        # this must always be initialized:
        xp = L2A_XmlParser(self, 'DS2A')
        di = xp.getTree('General_Info', 'Datatake_Info')
        self.spacecraftName = di.SPACECRAFT_NAME.text
        pic = xp.getTree('Image_Data_Info', 'Radiometric_Info')
        try:
            self._dnScale = float32(pic.QUANTIFICATION_VALUES_LIST.BOA_QUANTIFICATION_VALUE.text)
        except:
            self._dnScale = float32(pic.QUANTIFICATION_VALUE.text)
        node = pic.Reflectance_Conversion.Solar_Irradiance_List
        i = 0
        self._e0 = zeros(size(bandIndex), float32)
        for index in bandIndex:
            par = node.SOLAR_IRRADIANCE[index]
            if par is None: self.parNotFound(par)
            self._e0[i] = par.pyval / 10.0
            i += 1
        pi2a = xp.getTree('General_Info', 'Processing_Info')
        self._processing_centre = pi2a.PROCESSING_CENTER.text
        pi2a = xp.getTree('General_Info', 'Archiving_Info')
        self._archiving_centre = pi2a.ARCHIVING_CENTRE.text
        return

    def updateTiles(self):
        L1C_TILES = sorted(os.listdir(os.path.join(self.L1C_UP_DIR, 'GRANULE')))
        if not L1C_TILES:
            return False
        if self.namingConvention == 'SAFE_STANDARD':
            filemask = 'S2?_OPER_MSI_L1C_TL_*'
        else:
            filemask = 'L1C_*'

        L2A_TILES = []
        for tile in L1C_TILES:
            if not fnmatch.fnmatch(tile, filemask):
                continue
            self.tile = tile
            if self.create_L2A_Tile():
                if not self.L2A_TILE_MTD_XML:
                    self.logger.error(
                        'No metadata in tile, remove the corrupt Level 2A product and start from scratch, system will terminate now.')
                    return False

        L2A_TILES.append(os.path.join(self.L2A_UP_DIR, 'GRANULE', self.L2A_TILE_ID))
        return L2A_TILES

    def getTileId1c(self):
        L1C_TILES = sorted(os.listdir(os.path.join(self.input_dir, 'GRANULE')))
        if not L1C_TILES:
            return False
        if self.namingConvention == 'SAFE_STANDARD':
            filemask = 'S2?_OPER_MSI_L1C_TL_*'
        else:
            filemask = 'L1C_*'

        for tile in L1C_TILES:
            if not fnmatch.fnmatch(tile, filemask):
                continue

        return tile

    def create_L2A_Tile(self):
        self.getEntriesFromDatastrip()
        if self.L2A_TILE_ID:
            self.logger.info('output tile already exists, no creation required')
            return None

        tile1cS = self.tile.replace('__','_')
        tile1cS = tile1cS.split('_')
        ds2aS = self.L2A_DS_ID.replace('__','_')
        ds2aS = ds2aS.split('_')
        pbStr = 'N%05.2f' % self.processingBaseline
        processingCentre = self.processing_centre
        creationDate = ds2aS[2]
        sensingStart = ds2aS[3]

        if self.operationMode == 'TOOLBOX':
            spacecraftName = 'S' + self.spacecraftName.split('-')[-1]
            fileTypeDs = 'OPER_MSI_L2A_DS'
            fileTypeTl = spacecraftName+'_OPER_MSI_L2A_TL'
            L2A ='L2A'
            if self.namingConvention == 'SAFE_STANDARD':
                tileId = tile1cS[-2]
                absoluteOrbit = tile1cS[-3]
                tileDiscriminator = sensingStart[1:]
            elif self.namingConvention == 'SAFE_COMPACT':
                tileId = tile1cS[1]
                absoluteOrbit = tile1cS[2]
                tileDiscriminator = sensingStart[1:]

            L2A_TILE_ID = '_'.join([L2A, tileId, absoluteOrbit, tileDiscriminator])
            self._L2A_TILE_ID_long = '_'.join([fileTypeTl, processingCentre, creationDate, absoluteOrbit, tileId, pbStr])
            self._L2A_DS_ID_long = '_'.join([spacecraftName, fileTypeDs, processingCentre, creationDate, sensingStart, pbStr])

        elif self.operationMode == 'PROCESS_TILE':
            spacecraftName = 'S' + self.spacecraftName.split('-')[-1]
            fileTypeTl = spacecraftName+'_OPER_MSI_L2A_TL'
            if self.namingConvention == 'SAFE_STANDARD':
                creationDate = ds2aS[6]
                absoluteOrbit = tile1cS[7]
                tileId = tile1cS[8]
            elif self.namingConvention == 'SAFE_COMPACT':
                creationDate = ds2aS[6]
                absoluteOrbit = tile1cS[2]
                tileId = tile1cS[1]
                
            L2A_TILE_ID = '_'.join([fileTypeTl, processingCentre, creationDate, absoluteOrbit, tileId, pbStr])
            self._L2A_TILE_ID_long = L2A_TILE_ID
            self._L2A_DS_ID_long = self.L2A_DS_ID
        else:
            return None

        L1C_TILE_ID = self.tile
        self.L1C_TILE_ID = L1C_TILE_ID
        self.L2A_TILE_ID = L2A_TILE_ID
        self._L2A_TILE_ID_STD = None

        # set absolute path for local tile IDs:
        if self.operationMode == 'TOOLBOX':
            GRANULE='GRANULE'
            L1C_TILE_ID = os.path.join(self.L1C_UP_DIR, GRANULE, L1C_TILE_ID)
            L2A_TILE_ID = os.path.join(self.L2A_UP_DIR, GRANULE, L2A_TILE_ID)
        else:
            L1C_TILE_ID = os.path.join(self.input_dir, L1C_TILE_ID)
            L2A_TILE_ID = os.path.join(self.output_dir, L2A_TILE_ID)

        self.logger.info('L2A tile directory is: ' + L2A_TILE_ID)
        if (os.name == 'nt') and (self.namingConvention == 'SAFE_STANDARD'):
            # special treatment for windows for long pathnames:
            L1C_TILE_ID_ = u'\\'.join([u'\\\\?', L1C_TILE_ID])
            L2A_TILE_ID_ = u'\\'.join([u'\\\\?', L2A_TILE_ID])
            configFn = u'\\'.join([u'\\\\?', self.configFn])
        else:
            L1C_TILE_ID_ = L1C_TILE_ID
            L2A_TILE_ID_ = L2A_TILE_ID

        # create the L2A tile folder:
        if (os.path.exists(L2A_TILE_ID_)):
            self.logger.info('output tile already exists, no creation required')
            return None
        else:
            os.mkdir(L2A_TILE_ID_)
            chmod_recursive(L2A_TILE_ID_, 0755)

            # create the IMG_DATA folder:
            IMG_DATA = 'IMG_DATA'
            os.mkdir(os.path.join(L2A_TILE_ID_, IMG_DATA))

            # copy the TILE Metadata:
            filelist = sorted(os.listdir(L1C_TILE_ID_))
            found = False

            L1C_MTD_TL_MASK_STANDARD = 'S2?_OPER_MTD_L1C_TL_*.xml'
            L1C_MTD_TL_MASK_COMPACT = 'MTD_TL.xml'

            for filename in filelist:
                if (fnmatch.fnmatch(filename, L1C_MTD_TL_MASK_STANDARD) == True):
                    found = True
                    break
                elif (fnmatch.fnmatch(filename, L1C_MTD_TL_MASK_COMPACT) == True):
                    found = True
                    break
            if not found:
                self.logger.fatal('No metadata in tile')

            L1C_TILE_MTD_XML = os.path.join(L1C_TILE_ID_, filename)
            L2A_TILE_MTD_XML = 'MTD_TL.xml'
            L2A_TILE_MTD_XML = os.path.join(L2A_TILE_ID_, L2A_TILE_MTD_XML)
            copyfile(L1C_TILE_MTD_XML, L2A_TILE_MTD_XML)
            self.L1C_TILE_MTD_XML = L1C_TILE_MTD_XML
            self.L2A_TILE_MTD_XML = L2A_TILE_MTD_XML

            xp = L2A_XmlParser(self, 'T2A')
            if (xp.convert() == False):
                self.logger.fatal('error in converting tile metadata to level 2A')

            # write the tile IDs into the cPickle for later use:
            if self.operationMode == 'TOOLBOX':
                self.picFn = os.path.join(L2A_TILE_ID, 'config.pic')
            else:
                self.picFn = os.path.join(self.work_dir, self.L2A_TILE_ID + '.pic')
            logger = self.logger
            self.logger = None
            src = open(self.picFn, 'wb')
            pickle.dump(self, src, 2)
            self.logger = logger
        return True

    def set_L2A_DS_and_Tile_metadata(self):
        if (self._resolution == 10):
            return

        l.acquire
        xp = L2A_XmlParser(self, 'T2A')
        gi = xp.getRoot('General_Info')
        try:
            tid = xp.getTree('General_Info', 'TILE_ID')
            try:
                tid1c = xp.getTree('General_Info', 'L1C_TILE_ID')
                tid1c._setText(tid1c.text)
            except:
                tid1c = etree.Element('L1C_TILE_ID')
                tid1c.text = tid.text
                tid1c.attrib['metadataLevel'] = tid.attrib['metadataLevel']
                gi.insert(0, tid1c)
            tid._setText(self._L2A_TILE_ID_long)
            dsid = xp.getTree('General_Info', 'DATASTRIP_ID')
            dsid._setText(self._L2A_DS_ID_long)
            xp.export()
        except:
            self.logger.error(
                'Wrong operation mode or target product corrupt, change mode or remove target product and start again.\n')
        finally:
            l.release
        return

    def postprocess(self):

        xp = L2A_XmlParser(self, 'DS2A')
        ll = xp.getTree('Auxiliary_Data_Info', 'LUT_List')
        ll.LUT_FILENAME = self.atmDataFn.split('/')[-1]
        xp.export()
        xp.validate()

        if self.L2A_TILE_MTD_XML is not None:
            try:
                if self.operationMode == 'TOOLBOX':
                    self.updateAuxInfoPdgs('UP2A')
                    xp = L2A_XmlParser(self, 'UP2A')
                    ll = xp.getTree('Auxiliary_Data_Info', 'LUT_List')
                    ll.LUT_FILENAME = self.atmDataFn.split('/')[-1]
                    xp.export()
                    xp.validate()

                    xp = L2A_XmlParser(self, 'INSPIRE')
                    xp.convert()
                    # HTML parser must be implemented first, to work properly
                    # xp = L2A_XmlParser(self, 'HTML')
                    # xp.convertUpIndex()

                cPickle = self.picFn
                if self.resolution == 10:
                    try:
                        os.remove(cPickle)
                    except:
                        pass

                try:
                    dt = datetime.utcnow()
                    xp = L2A_XmlParser(self, 'T2A')
                    ai = xp.getTree('General_Info', 'Archiving_Info')
                    ai.ARCHIVING_TIME = strftime('%Y-%m-%dT%H:%M:%S.', dt.timetuple()) + str(dt.microsecond)[:3]+'Z'
                    ai.ARCHIVING_CENTRE = self.archiving_centre
                    xp.export()
                    xp = L2A_XmlParser(self, 'T2A')
                    xp.validate()
                except:
                    self.logger.fatal('read write error for tile metadata')
            except:
                return False

        tMeasure = time() - self._localTimestamp
        self.writeTimeEstimation(tMeasure)

        return True

    def updateAuxInfoPdgs(self, product):
        try:
            xp = L2A_XmlParser(self, product)
            if product == 'UP2A':
                pi = xp.getTree('General_Info', 'Product_Info')
            else:
                pi = xp.getTree('General_Info', 'Processing_Info')

            pv = '0' + self.processorVersion.replace('.','')
            pb = '{:05.2f}'.format(self.processingBaseline)
            pi.PROCESSING_BASELINE = pb
            l2aTileId = self.L2A_TILE_ID

            gipp = xp.getTree('Auxiliary_Data_Info', 'GIPP_List')
            if not gipp.find("GIPP_FILENAME[@type='GIP_L2ACSC']"):
                if self.configSC:
                    gippFn = etree.Element('GIPP_FILENAME', type='GIP_L2ACSC', version=pv)
                    sc = os.path.basename(self.configSC)
                    gippFn.text = os.path.splitext(sc)[0]
                    gipp.append(gippFn)
                else:
                    gippFn = etree.Element('GIPP_FILENAME', type='GIP_L2ACSC', version=pv)
                    gippFn.text = l2aTileId + '_L2A_CAL_SC'
                    gipp.append(gippFn)

            if not gipp.find("GIPP_FILENAME[@type='GIP_L2ACAC']"):
                if self.configSC:
                    gippFn = etree.Element('GIPP_FILENAME', type='GIP_L2ACAC', version=pv)
                    ac = os.path.basename(self.configAC)
                    gippFn.text = os.path.splitext(ac)[0]
                    gipp.append(gippFn)
                else:
                    gippFn = etree.Element('GIPP_FILENAME', type='GIP_L2ACAC', version=pv)
                    gippFn.text = l2aTileId + '_L2A_CAL_AC'
                    gipp.append(gippFn)

            if not gipp.find("GIPP_FILENAME[@type='GIP_PROBA2']"):
                gippFn = etree.Element('GIPP_FILENAME', type='GIP_PROBA2', version=pb.replace('.', ''))

            if self.configPB:
                pb = os.path.basename(self.configPB)
                gippFn.text = os.path.splitext(pb)[0]
                gipp.append(gippFn)

            pdt = xp.getTree('Auxiliary_Data_Info', 'PRODUCTION_DEM_TYPE')
            if self.demDirectory == 'NONE':
                pdt._setText('None')
            else:
                pdt._setText(self.demReference)
            xp.export()
            return True
        except:
            return False

    def setTimeEstimation(self, resolution):
        if self.selectedTile is not None:
            nrTiles = 1
        else:
            nrTiles = self.nrTiles

        factor = float32(nrTiles)

        config = ConfigParser.RawConfigParser(allow_no_value=True)
        l.acquire()
        try:
            config.read(self._processingEstimationFn)
            tEst60 = config.getfloat('time estimation', 't_est_60') * factor
            tEst20 = config.getfloat('time estimation', 't_est_20') * factor
            tEst10 = config.getfloat('time estimation', 't_est_10') * factor
        finally:
            l.release()

        if (resolution == 60):
            self._tEstimation = tEst60
        elif (resolution == 20):
            self._tEstimation = tEst20
        elif (resolution == 10):
            self._tEstimation = (tEst20 + tEst10)
        else:
            self._tEstimation = (tEst60 + tEst20 + tEst10)
        return

    def writeTimeEstimation(self, tMeasure):
        l.acquire()
        try:
            config = ConfigParser.RawConfigParser()
            config.read(self._processingEstimationFn)

            if (self.resolution == 60):
                tEst = config.getfloat('time estimation', 't_est_60')
                tMeasureAsString = str((tEst + tMeasure) / 2.0)
                config.set('time estimation', 't_est_60', tMeasureAsString)

            elif (self.resolution == 20):
                tEst = config.getfloat('time estimation', 't_est_20')
                tMeasureAsString = str((tEst + tMeasure) / 2.0)
                config.set('time estimation', 't_est_20', tMeasureAsString)

            elif (self.resolution == 10):
                tEst = config.getfloat('time estimation', 't_est_10')
                tMeasureAsString = str((tEst + tMeasure) / 2.0)
                config.set('time estimation', 't_est_10', tMeasureAsString)

            configFile = open(self._processingEstimationFn, 'w')
            config.write(configFile)
            configFile.close()
        finally:
            l.release()

    def timestamp(self, procedure):
        import multiprocessing
        p = multiprocessing.current_process()
        l.acquire()
        try:
            tNow = datetime.utcnow()
            tDelta = tNow - self._timestamp
            tTotalDelta = tNow - self._processingStartTimestamp
            self._timestamp = tNow
            if (self.logger.getEffectiveLevel() != logging.NOTSET):
                self.logger.info('Procedure: ' + procedure + ', elapsed time[s]: %0.3f, total: %s' % (tDelta.total_seconds(), tTotalDelta))

            f = open(self._processingStatusFn, 'r')
            tTotal = float(f.readline()) * 0.01
            f.close()
            increment = tDelta.total_seconds() / self.tEstimation
            tTotal += increment
            if tTotal > 1.0:
                tWeighted = 100.0 - exp(-tTotal)
            elif tTotal > 0.98:
                tWeighted = tTotal * 100.0 - exp(-tTotal)
            else:
                tWeighted = tTotal * 100.0
            
            self.logger.stream('Progress[%%]: %03.2f : PID-%d, %s, elapsed time[s]: %0.3f, total: %s' % (tWeighted,p.pid, procedure, tDelta.total_seconds(), tTotalDelta))

            f = open(self._processingStatusFn, 'w')
            f.write(str(tWeighted) + '\n')
            f.close()
        except:
            f = open(self._processingStatusFn, 'w')
            f.write('0.0\n')
            f.close()
        finally:
            l.release()
        return

    def parNotFound(self, parameter):
        basename = os.path.basename(self._configFn)
        self.logger.fatal('Configuration parameter <%s> not found in %s' % (parameter, basename))
        return False

    def readPreferences(self):
        DATASTRIP = 'DATASTRIP'

        ### Reading the datastrip ID and the Satellite-Name:
        try:
            scriptDir = os.environ['SEN2COR_BIN']
        except:
            scriptDir = getScriptDir()
        libDir = os.path.join(scriptDir, 'lib')

        # search L1C datastrip ID to SAFE_COMPACT or SAFE_STANDARD format:
        found = False
        if self.namingConvention == 'SAFE_STANDARD':
            DS_mask = 'S2?_*_DS_*'
            DS_MTD_mask = 'S2?_*.xml'
        else:
            DS_mask = 'DS_*'
            DS_MTD_mask = 'MTD_DS.xml'

        if self.operationMode == 'GENERATE_DATASTRIP':
            dsDir = os.path.join(self.datastrip_root_folder)
            self.L1C_DS_DIR, self.L1C_DS_ID = os.path.split(dsDir)
            fileList = sorted(os.listdir(dsDir))
            for filename in fileList:
                if (fnmatch.fnmatch(filename, DS_MTD_mask) == True):
                    self.L1C_DS_MTD_XML = os.path.join(dsDir, filename)
                    xp = L2A_XmlParser(self, 'DS1C')
                    xp.validate()
                    di = xp.getTree('General_Info', 'Datatake_Info')
                    self.spacecraftName = di.SPACECRAFT_NAME.text
                    libDir = libDir + '_S' + self.spacecraftName[-2:]
                    found = True
                    break
            if not found:
                self.logger.fatal('No datastrip found.')
                return False
            #return True

        elif self.operationMode == 'PROCESS_TILE':
            test = os.path.join(self.datastrip_root_folder, 'MTD_DS.xml')
            if os.path.isfile(test):
                self.L2A_DS_MTD_XML = test
                xp = L2A_XmlParser(self, 'DS2A')
                xp.validate()
                di = xp.getTree('General_Info', 'Datatake_Info')
                self.spacecraftName = di.SPACECRAFT_NAME.text
                libDir = libDir + '_S' + self.spacecraftName[-2:]
                found = True

        elif self.operationMode == 'TOOLBOX':
            self.L1C_DS_DIR = os.path.join(self.input_dir, DATASTRIP)
            dirlist = sorted(os.listdir(self.L1C_DS_DIR))
            for dirname in dirlist:
                if (fnmatch.fnmatch(dirname, DS_mask) == True):
                    self.L1C_DS_ID = dirname
                    found = True
                    break
            if not found:
                self.logger.fatal('No datastrip found.')
                return False
            else:
                dsDir = os.path.join(self.L1C_DS_DIR,self.L1C_DS_ID)
                fileList = sorted(os.listdir(dsDir))
                for filename in fileList:
                    if (fnmatch.fnmatch(filename, DS_MTD_mask) == True):
                        self.L1C_DS_MTD_XML = os.path.join(dsDir, filename)
                        xp = L2A_XmlParser(self, 'DS1C')
                        if self._productVersion > float32(14.2):
                            xp.validate()
                        di = xp.getTree('General_Info', 'Datatake_Info')
                        self.spacecraftName = di.SPACECRAFT_NAME.text
                        libDir = libDir + '_S' + self.spacecraftName[-2:]
                        found = True
                        break
        if not found:
            self.logger.fatal('No metadata in datastrip.')
            return False

        self.logger.info('input product origins from: ' + self.spacecraftName)
        self.auxDir = os.path.join(scriptDir, 'aux_data')
        # SIIMPC-889-1, end
        if self._resolution == 10:
            self.libDir = os.path.join(libDir, '10')
        else:
            self.libDir = os.path.join(libDir, '20_60')

        ### Classificators
        ####READING FROM L2A_CAL_SC_GIPP.xml
        xp = L2A_XmlParser(self, 'SC_GIPP')
        xp.validate()

        ### Snow_map_reference
        node = xp.getTree('Scene_Classification', 'References')

        par = node.Snow_Map
        if par is None: self.parNotFound(node)
        self.snowMapReference = par.text

        ### ESA_CCI_WaterBodies_map_reference
        par = node.ESACCI_WaterBodies_Map
        if par is None: self.parNotFound(node)
        self.esacciWaterBodiesReference = par.text

        ### ESA_CCI_LandCover_map_reference
        par = node.ESACCI_LandCover_Map
        if par is None: self.parNotFound(node)
        self.esacciLandCoverReference = par.text

        ### ESA_CCI_SnowCondition_map_directory_reference
        par = node.ESACCI_SnowCondition_Map_Dir
        if par is None: self.parNotFound(node)
        self.esacciSnowConditionDirReference = par.text

        node = xp.getTree('Scene_Classification', 'Classificators')

        par = node.NO_DATA
        if par is None: self.parNotFound(node)
        self.noData = int32(par.pyval)

        par = node.SATURATED_DEFECTIVE
        if par is None: self.parNotFound(node)
        self.saturatedDefective = int32(par.pyval)

        par = node.DARK_FEATURES
        if par is None: self.parNotFound(node)
        self.darkFeatures = int32(par.pyval)

        par = node.CLOUD_SHADOWS
        if par is None: self.parNotFound(node)
        self.cloudShadows = int32(par.pyval)

        par = node.VEGETATION
        if par is None: self.parNotFound(node)
        self.vegetation = int32(par.pyval)

        par = node.NOT_VEGETATED
        if par is None: self.parNotFound(node)
        self.bareSoils = int32(par.pyval)

        par = node.WATER
        if par is None: self.parNotFound(node)
        self.water = int32(par.pyval)

        par = node.UNCLASSIFIED
        if par is None: self.parNotFound(node)
        self.lowProbaClouds = int32(par.pyval)

        par = node.MEDIUM_PROBA_CLOUDS
        if par is None: self.parNotFound(node)
        self.medProbaClouds = int32(par.pyval)

        par = node.HIGH_PROBA_CLOUDS
        if par is None: self.parNotFound(node)
        self.highProbaClouds = int32(par.pyval)

        par = node.THIN_CIRRUS
        if par is None: self.parNotFound(node)
        self.thinCirrus = int32(par.pyval)

        par = node.SNOW_ICE
        if par is None: self.parNotFound(node)
        self.snowIce = int32(par.pyval)

        ### Thresholds
        node = xp.getTree('Scene_Classification', 'Thresholds')

        par = node.T1_B02
        if par is None: self.parNotFound(node)
        self.T1_B02 = float32(par.pyval)

        par = node.T2_B02
        if par is None: self.parNotFound(node)
        self.T2_B02 = float32(par.pyval)

        par = node.T1_B04
        if par is None: self.parNotFound(node)
        self.T1_B04 = float32(par.pyval)

        par = node.T2_B04
        if par is None: self.parNotFound(node)
        self.T2_B04 = float32(par.pyval)

        par = node.T1_B8A
        if par is None: self.parNotFound(node)
        self.T1_B8A = float32(par.pyval)

        par = node.T2_B8A
        if par is None: self.parNotFound(node)
        self.T2_B8A = float32(par.pyval)

        par = node.T1_B10
        if par is None: self.parNotFound(node)
        self.T1_B10 = float32(par.pyval)

        par = node.T2_B10
        if par is None: self.parNotFound(node)
        self.T2_B10 = float32(par.pyval)

        par = node.T1_B12
        if par is None: self.parNotFound(node)
        self.T1_B12 = float32(par.pyval)

        par = node.T2_B12
        if par is None: self.parNotFound(node)
        self.T2_B12 = float32(par.pyval)

        par = node.T1_NDSI_CLD
        if par is None: self.parNotFound(node)
        self.T1_NDSI_CLD = float32(par.pyval)

        par = node.T2_NDSI_CLD
        if par is None: self.parNotFound(node)
        self.T2_NDSI_CLD = float32(par.pyval)

        par = node.T1_NDSI_SNW
        if par is None: self.parNotFound(node)
        self.T1_NDSI_SNW = float32(par.pyval)

        par = node.T2_NDSI_SNW
        if par is None: self.parNotFound(node)
        self.T2_NDSI_SNW = float32(par.pyval)

        par = node.T1_R_B02_B04
        if par is None: self.parNotFound(node)
        self.T1_R_B02_B04 = float32(par.pyval)

        par = node.T2_R_B02_B04
        if par is None: self.parNotFound(node)
        self.T2_R_B02_B04 = float32(par.pyval)

        par = node.T1_R_B8A_B03
        if par is None: self.parNotFound(node)
        self.T1_R_B8A_B03 = float32(par.pyval)

        par = node.T2_R_B8A_B03
        if par is None: self.parNotFound(node)
        self.T2_R_B8A_B03 = float32(par.pyval)

        par = node.T1_R_B8A_B11
        if par is None: self.parNotFound(node)
        self.T1_R_B8A_B11 = float32(par.pyval)

        par = node.T2_R_B8A_B11
        if par is None: self.parNotFound(node)
        self.T2_R_B8A_B11 = float32(par.pyval)

        par = node.T1_SNOW
        if par is None: self.parNotFound(node)
        self.T1_SNOW = float32(par.pyval)

        par = node.T2_SNOW
        if par is None: self.parNotFound(node)
        self.T2_SNOW = float32(par.pyval)

        par = node.T1_NDVI
        if par is None: self.parNotFound(node)
        self.T1_NDVI = float32(par.pyval)

        par = node.T2_NDVI
        if par is None: self.parNotFound(node)
        self.T2_NDVI = float32(par.pyval)

        par = node.T1_R_B8A_B03
        if par is None: self.parNotFound(node)
        self.T1_R_B8A_B03 = float32(par.pyval)

        par = node.T2_R_B8A_B03
        if par is None: self.parNotFound(node)
        self.T2_R_B8A_B03 = float32(par.pyval)

        par = node.T11_B02
        if par is None: self.parNotFound(node)
        self.T11_B02 = float32(par.pyval)

        par = node.T12_B02
        if par is None: self.parNotFound(node)
        self.T12_B02 = float32(par.pyval)

        par = node.T11_R_B02_B11
        if par is None: self.parNotFound(node)
        self.T11_R_B02_B11 = float32(par.pyval)

        par = node.T12_R_B02_B11
        if par is None: self.parNotFound(node)
        self.T12_R_B02_B11 = float32(par.pyval)

        par = node.T21_B12
        if par is None: self.parNotFound(node)
        self.T21_B12 = float32(par.pyval)

        par = node.T22_B12
        if par is None: self.parNotFound(node)
        self.T22_B12 = float32(par.pyval)

        par = node.T21_R_B02_B11
        if par is None: self.parNotFound(node)
        self.T21_R_B02_B11 = float32(par.pyval)

        par = node.T22_R_B02_B11
        if par is None: self.parNotFound(node)
        self.T22_R_B02_B11 = float32(par.pyval)

        par = node.T_CLOUD_LP
        if par is None: self.parNotFound(node)
        self.T_CLOUD_LP = float32(par.pyval)

        par = node.T_CLOUD_MP
        if par is None: self.parNotFound(node)
        self.T_CLOUD_MP = float32(par.pyval)

        par = node.T_CLOUD_HP
        if par is None: self.parNotFound(node)
        self.T_CLOUD_HP = float32(par.pyval)

        par = node.T1_B10
        if par is None: self.parNotFound(node)
        self.T1_B10 = float32(par.pyval)

        par = node.T2_B10
        if par is None: self.parNotFound(node)
        self.T2_B10 = float32(par.pyval)

        par = node.T_SDW
        if par is None: self.parNotFound(node)
        self.T_SDW = float32(par.pyval)

        par = node.T_B02_B12
        if par is None: self.parNotFound(node)
        self.T_B02_B12 = float32(par.pyval)

        ###READING FROM L2A_CAL_AC_GIPP.xml
        ### Scaling:
        xp = L2A_XmlParser(self, 'AC_GIPP')
        xp.validate()

        node = xp.getNode('Flags')
        try:
            par = node.Scaling_Disabler
            self._scaling_disabler = node.Scaling_Disabler.pyval
            par = node.Scaling_Limiter
            self._scaling_limiter = node.Scaling_Limiter.pyval
            # implementation of SIIMPC-557, UMW:
            par = node.Rho_Retrieval_Step2.pyval
            self.rho_retrieval_step2 = node.Rho_Retrieval_Step2.pyval
            # end implementation of SIIMPC-557
        except:
            self.parNotFound(par)

        node = xp.getNode('References')
        par = node.Lib_Dir.pyval
        if par is None: self.parNotFound(par)
        if self._resolution == 10:
            bandIndex = [1, 2, 3, 7]
            self._ch940 = [0, 0, 0, 0, 0, 0]
        else:
            bandIndex = [0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12]
            self._ch940 = [8, 8, 9, 9, 0, 0]

        sensor = xp.getNode('Sensor')
        try:
            par = sensor.Calibration.min_sc_blu
            self.min_sc_blu = par.pyval
            par = sensor.Calibration.max_sc_blu
            self.max_sc_blu = par.pyval
        except:
            self.parNotFound(par)

        wavelength = sensor.Calibration.Band_List.wavelength
        i = 0
        self._c0 = zeros(size(bandIndex), float32)
        self._c1 = zeros(size(bandIndex), float32)
        self._wvlsen = zeros(size(bandIndex), float32)
        for index in bandIndex:
            self._c0[i] = float32(wavelength[index].attrib['c0'])
            self._c1[i] = float32(wavelength[index].attrib['c1'])
            self._wvlsen[i] = float32(wavelength[index].text)
            i += 1

        i = 0
        self._fwhm = zeros(size(bandIndex), float32)
        for index in bandIndex:
            par = sensor.Resolution.Band_List.fwhm[index]
            if par is None: self.parNotFound(par)
            self._fwhm[i] = float32(par.pyval)
            i += 1
            ###
            ### New parameters for SEN2COR 2.3:
            ###
        node = xp.getNode('ACL_Prio_1')
        try:
            self._AC_Min_Ddv_Area = node.AC_Min_Ddv_Area.pyval  # OK
            self._AC_Swir_Refl_Lower_Th = node.AC_Swir_Refl_Lower_Th.pyval
            self._AC_Swir_22um_Red_Refl_Ratio = node.AC_Swir_22um_Red_Refl_Ratio.pyval  # OK
            self._AC_Red_Blue_Refl_Ratio = node.AC_Red_Blue_Refl_Ratio.pyval  # OK
            self._AC_Cut_Off_Aot_Iter_Vegetation = node.AC_Cut_Off_Aot_Iter_Vegetation.pyval
            self._AC_Cut_Off_Aot_Iter_Water = node.AC_Cut_Off_Aot_Iter_Water.pyval
            self._AC_Aerosol_Type_Ratio_Th = node.AC_Aerosol_Type_Ratio_Th.pyval
            self._AC_Topo_Corr_Th = node.AC_Topo_Corr_Th.pyval
            self._AC_Slope_Th = node.AC_Slope_Th.pyval
            self._AC_Dem_P2p_Val = node.AC_Dem_P2p_Val.pyval
        except:
            self.parNotFound(node)

        node = xp.getNode('ACL_Prio_2')
        try:
            self._AC_Swir_Refl_Ndvi_Th = node.AC_Swir_Refl_Ndvi_Th.pyval
            self._AC_Ddv_Swir_Refl_Th1 = node.AC_Ddv_Swir_Refl_Th1.pyval
            self._AC_Ddv_Swir_Refl_Th2 = node.AC_Ddv_Swir_Refl_Th2.pyval
            self._AC_Ddv_Swir_Refl_Th3 = node.AC_Ddv_Swir_Refl_Th3.pyval
            self._AC_Ddv_16um_Refl_Th1 = node.AC_Ddv_16um_Refl_Th1.pyval
            self._AC_Ddv_16um_Refl_Th2 = node.AC_Ddv_16um_Refl_Th2.pyval
            self._AC_Ddv_16um_Refl_Th3 = node.AC_Ddv_16um_Refl_Th3.pyval
            self._AC_Dbv_Nir_Refl_Th = node.AC_Dbv_Nir_Refl_Th.pyval
            self._AC_Dbv_Ndvi_Th = node.AC_Dbv_Ndvi_Th.pyval
            self._AC_Red_Ref_Refl_Th = node.AC_Red_Ref_Refl_Th.pyval
            self._AC_Dbv_Red_Veget_Tst_Ndvi_Th = node.AC_Dbv_Red_Veget_Tst_Ndvi_Th.pyval
            self._AC_Dbv_Red_Veget_Refl_Th = node.AC_Dbv_Red_Veget_Refl_Th.pyval
            self._AC_Wv_Iter_Start_Summer = node.AC_Wv_Iter_Start_Summer.pyval
            self._AC_Wv_Iter_Start_Winter = node.AC_Wv_Iter_Start_Winter.pyval
            self._AC_Rng_Nbhd_Terrain_Corr = node.AC_Rng_Nbhd_Terrain_Corr.pyval
            self._AC_Max_Nr_Topo_Iter = node.AC_Max_Nr_Topo_Iter.pyval
            self._AC_Topo_Corr_Cutoff = node.AC_Topo_Corr_Cutoff.pyval
            self._AC_Vegetation_Index_Th = node.AC_Vegetation_Index_Th.pyval
        except:
            self.parNotFound(node)

        node = xp.getNode('ACL_Prio_3')
        try:
            self._AC_Limit_Area_Path_Rad_Scale = node.AC_Limit_Area_Path_Rad_Scale.pyval
            self._AC_Ddv_Smooting_Window = node.AC_Ddv_Smooting_Window.pyval
            self._AC_Terrain_Refl_Start = node.AC_Terrain_Refl_Start.pyval
            self._AC_Spr_Refl_Percentage = node.AC_Spr_Refl_Percentage.pyval
            self._AC_Spr_Refl_Promille = node.AC_Spr_Refl_Promille.pyval
        except:
            self.parNotFound(node)

            # node = xp.getNode('ACL_Prio_4')
            # try:
            #     self._AC_Cloud_Refl_Th = node.AC_Cloud_Refl_Th.pyval
            #     self._AC_Vis_Inc_Aot_Iter = node.AC_Vis_Inc_Aot_Iter.pyval
            # except:
            #     self.parNotFound(node)

            #####READING FROM L2A_GIPP.xml
        xp = L2A_XmlParser(self, 'GIPP')
        # fix for SIIMPC-599, UMW
        # xp.export()
        xp.validate()

        ### Common_Section:
        node = xp.getNode('Common_Section')
        if node is None: self.parNotFound(node)

        par = node.Log_Level
        if par is None: self.parNotFound(par)
        self._logLevel = par.text

        par = node.Nr_Threads
        if par is None: self.parNotFound(par)
        self.nrThreads = par.pyval

        par = node.DEM_Directory
        if par is None: self.parNotFound(par)
        self.demDirectory = par.text

        par = node.DEM_Reference
        if par is None: self.parNotFound(par)
        self.demReference = par.text

        par = node.Generate_DEM_Output
        if par is None:
            self.parNotFound(par)
        elif par == 'TRUE':
            self.demOutput = True
        else:
            self.demOutput = False

        par = node.Generate_TCI_Output
        if par is None:
            self.parNotFound(par)
        elif par == 'TRUE':
            self.tciOutput = True
        else:
            self.tciOutput = False

        par = node.Generate_DDV_Output
        if par is None:
            self.parNotFound(par)
        elif par == 'TRUE':
            self.ddvOutput = True
        else:
            self.ddvOutput = False

        if self.downsample20to60 == True:
            par = node.Downsample_20_to_60
            if par is None:
                self.parNotFound(par)
            elif par == 'TRUE':
                self.downsample20to60 = True
            else:
                self.downsample20to60 = False

            ### Scene Classification:    
            ### Filters:
        node = xp.getTree('Scene_Classification', 'Filters')
        if node is None: self.parNotFound(node)

        par = node.Median_Filter
        if par is None: self.parNotFound(node)
        self.medianFilter = int(par.pyval)

        ### Atmospheric Correction:
        ### References:
        node = xp.getTree('Atmospheric_Correction', 'Look_Up_Tables')
        if node is None: self.parNotFound(node)

        if self.aerosolType is None:
            par = node.Aerosol_Type
            if par is None: self.parNotFound(par)
            self.aerosolType = par.text

        if self.midLatitude is None:
            par = node.Mid_Latitude
            if par is None: self.parNotFound(par)
            self.midLatitude = par.text

        if self.ozoneSetpoint is None:
            par = node.Ozone_Content
            if par is None: self.parNotFound(par)
            self.ozoneSetpoint = float32(par.pyval)

            ### Flags:
        node = xp.getTree('Atmospheric_Correction', 'Flags')
        if node is None: self.parNotFound(node)

        # SIIMPC-1019, enabled for release 2.6.7
        par = node.DEM_Terrain_Correction
        if par is None: self.parNotFound(par)
        value = par.pyval
        if value == 'TRUE':
            self.dem_terrain_correction = 1
        else:
            self.dem_terrain_correction = 0

        par = node.BRDF_Correction
        if par is None: self.parNotFound(par)
        self.ibrdf = int32(par.pyval)

        par = node.BRDF_Lower_Bound
        if par is None: self.parNotFound(par)
        self.thr_g = float32(par.pyval)

        par = node.WV_Correction
        if par is None: self.parNotFound(par)
        self.iwaterwv = int32(par.pyval)

        par = node.VIS_Update_Mode
        if par is None: self.parNotFound(par)
        self.npref = int32(par.pyval)

        par = node.WV_Watermask
        if par is None: self.parNotFound(par)
        self.iwv_watermask = int32(par.pyval)

        par = node.Cirrus_Correction
        if par is None: self.parNotFound(par)
        value = par.pyval
        if value == 'TRUE':
            self.icirrus = 1
        else:
            self.icirrus = 0

        ### Calibration:
        node = xp.getTree('Atmospheric_Correction', 'Calibration')
        if node is None: self.parNotFound(node)

        par = node.Adj_Km
        if par is None: self.parNotFound(par)
        self.adj_km = float32(par.pyval)

        par = node.Visibility
        if par is None: self.parNotFound(par)
        self.visibility = float32(par.pyval)

        par = node.Altitude
        if par is None: self.parNotFound(par)
        self.altit = float32(par.pyval)

        par = node.Smooth_WV_Map
        if par is None: self.parNotFound(par)
        self.smooth_wvmap = float32(par.pyval)
        if (self.smooth_wvmap < 0.0): self.smooth_wvmap = 0.0

        par = node.WV_Threshold_Cirrus
        if par is None: self.parNotFound(par)
        self.wv_thr_cirrus = clip(float32(par.pyval), 0.1, 1.0)

        par = node.Database_Compression_Level
        if par is None: self.parNotFound(par)
        self.db_compression_level = int32(par.pyval)

        return True


    def readTileMetadata(self):
        xp = L2A_XmlParser(self, 'T2A')
        # fix for SIIMPC-733, UMW: update acquisition date from metadata:
        st = xp.getTree('General_Info', 'SENSING_TIME')
        self.acquisitionDate = st.text
        # end fix for SIIMPC-733
        ang = xp.getTree('Geometric_Info', 'Tile_Angles')
        try:
            azimuthAnglesList = ang.Sun_Angles_Grid.Azimuth.Values_List.VALUES
            solaz_arr = self.getFloatArray(azimuthAnglesList)
            min = solaz_arr[solaz_arr > 0].min()
            solaz_arr[solaz_arr == 0] = min
        except:
            self.logger.warning('No azimuth angular values in tile metadata available, will be set to 0')
            solaz_arr = 0
        try:
            zenithAnglesList = ang.Sun_Angles_Grid.Zenith.Values_List.VALUES
            solze_arr = self.getFloatArray(zenithAnglesList)
            min = solze_arr[solze_arr > 0].min()
            solze_arr[solze_arr == 0] = min
        except:
            self.logger.warning('No zenith angular values in user metadata available, will be set to 0')
            solze_arr = 0
        # images may be not squared - this is the case for the current testdata used
        # angle arrays have to be adapted, otherwise the bilinear interpolation is misaligned.
        imgSizeList = xp.getTree('Geometric_Info', 'Tile_Geocoding')
        size = imgSizeList.Size
        sizelen = len(size)
        nrows = None
        ncols = None
        for i in range(sizelen):
            if int(size[i].attrib['resolution']) == self._resolution:
                nrows = int(size[i].NROWS)
                ncols = int(size[i].NCOLS)
                break

        if (nrows is None or ncols is None):
            self.logger.fatal('no image dimension in metadata specified, please correct')
        if (nrows < ncols):
            last_row = int(solaz_arr[0].size * float(nrows) / float(ncols) + 0.5)
            saa = solaz_arr[0:last_row, :]
            sza = solze_arr[0:last_row, :]
        elif (ncols < nrows):
            last_col = int(solaz_arr[1].size * float(ncols) / float(nrows) + 0.5)
            saa = solaz_arr[:, 0:last_col]
            sza = solze_arr[:, 0:last_col]
        else:
            saa = solaz_arr
            sza = solze_arr

        if (saa.max() < 0):
            saa *= -1
        self.solaz_arr = clip(saa, 0, 360.0)

        sza = absolute(sza)
        self.solze_arr = clip(sza, 0, 70.0)

        if self.TESTMODE:
            self.nrows = uint16(nrows * 0.1)
            self.ncols = uint16(ncols * 0.1)
        else:
            self.nrows = nrows
            self.ncols = ncols
        try:
            solze = float32(ang.Mean_Sun_Angle.ZENITH_ANGLE.text)
        except:
            self.logger.warning('No mean zenith angular values in tile metadata available, will be set to 0')
            solze = 0
        try:
            solaz = float32(ang.Mean_Sun_Angle.AZIMUTH_ANGLE.text)
        except:
            self.logger.warning('No mean azimuth angular values in tile metadata available, will be set to 0')
            solaz = 0

        self._solze = absolute(solze)
        if self.solze > 70.0:
            self.solze = 70.0

        if solaz < 0:
            solaz *= -1
        if solaz > 360.0:
            solaz = 360.0
        self.solaz = solaz

        #
        # ATCOR employs the Lamberts reflectance law and assumes a constant viewing angle per tile (sub-scene)
        # as this is not given, this is a workaround, which have to be improved in a future version
        #
        try:
            viewAnglesList = ang.Mean_Viewing_Incidence_Angle_List.Mean_Viewing_Incidence_Angle
            arrlen = len(viewAnglesList)
            vaa = zeros(arrlen, float32)
            vza = zeros(arrlen, float32)
            for i in range(arrlen):
                # implementation of SIIMPC-816-1, UMW:
                if viewAnglesList[i].AZIMUTH_ANGLE.text == 'NaN':
                    self.logger.warning('mean azimuth angles are NaN, will be set to 0')
                    vaa[i] = 0
                else:
                    vaa[i] = float32(viewAnglesList[i].AZIMUTH_ANGLE.text)
                # implementation of SIIMPC-816-2, UMW:
                if viewAnglesList[i].ZENITH_ANGLE.text == 'NaN':
                    self.logger.warning('mean zenith angles are NaN, will be set to 0')
                    vza[i] = 0
                else:
                    vza[i] = float32(viewAnglesList[i].ZENITH_ANGLE.text)
        except:
            self.logger.warning(
                'No Mean_Viewing_Incidence_Angle values in tile metadata available, will be set to default values')
            viewAnglesList = 0
            vaa = zeros([2, 2], float32)
            vza = zeros([2, 2], float32)

        _min = vaa.min()
        _max = vaa.max()
        if _min < 0: _min += 360
        if _max < 0: _max += 360
        vaa_arr = array([_min, _min, _max, _max])
        self.vaa_arr = vaa_arr.reshape(2, 2)

        _min = absolute(vza.min())
        _max = absolute(vza.max())
        if _min > 12.0: _min = 12.0
        if _max > 12.0: _max = 12.0
        vza_arr = array([_min, _min, _max, _max])
        self.vza_arr = vza_arr.reshape(2, 2)
        return

    def _get_subNodes(self, node, valtype):
        count = int(node.attrib['count'])
        if (valtype == 'int'):
            arr = zeros([count], int)
        elif (valtype == 'float'):
            arr = zeros([count], float32)
        else:
            self.logger.error('wrong type declatarion: ' + type)
            self.parNotFound('wrong type declatarion: ' + type)

        i = 0
        for sub in node:
            if (valtype == 'int'):
                arr[i] = int(sub.text)
            else:
                arr[i] = float32(sub.text)
            i += 1
        return arr

    def getIntArray(self, node):
        nrows = len(node)
        if nrows < 0:
            return False

        ncols = len(node[0].split())
        a = zeros([nrows, ncols], dtype=int)

        for i in range(nrows):
            a[i, :] = array(node[i].split(), dtype(int))

        return a

    def getUintArray(self, node):
        nrows = len(node)
        if nrows < 0:
            return False

        ncols = len(node[0].split())
        a = zeros([nrows, ncols], dtype=uint)

        for i in range(nrows):
            a[i, :] = array(node[i].split(), dtype(uint))

        return a

    def getFloatArray(self, node):
        nrows = len(node)
        if nrows < 0:
            return False

        ncols = len(node[0].text.split())
        a = zeros([nrows, ncols], dtype=float32)

        for i in range(nrows):
            a[i, :] = nan_to_num(array(node[i].text.split(), dtype(float32)))

        return a

    def putArrayAsStr(self, a, node):
        set_printoptions(precision=6)
        if a.ndim == 1:
            nrows = a.shape[0]
            for i in range(nrows):
                node[i] = a[i], dtype = str

        elif a.ndim == 2:
            nrows = a.shape[0]
            for i in range(nrows):
                aStr = array_str(a[i, :]).strip('[]')
                node[i] = aStr
            return True
        else:
            return False

    def getStringArray(self, node):
        nrows = len(node)
        if nrows < 0:
            return False

        ncols = len(node[0].text.split())
        a = zeros([nrows, ncols], dtype=str)

        for i in range(nrows):
            a[i, :] = array(node[i].text.split(), dtype(str))

        return a

    def _adapt(self, default, setpoint, theRange):
        setpoint *= 0.001  # convert to micron
        # check if valid range, allow a broader interval than default
        if (setpoint[0] < default[0] - theRange):
            default[0] -= theRange
            self.logger.info(
                'Adaptation of band interval. Setpoint: ' + str(setpoint[0] * 1000.0) + ', new value: ' + str(
                    default[0] * 1000.0))
        elif (setpoint[0] > default[1]):
            self.logger.info('Setpoint > upper limit, will be ignored! Setpoint: ' + str(
                setpoint[0] * 1000.0) + ', new value: ' + str(default[0] * 1000.0))
            pass
        else:
            default[0] = setpoint[0]

        if (setpoint[1] > default[1] + theRange):
            default[1] += theRange
            self.logger.info(
                'Adaptation of band interval. Setpoint: ' + str(setpoint[1] * 1000.0) + ', new value: ' + str(
                    default[1] * 1000.0))
        elif (setpoint[1] < default[0]):
            self.logger.info('Setpoint < lower limit, will be ignored! Setpoint: ' + str(
                setpoint[1] * 1000.0) + ', new value: ' + str(default[1] * 1000.0))
            pass
        else:
            default[1] = setpoint[1]
        return default

    def _getDoc(self):
        from xml.etree import ElementTree as ET
        try:
            tree = ET.parse(self.configFn)
        except Exception, inst:
            self.logger.exception("Unexpected error opening %s: %s", self.configFn, inst)
            self.logger.fatal('Error in XML document')
        doc = tree.getNode()
        return doc

    def getInt(self, label, key):
        doc = self._getDoc()
        parameter = label + '/' + key
        par = doc.find(parameter)
        if par is None: self.parNotFound(parameter)
        return int32(par.pyval)

    def getFloat(self, label, key):
        doc = self._getDoc()
        parameter = label + '/' + key
        par = doc.find(parameter)
        if par is None: self.parNotFound(parameter)
        return float32(par.pyval)

    def getStr(self, label, key):
        doc = self._getDoc()
        parameter = label + '/' + key
        par = doc.find(parameter)
        if par is None: self.parNotFound(parameter)
        return par.text

        # implementation of SIIMPC-828, UMW: moved from L2A_Tables:

    def createAtmDataFilename(self):
        extension = '.atm'
        height = '99000_'
        if self.midLatitude == 'SUMMER':
            waterVapour = 'wv20_'
        elif self.midLatitude == 'WINTER':
            waterVapour = 'wv04_'
        elif self.midLatitude == 'AUTO':
            waterVapour = 'wv00_'
        else:
            waterVapour = 'wv20_'  # default

        par = self.aerosolType
        if par == 'RURAL':
            aerosolType = 'rura'
        elif par == 'MARITIME':
            aerosolType = 'mari'
        elif par == 'AUTO':
            aerosolType = 'auto'
        else:
            aerosolType = 'rura'  # default

        delta = self.assignOzoneContent()
        self.logger.info(
            'Ozone_Content is set to %s with %f least difference to input value' % (self.ozoneContent, delta))

        atmDataFn = self.ozoneContent + height + waterVapour + aerosolType + extension
        self.logger.info('generated file name for look up tables is: ' + atmDataFn)
        self.atmDataFn = os.path.join(self.libDir, atmDataFn)
        # implementation of SIIMPC-889-2, UMW: automatic detection and switch of LUT:
        try:
            os.stat(self.atmDataFn)
            self.logger.info('look up table for %s found and used' % self._satelliteId)
            return
        except:
            self.logger.warning('no specific look up table for S2B found, default one will be used instead')
            self.atmDataFn = self.atmDataFn.replace('S2B', 'S2A')
        try:
            os.stat(self.atmDataFn)
            self.logger.info('look up table for S2A found and used')
            return
        except:
            self.logger.fatal('look up table not found: ' + self.atmDataFn)
            return
            # end of implementation SIIMPC-889-2

    # implementation of SIIMPC-828, UMW: moved from L2A_Tables:
    def assignOzoneContent(self):
        # get the ozone value from metadata:
        columns = None
        ozoneSetpoint = self.ozoneSetpoint
        if self.midLatitude == 'SUMMER':
            columns = {
                "f": abs(250 - ozoneSetpoint),
                "g": abs(290 - ozoneSetpoint),
                "h": abs(331 - ozoneSetpoint),
                "i": abs(370 - ozoneSetpoint),
                "j": abs(410 - ozoneSetpoint),
                "k": abs(450 - ozoneSetpoint)
            }
        elif self.midLatitude == 'WINTER':
            columns = {
                "t": abs(250 - ozoneSetpoint),
                "u": abs(290 - ozoneSetpoint),
                "v": abs(330 - ozoneSetpoint),
                "w": abs(377 - ozoneSetpoint),
                "x": abs(420 - ozoneSetpoint),
                "y": abs(460 - ozoneSetpoint)
            }

        self.ozoneContent = min(columns, key=columns.get)
        delta = columns[self.ozoneContent]
        return delta

    # implementation of SIIMPC-828, UMW: moved from L2A_Tables:
    def setOzoneContentFromMetadata(self, ozoneSetpoint):
        if ozoneSetpoint:
            self.ozoneSetpoint = ozoneSetpoint
            self.logger.info('ozone mean value is: ' + str(self.ozoneSetpoint))
        else:
            if self.midLatitude == 'SUMMER':
                self.logger.info('no ozone data present, standard mid summer will be used')
                self.ozoneContent = 'h'
            elif self.midLatitude == 'WINTER':
                self.logger.info('no ozone data present, standard mid winter will be used')
                self.ozoneContent = 'w'
            else:
                self.logger.info(
                    'no ozone data present and no mid latitude configured, standard mid summer will be used')
                self.ozoneContent = 'h'

        if self.aerosolType != 'AUTO':
            self.createAtmDataFilename()
        return

    # implementation of SIIMPC-828, UMW: get mid latitude from geoposition and date:
    def setMidLatitude(self):
        if self.midLatitude in ['SUMMER', 'WINTER']:
            return
        # else:
        xp = L2A_XmlParser(self, 'T2A')
        tg = xp.getTree('Geometric_Info', 'Tile_Geocoding')
        nrows = self.nrows
        ncols = self.ncols
        idx = getResolutionIndex(self.resolution)
        ulx = tg.Geoposition[idx].ULX
        uly = tg.Geoposition[idx].ULY
        res = float32(self.resolution)
        geoTransformation = [ulx, res, 0.0, uly, 0.0, -res]
        extent = GetExtent(geoTransformation, ncols, nrows)
        xy = asarray(extent)
        hcsName = tg.HORIZONTAL_CS_NAME.text
        zone = hcsName.split()[4]
        zone1 = int(zone[:-1])
        zone2 = zone[-1:].upper()
        dummy, latMin, dummy = transform_utm_to_wgs84(xy[1, 0], xy[1, 1], zone1, zone2)
        dummy, latMax, dummy = transform_utm_to_wgs84(xy[3, 0], xy[3, 1], zone1, zone2)

        lat_cen = int((latMax + latMin) / 2)
        doy, isLeap = getDayOfYear(self._acquisitionDate)
        if not isLeap:
            Apr1 = 91  # 1. April
            Oct1 = 274  # 1. October
        else:
            Apr1 = 92  # 1. April
            Oct1 = 275  # 1. October

        self.logger.info('center of latitude is: %d. Day of year is: %d' % (lat_cen, doy))

        # for Tropical/Equatorial areas ( latitude [-30:30] ):
        if -30 <= lat_cen < 30:
            self.midLatitude = 'SUMMER'
        # for Northern Hemisphere( latitude [ 30:90] ):
        elif 30 <= lat_cen < 90:
            if Apr1 <= doy < Oct1:
                self.midLatitude = 'SUMMER'
            else:
                self.midLatitude = 'WINTER'
        # for Southern Hemisphere( latitude [-90:-30]):
        elif -90 <= lat_cen < -30:
            if Apr1 <= doy < Oct1:
                self.midLatitude = 'WINTER'
            else:
                self.midLatitude = 'SUMMER'
        self.logger.info('mid latitude set to %s according to area and date' % (self.midLatitude))
        return

    def setProductVersion(self):
        if not self.logger:
            from L2A_Logger import L2A_Logger
            self.logger = L2A_Logger('sen2cor',operation_mode=self.operationMode)
        # get the product version from datastrip metadata:
        if self.operationMode == 'TOOLBOX':
            path = os.path.join(self.input_dir, 'DATASTRIP')
            if self.namingConvention == 'SAFE_STANDARD':
                filemask = 'S2?_????_MTD_L1C_DS_*.xml'
            else:
                filemask = 'MTD_DS.xml'

        elif self.operationMode == 'GENERATE_DATASTRIP':
            path = os.path.join(self.datastrip_root_folder)
            if self.namingConvention == 'SAFE_STANDARD':
                filemask = 'S2?_????_MTD_L1C_DS_*.xml'
            else:
                filemask = 'MTD_DS.xml'

        elif self.operationMode == 'PROCESS_TILE':
            path = os.path.join(self.datastrip_root_folder)
            filemask = 'MTD_DS.xml'

        processingBaseline = productVersion = None

        for rt, dirs, files in os.walk(path):
            for name in files:
                if (fnmatch.fnmatch(name, filemask) == True):
                    try:
                        doc = objectify.parse(os.path.join(rt, name))
                        root = doc.getroot()
                        url = root.nsmap['n1']
                        productVersion = int(url[12:14])
                        if self.operationMode == 'PROCESS_TILE':
                            # processing baseline can be read from the DS metadata,
                            # as datastrip has been processed before:
                            tree = root['General_Info']
                            pi = tree['{}' + 'Processing_Info']
                            processingBaseline = float32(pi.PROCESSING_BASELINE.pyval)
                        if (productVersion == int(12)):
                            self.logger.info('Sen2Cor '+ self.processorVersion + \
                            ': Product version is below 14.5, will be converted to 14.5.')
                            productVersion = float32(14.2)
                        elif (productVersion == int(14)):
                            productVersion = float32(14.5)
                        elif productVersion > float32(14.5):
                            self.logger.warn('Product version %04.1f is not implemented yet.' % (productVersion))
                            self.logger.warn('Version 14.5 will be used but warnings during validation might occur.\n')
                            productVersion = float32(14.5)
                    except:
                        self.logger.warn('Product version cannot be read.')
                        self.logger.warn('14.5 will be used by default, but warnings during validation might occur.\n')
                    if not self.processingBaseline:
                        xp = L2A_XmlParser(self, 'PB_GIPP')
                        try:
                            xp.validate()
                            node = xp.getNode('DATA')
                            self.processingBaseline = float32(node.Baseline_Version.pyval)
                        except: # baseline is known from datastrip, if PDGS mode:
                            if processingBaseline:
                                self.processingBaseline = processingBaseline
                            else:
                                self.logger.error('Processing baseline not present or cannot be read.')
                                return False
                    if not productVersion:
                        self.logger.error('Sen2Cor ' + self.processorVersion + ': Product version cannot be read.')
                        return False
                    self.productVersion = productVersion

                    return True

        self.logger.error('Product metadata file cannot be read.\n')
        return False

    def setDatastripMetadataReference(self):
        test = os.path.join(self.datastrip_root_folder, 'MTD_DS.xml')
        if os.path.isfile(test):
            self._L2A_DS_MTD_XML = test
        else:
            self.logger.fatal('No metadata in datastrip.')
            return False

    def setTileMetadataReference(self):
        test = os.path.join(self.output_dir, self.tile.replace('L1C','L2A'), 'MTD_TL.xml')
        if os.path.isfile(test):
            self._L2A_TILE_MTD_XML = test
        else:
            self.logger.fatal('No metadata in tile.')
            return False

    def getDatatakeSensingStart(self):
        xp = L2A_XmlParser(self, 'DS2A')
        di = xp.getTree('General_Info', 'Datatake_Info')
        dsstxt = di.DATATAKE_SENSING_START.text[:-5]
        if dsstxt:
            dsstxt = dsstxt.replace('-','')
            dsstxt = dsstxt.replace(':','')
            return dsstxt
        return None

    def test_L2A_Tiles(self):
        try:
            granuleDir = os.path.join(self.L2A_UP_DIR, 'GRANULE')
            filelist = sorted(os.listdir(granuleDir))
            if len(filelist) > 0:
                return True
            else:
                return False
        except:
            return False

    def get_geobox(self):
        if self.resolution == 10:
            return self._geobox_10
        elif self.resolution == 20:
            return self._geobox_20
        elif self.resolution == 60:
            return self._geobox_60

    def set_geobox(self, geobox, resolution):
        if resolution == 10:
            self._geobox_10 = geobox
        elif resolution == 20:
            self._geobox_20 = geobox
        elif resolution == 60:
            self._geobox_60 = geobox

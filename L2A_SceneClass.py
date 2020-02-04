#!/usr/bin/env python
import gc
from numpy import *
from scipy.ndimage.morphology import *
from scipy.ndimage.interpolation import *
from scipy.ndimage.filters import median_filter, gaussian_filter
from scipy import ndimage
from lxml import objectify
from L2A_Library import *
from L2A_XmlParser import L2A_XmlParser
from multiprocessing import Lock
import cPickle as pickle
import os

l = Lock()
set_printoptions(precision = 7, suppress = True)

class L2A_SceneClass(object):
    def __init__(self, config, tables):
        self._notClassified = 100
        self._notSnow = 50
        self._config = config
        self._tables = tables
        self._logger = config.logger
        x = self.config.nrows
        y = self.config.ncols
        self.classificationMask = ones([x,y], uint16) * self._notClassified
        self.confidenceMaskSnow = zeros_like(tables.getBand(self.tables.B02))
        self.confidenceMaskCloud = zeros_like(tables.getBand(self.tables.B02))
        self._meanShadowDistance = 0
        self.filter =  None
        self.LOWEST = 0.000001
        self._noData = self.config.noData
        self._saturatedDefective = self.config.saturatedDefective
        self._darkFeatures = self.config.darkFeatures
        self._cloudShadows = self.config.cloudShadows
        self._vegetation = self.config.vegetation
        self._bareSoils = self.config.bareSoils
        self._water = self.config.water
        self._lowProbaClouds = self.config.lowProbaClouds
        self._medProbaClouds = self.config.medProbaClouds
        self._highProbaClouds = self.config.highProbaClouds
        self._thinCirrus = self.config.thinCirrus
        self._snowIce = self.config.snowIce
        self._cloudCoverage = 12
        self.logger.debug('Module L2A_SceneClass initialized')
        self._processingStatus = True
        self._sumPercentage = 0.0

    def assignClassifcation(self, arr, treshold, classification):
        cm = self.classificationMask
        cm[(arr == treshold) & (cm == self._notClassified)] = classification
        self.confidenceMaskCloud[(cm == classification)] = 0
        return

    def get_logger(self):
        return self._logger

    def set_logger(self, value):
        self._logger = value

    def del_logger(self):
        del self._logger
        
    def get_config(self):
        return self._config

    def get_tables(self):
        return self._tables

    def set_config(self, value):
        self._config = value

    def set_tables(self, value):
        self._tables = value

    def del_config(self):
        del self._config

    def del_tables(self):
        del self._tables

    tables = property(get_tables, set_tables, del_tables, "tables's docstring")
    config = property(get_config, set_config, del_config, "config's docstring")
    logger = property(get_logger, set_logger, del_logger, "logger's docstring")

    def preprocess(self):
        # fix for SIIMPC-1006.1 UMW:
        bandIndex = self.tables.bandIndex
        for i in bandIndex:
            band = self.tables.getBand(i)
            self.classificationMask[band == 0] = self._noData
        if self.classificationMask.max() == self._noData:
            self.logger.warning('All images contain only background pixels, output product will be created without atmospheric correction')
            return False
        return True

    def postprocess(self):
        if(self._processingStatus == False):
            return False
        CM = self.classificationMask
        CM[(CM == self._notClassified)] = self._lowProbaClouds # modification JL20151222
        value = self.config.medianFilter
        if(value > 0):
            CM = median_filter(CM, value)
            self.logger.info('Filtering output with level: ' + str(value))
        # SIIMPC-1392: QI datamasks should use the same no_data_mask as Scene Classification:
        # not included in release 2.8.0:
        # shall be enabled for release 2.9.0:
        #self.confidenceMaskSnow[CM == self._noData] = 0
        #self.confidenceMaskCloud[CM == self._noData] = 0
        self.logger.info('Storing final Classification Mask')
        self.tables.setBand(self.tables.SCL,(CM).astype(uint8))
        self.logger.info('Storing final Snow Confidence Mask')
        self.tables.setBand(self.tables.SNW,(self.confidenceMaskSnow*100+0.5).astype(uint8))
        self.logger.info('Storing final Cloud Confidence Mask')
        self.tables.setBand(self.tables.CLD,(self.confidenceMaskCloud*100+0.5).astype(uint8))
        try:
            if not self.tables.checkB2isPresent(20):
                # add L2A quality info on tile level:
                self.updateQualityIndicators(1, 'T2A')
                if self.config.operationMode == 'TOOLBOX':
                    self.updateQualityIndicators(1, 'UP2A')
        except:
            self.logger.error('error in updating quality indicators')

        picFn = self.config.picFn
        self.config.logger = None
        try:
            f = open(picFn, 'wb')
            pickle.dump(self.config, f, 2)
            f.close()
            self.config.logger = self.logger
        except:
            self.config.logger = self.logger
            self.logger.fatal('cannot update configuration' % picFn)
        return

    def postprocessnew(self):
        if(self._processingStatus == False):
            return False
        CM = self.classificationMask
        CM[(CM == self._notClassified)] = self._lowProbaClouds # modification JL20151222
        value = self.config.medianFilter
        if(value > 0):
            CM = median_filter(CM, value)
            self.logger.info('Filtering output with level: ' + str(value))

        # Soft dilatation of Cloud mask (120 m dilatation) 60m processing
        def filter_isolated_cells(image, struct):
            """ Return array with completely isolated single cells removed
            :param array: Array with completely isolated single cells
            :param struct: Structure array for generating unique regions
            :return: Array with minimum region size > 1
            """
            filtered_image = copy(image)
            id_regions, num_ids = ndimage.label(filtered_image, structure=struct)
            id_sizes = array(ndimage.sum(image, id_regions, range(num_ids + 1)))
            area_mask = (id_sizes == 1)
            filtered_image[area_mask[id_regions]] = 0

            return filtered_image

        rows, cols = shape(CM)
        CM_Clouds = uint8(zeros((rows, cols)))

        CM_Clouds[((CM == self._medProbaClouds) | (CM == self._highProbaClouds) | (CM == self._thinCirrus)) & (CM!=self._noData)] = 1
        CM_Clouds_filtered = filter_isolated_cells(CM_Clouds, struct=ones((3, 3)))
        # Dilatation square operator (5x5)
        CM_Clouds_filtered_dil = binary_dilation(CM_Clouds_filtered, ones((5, 5)))
        CM_Clouds_filtered_dil_med = median_filter(CM_Clouds_filtered_dil, 5)
        Diff_dil_med_original = CM_Clouds_filtered_dil_med - CM_Clouds
        CM[Diff_dil_med_original == 1] = self._medProbaClouds

        self.logger.info('Storing final Classification Mask')
        self.tables.setBand(self.tables.SCL,(CM).astype(uint8))
        self.logger.info('Storing final Snow Confidence Mask')
        self.tables.setBand(self.tables.SNW,(self.confidenceMaskSnow*100+0.5).astype(uint8))
        self.logger.info('Storing final Cloud Confidence Mask')
        self.tables.setBand(self.tables.CLD,(self.confidenceMaskCloud*100+0.5).astype(uint8))
        try:
            if not self.tables.checkB2isPresent(20):
                # add L2A quality info on tile level:
                self.updateQualityIndicators(1, 'T2A')
                # add L2A quality info on user level:
                self.updateQualityIndicators(1, 'UP2A')
        except:
            self.logger.error('error in updating quality indicators')

        picFn = self.config.picFn
        self.config.logger = None
        try:
            f = open(picFn, 'wb')
            pickle.dump(self.config, f, 2)
            f.close()
            self.config.logger = self.logger
        except:
            self.config.logger = self.logger
            self.logger.fatal('cannot update configuration' % picFn)
        return

    def __exit__(self):
        sys.exit(-1)

    def __del__(self):
        self.logger.info('Module L2A_SceneClass deleted')

    def L2A_CSND_1_1(self):
        # Step 1a: Brightness threshold on red (Band 4)
        T1_B04 = self.config.T1_B04
        T2_B04 = self.config.T2_B04
        T1_B08 = 0.04
        T2_B08 = 0.15
        T1 = 0.1 # Check influence of T1 with test B04<T1
        B04 = self.tables.getBand(self.tables.B04)
        B08 = self.tables.getBand(self.tables.B8A)
        self.confidenceMaskCloud = clip(B04, T1_B04, T2_B04)
        self.confidenceMaskCloud = ((self.confidenceMaskCloud - T1_B04)/(T2_B04-T1_B04))**2
        #JL20151217 self.confidenceMaskCloud = ((self.confidenceMaskCloud - T1_B04)/(T2_B04-T1_B04))
        CM = self.classificationMask
        CM[(B04<T1) & (B08>T1_B08) & (B08<T2_B08) & (CM==self._notClassified) & (CM!=self._noData)] = self._darkFeatures
        self.confidenceMaskCloud[(CM == self._darkFeatures)] = 0
        self.logger.debug(statistics(self.confidenceMaskCloud, 'CM Cloud step 1.1'))
        return

    def L2A_CSND_1_2(self):
        # Step 1b: Normalized Difference Snow Index (NDSI)
        T1_NDSI_CLD = self.config.T1_NDSI_CLD
        T2_NDSI_CLD = self.config.T2_NDSI_CLD
        #JL20151217 f1 = self.confidenceMaskCloud > 0
        B03 = self.tables.getBand(self.tables.B03)
        B11 = self.tables.getBand(self.tables.B11)
        NDSI = (B03 - B11) / maximum((B03 + B11), self.LOWEST)
        CMC = clip(NDSI, T1_NDSI_CLD, T2_NDSI_CLD)
        CMC = ((CMC - T1_NDSI_CLD)/(T2_NDSI_CLD-T1_NDSI_CLD))
        CM = self.classificationMask
        CM[(CMC==0) & (CM!=self._noData)] = self._notClassified
        self.confidenceMaskCloud *= CMC
        self.logger.debug(statistics(self.confidenceMaskCloud, 'CM Cloud step 1.2'))
        return

    def L2A_CSND_2_0(self):
        return

    def L2A_CSND_2_1(self):
        # Snow filter 1: Normalized Difference Snow Index (NDSI)
        T1_NDSI_SNW = self.config.T1_NDSI_SNW
        T2_NDSI_SNW = self.config.T2_NDSI_SNW
        B03 = self.tables.getBand(self.tables.B03)
        B11 = self.tables.getBand(self.tables.B11)
        NDSI = (B03 - B11) / maximum((B03 + B11), self.LOWEST)
        CMS = clip(NDSI, T1_NDSI_SNW, T2_NDSI_SNW)
        CMS = ((CMS - T1_NDSI_SNW)/(T2_NDSI_SNW-T1_NDSI_SNW))
        
        # JL20151217 snow filter applied only on potential clouds
        # JL20170809 usage of ESA CCI Snow Condition Map to exclude additional pixels
        CMC = self.confidenceMaskCloud
        CM = self.classificationMask
        CMS[(CMC == 0) & (CM != self._noData)] = 0  # exclude non potential cloud from snow probability

        if self.tables.hasBand(self.tables.SNC) == True:
            SNC = self.tables.getBand(self.tables.SNC)

            # exclude no snow pixels from ESA CCI Snow Condition Map
            # Disabled for now 2.6.3. In next versions exclude only non Tropical regions: lat != [-30, 30]
            # because SNC file not accurate enough for rare events, e.g. Snow in Washington DC 2018 March 22
            #CMS[(SNC == 0) & (CM != self._noData)] = 0

            # exclude water pixels from ESA CCI Snow Condition Map only if mean of no water pixel is < 10.0
            # (TBD if Water Bodies Map could be used here also to improve coastline definition)
            if SNC[SNC != 254].sum() > 0:  # Enter only if tile is not full of sea pixels
                if SNC[SNC != 254].mean() < 10.0:
                    CMS[(SNC == 254) & (CM != self._noData)] = 0
        # end JL20151217 snow filter applied only on potential clouds
        # end JL20170809 usage of ESA CCI Snow Condition Map to exclude additional pixels

        CM = self.classificationMask
        CM[(CMS == 0) & (CM == self._notClassified) & (CM!=self._noData)] = self._notSnow
        self.confidenceMaskSnow = CMS
        return

    def L2A_CSND_2_1bis(self):
        # New threshold using Band 5 and Band 8 to limit false snow detection
        T2_SNOW_R_B05_B8A = 0.85  # was 1.0. Updated because too much "frozen ground"was omitted 
        B05 = self.tables.getBand(self.tables.B05)
        B8A = self.tables.getBand(self.tables.B8A)
        Ratio_B05B8A = B05 / maximum(B8A, self.LOWEST)
        
        # Exclude potential snow pixels satisfying the condition         
        self.confidenceMaskSnow[(Ratio_B05B8A<T2_SNOW_R_B05_B8A)]= 0     
     
        CM = self.classificationMask
        CM[(self.confidenceMaskSnow == 0) & (CM == self._notClassified) & (CM!=self._noData)] = self._notSnow
        return

    def L2A_CSND_2_2(self):
        # Snow filter 2: Band 8 thresholds
        T1_B8A = self.config.T1_B8A
        T2_B8A = self.config.T2_B8A
        B8A = self.tables.getBand(self.tables.B8A)
        CMS = clip(B8A, T1_B8A, T2_B8A)
        CMS = ((CMS - T1_B8A) / (T2_B8A - T1_B8A))
        CM = self.classificationMask
        CM[(CMS == 0) & (CM == self._notClassified) & (CM!=self._noData)] = self._notSnow
        self.confidenceMaskSnow *= CMS
        return

    def L2A_CSND_2_3(self):
        # Snow filter 3: Band 2 thresholds
        T1_B02 = self.config.T1_B02
        T2_B02 = self.config.T2_B02
        B02 = self.tables.getBand(self.tables.B02)
        CMS = clip(B02, T1_B02, T2_B02)
        CMS = ((CMS - T1_B02) / (T2_B02 - T1_B02))
        CM = self.classificationMask
        CM[(CMS == 0) & (CM == self._notClassified) & (CM!=self._noData)] = self._notSnow
        self.confidenceMaskSnow *= CMS
        return

    def L2A_CSND_2_4(self):
        # Snow filter 4: Ratio Band 2 / Band 4
        T1_R_B02_B04 = self.config.T1_R_B02_B04
        T2_R_B02_B04 = self.config.T2_R_B02_B04
        B02 = self.tables.getBand(self.tables.B02)
        B04 = self.tables.getBand(self.tables.B04)
        RB02_B04 = B02 / maximum(B04,self.LOWEST)
        CMS = clip(RB02_B04, T1_R_B02_B04, T2_R_B02_B04)
        CMS = ((CMS - T1_R_B02_B04) / (T2_R_B02_B04 - T1_R_B02_B04))
        CM = self.classificationMask
        CM[(CMS == 0) & (CM == self._notClassified) & (CM!=self._noData)] = self._notSnow
        self.confidenceMaskSnow *= CMS
        CM = self.classificationMask
        return

    def L2A_CSND_2_5(self):
        # CHECK RING ALGORITHM THAT WAS NOT IMPLEMENTED BEFORE THIS VERSION.
        # Snow filter 5: snow boundary zones
        T1_SNOW = self.config.T1_SNOW
        T2_SNOW = self.config.T2_SNOW
        B12 = self.tables.getBand(self.tables.B12)
        CM = self.classificationMask
        CMS = self.confidenceMaskSnow
        snow_mask = (CMS >T1_SNOW) & (CM!=self._noData)
        CM[snow_mask] = self._snowIce
        # Dilatation cross-shape operator (5x5)
        struct = iterate_structure(generate_binary_structure(2,1), 3)
        snow_mask_dil = binary_dilation(snow_mask, struct)
        ring = snow_mask_dil ^ snow_mask
        ring_no_clouds = (ring &  (B12 < T2_SNOW))
        # important, if classified as snow, this should not become cloud:
        self.confidenceMaskCloud[ring_no_clouds | (CM == self._snowIce)] = 0
        # release the lock for the non snow classification
        CM[(CM == self._notSnow) & (CM!=self._noData)] = self._notClassified
        return

    def L2A_SnowPostProcessingCCI(self):
        if (self.tables.hasBand(self.tables.SNC) == True):
            SNC = self.tables.getBand(self.tables.SNC)
            CM = self.classificationMask

            # exclude no snow pixels from ESA CCI Snow Condition Map
            # Disabled for now 2.6.3. In next versions exclude only non Tropical regions: lat != [-30, 30]
            # because SNC file not accurate enough for rare events, e.g. Snow in Washington DC 2018 March 22
            #CM[(SNC == 0) & (CM == self._snowIce) & (CM!=self._noData)] = self._medProbaClouds    # no snow

            #CM[(SNC == 255) & (CM == self._snowIce) & (CM!=self._noData)] = self._medProbaClouds    # Nan values

	        # exclude water pixels from ESA CCI Snow Condition Map only if mean of no water pixel is < 10.0
            # (TBD if Water Bodies Map could be used here also to improve coastline definition)
            if SNC[SNC != 254].sum() > 0:  # Enter only if tile is not full of sea pixels
                if SNC[SNC != 254].mean() < 10.0:
                    CM[(SNC == 254) & (CM == self._snowIce) & (CM!=self._noData)] = self._medProbaClouds    # water
        return

    def L2A_CSND_3(self):
        # Step 3: Normalized Difference Vegetation Index (NDVI)
        T1_NDVI = self.config.T1_NDVI
        T2_NDVI = self.config.T2_NDVI
        T1_B2T = 0.15
        B02 = self.tables.getBand(self.tables.B02)
        B04 = self.tables.getBand(self.tables.B04)
        B8A = self.tables.getBand(self.tables.B8A)
        NDVI = (B8A - B04) / maximum((B8A + B04), self.LOWEST)
        CMC = clip(NDVI, T1_NDVI, T2_NDVI)
        CMC = ((CMC - T1_NDVI)/(T2_NDVI-T1_NDVI))
        CM = self.classificationMask
        CM[(CMC==1) & (CM == self._notClassified) & (B02 < T1_B2T) & (CM!=self._noData)] = self._vegetation
        CMC[(CM== self._vegetation)] = 0
        FLT = [(CMC>0) & (CMC < 1.0)]
        CMC[FLT] = CMC[FLT] * -1 + 1
        self.confidenceMaskCloud[FLT] *= CMC[FLT]
        self.logger.debug(statistics(self.confidenceMaskCloud, 'CM Cloud step 3'))
        return

    def L2A_CSND_4(self):
        # Step 4: Ratio Band 8 / Band 3 for senescing vegetation
        T1_R_B8A_B03 = self.config.T1_R_B8A_B03
        T2_R_B8A_B03 = self.config.T2_R_B8A_B03
        B03 = self.tables.getBand(self.tables.B03)
        B8A = self.tables.getBand(self.tables.B8A)
        rb8b3 = B8A/maximum(B03,self.LOWEST)
        CMC = clip(rb8b3, T1_R_B8A_B03 , T2_R_B8A_B03)
        CMC = (CMC - T1_R_B8A_B03 ) / (T2_R_B8A_B03 - T1_R_B8A_B03 )
        CM = self.classificationMask
        CM[(CMC==1) & (CM == self._notClassified) & (CM!=self._noData)] = self._vegetation
        CMC[(CM== self._vegetation)] = 0
        FLT = [(CMC>0) & (CMC < 1.0)]
        CMC[FLT] = CMC[FLT] * -1 + 1
        self.confidenceMaskCloud[FLT] *= CMC[FLT]
        self.logger.debug(statistics(self.confidenceMaskCloud, 'CM Cloud step 4'))
        return

    def L2A_CSND_5_1(self):
        # Step 5.1: Ratio Band 2 / Band 11 for soils
        T11_B02 = self.config.T11_B02 # -0.40
        T12_B02 = self.config.T12_B02 #  0.46
        T11_R_B02_B11 = self.config.T11_R_B02_B11 # 0.55 # 0.70
        T12_R_B02_B11 = self.config.T12_R_B02_B11 # 0.80 # 1.0
        B02 = self.tables.getBand(self.tables.B02)
        B11 = self.tables.getBand(self.tables.B11)
        R_B02_B11 = clip((B02/maximum(B11,self.LOWEST)),0,100)
        B02_FT = clip(R_B02_B11*T11_B02+T12_B02, 0.15, 0.32)        
        CM = self.classificationMask
        # Correction JL20151223: condition for bare_soils is on threshold T11_R_B02_B11
        CM[(B02 < B02_FT) & (R_B02_B11 < T11_R_B02_B11) & (CM == self._notClassified) & (CM!=self._noData)] = self._bareSoils
        self.confidenceMaskCloud[CM == self._bareSoils] = 0
        CMC = clip(R_B02_B11, T11_R_B02_B11, T12_R_B02_B11)
        CMC = ((CMC - T11_R_B02_B11)/(T12_R_B02_B11-T11_R_B02_B11))
        FLT = (R_B02_B11 > T11_R_B02_B11) & (R_B02_B11 < T12_R_B02_B11) & (B02 < B02_FT) & (CM == self._notClassified)
        self.confidenceMaskCloud[FLT] *= CMC[FLT]
        self.logger.debug(statistics(self.confidenceMaskCloud, 'CM Cloud step 5.1'))
        return

    def L2A_CSND_5_2(self):
        # Step 5.2: Ratio Band 2 / Band 11 for water bodies, dependent on Band 12
        T21_B12 = self.config.T21_B12 # 0.1
        T22_B12 = self.config.T22_B12 # -0.09
        T21_R_B02_B11 = self.config.T21_R_B02_B11 # 2.0
        T22_R_B02_B11 = self.config.T22_R_B02_B11 # 4.0
        T_B02 = 0.2 # modif JL water TOA reflectance shall be less than 20%
        B02 = self.tables.getBand(self.tables.B02)
        B11 = self.tables.getBand(self.tables.B11)
        B12 = self.tables.getBand(self.tables.B12)
        B8A = self.tables.getBand(self.tables.B8A) # B8A used for additional condition
        B04 = self.tables.getBand(self.tables.B04) # B04 used for additional condition
        R_B02_B11 = B02 / maximum(B11,self.LOWEST)
        B12_FT = clip(R_B02_B11*T21_B12+T22_B12, 0.07, 0.21)
        # additional condition on B8A and B04 to restrict over detection of water
        R_B02_B11_GT_T22_R_B02_B11 = where((R_B02_B11 > T22_R_B02_B11) & (B12 < B12_FT) & (B8A < B04) & (B02 < T_B02), True, False)
        CM = self.classificationMask # this is a reference, no need to reassign
        CM[(R_B02_B11_GT_T22_R_B02_B11 == True) & (CM == self._notClassified) & (CM!=self._noData)] = self._water
        self.confidenceMaskCloud[CM == self._water] = 0
        
        # additional condition on B8A and B04 to restrict over detection of water
        R15_AMB = (R_B02_B11 < T22_R_B02_B11) & (R_B02_B11 >= T21_R_B02_B11) & (B12 < B12_FT) & (B8A < B04) & (B02 < T_B02)
        if(R15_AMB.size > 0):
            a = -1 / (T22_R_B02_B11 - T21_R_B02_B11)
            b = -T21_R_B02_B11 * a + 1
            CMC = a * R_B02_B11[R15_AMB] + b
            self.confidenceMaskCloud[R15_AMB] *= CMC
        
        # second part, modification for improvement of water classification:
        T_24 = 0.034
        DIFF24_AMB = B02-B04
        #CM = self.classificationMask
        F1 = (DIFF24_AMB > T_24) & (B8A < B04) & (B02 < T_B02)
        #F2 = (DIFF24_AMB > T_24) & (B8A < B04) # potential topographic shadow over snow
        CM[F1 & (CM == self._notClassified)] = self._water
        self.confidenceMaskCloud[F1 & (CM == self._water) & (CM!=self._noData)] = 0
        #self.confidenceMaskCloud[F2 & (CM == self._notClassified)] = 0 # potential topographic shadow over snow
        self.logger.debug(statistics(self.confidenceMaskCloud, 'CM Cloud step 5.2'))
        return

    def L2A_CSND_6(self):
        # Step 6: Ratio Band 8 / Band 11 for rocks and sands in deserts
        T1_R_B8A_B11 = self.config.T1_R_B8A_B11 #0.90
        T2_R_B8A_B11 = self.config.T2_R_B8A_B11 #1.10
        T1_B02 = -0.25 
        T2_B02 = 0.475
        T_R_B02_B11 = 0.8
        B02 = self.tables.getBand(self.tables.B02)
        B8A = self.tables.getBand(self.tables.B8A)
        B11 = self.tables.getBand(self.tables.B11)
        R_B8A_B11 = B8A/maximum(B11,self.LOWEST)        
        B02_FT = clip(R_B8A_B11*T1_B02+T2_B02, 0.16, 0.35)        
        
        CM = self.classificationMask # this is a reference, no need to reassign
        # Correction JL20151223: condition for bare_soils is on threshold T1_R_B8A_B11 and B02 < T_R_B02_B11 * B11
        CM[(B02 < B02_FT) & (R_B8A_B11 < T1_R_B8A_B11) & (B02 < T_R_B02_B11*B11) & (CM == self._notClassified)] = self._bareSoils
        self.confidenceMaskCloud[CM == self._bareSoils] = 0  
        CMC = clip(R_B8A_B11, T1_R_B8A_B11, T2_R_B8A_B11)
        CMC = ((CMC - T1_R_B8A_B11)/(T2_R_B8A_B11-T1_R_B8A_B11))
        FLT = (R_B8A_B11 > T1_R_B8A_B11) & (R_B8A_B11 < T2_R_B8A_B11) & (B02 < B02_FT) & (CM == self._notClassified) & (CM!=self._noData)
        self.confidenceMaskCloud[FLT] *= CMC[FLT]
        self.logger.debug(statistics(self.confidenceMaskCloud, 'CM Cloud step 6'))
        return

    def L2A_CSND_6bis(self):
        # Step 6bis: Ratio Band 4 / Band 11 do discard cloud pixels with very high ratio B4/B11
        T1_R_B04_B11 = 3.0 #self.config.T1_R_B04_B11
        T2_R_B04_B11 = 6.0 #self.config.T2_R_B04_B11
        B04 = self.tables.getBand(self.tables.B04)
        B11 = self.tables.getBand(self.tables.B11)
        rb4b11 = B04/maximum(B11,self.LOWEST)
        CMC = clip(rb4b11, T1_R_B04_B11 , T2_R_B04_B11)
        CMC = (CMC - T1_R_B04_B11 ) / (T2_R_B04_B11 - T1_R_B04_B11 )
        CM = self.classificationMask
        FLT = [(CMC>0) & (CMC < 1.0)]
        CMC[FLT] = CMC[FLT] * -1 + 1
        self.confidenceMaskCloud[(CMC==1) & (CM == self._notClassified) & (CM!=self._noData)] = 0 # set cloud probability to 0 for CM ==1 e.g. B4/B11 > T2_R_B04_B11
        self.confidenceMaskCloud[FLT] *= CMC[FLT]
        self.logger.debug(statistics(self.confidenceMaskCloud, 'CM Cloud step 6bis'))
        return

    def L2A_CSND_7(self):
        import scipy.misc
        T_CLOUD_LP = self.config.T_CLOUD_LP
        T_CLOUD_MP = self.config.T_CLOUD_MP
        T_CLOUD_HP = self.config.T_CLOUD_HP
        T1_B10 = self.config.T1_B10
        T2_B10 = self.config.T2_B10
        B02 = self.tables.getBand(self.tables.B02)
        B10 = self.tables.getBand(self.tables.B10)
        LPC = self._lowProbaClouds
        MPC = self._medProbaClouds
        HPC = self._highProbaClouds
        CIR = self._thinCirrus
        CM = self.classificationMask
        CMC = self.confidenceMaskCloud
        
        REFL_BLUE_MAX = 0.50
        CM[(CMC > T_CLOUD_LP) & (CMC < T_CLOUD_MP) & (CM == self._notClassified)] = LPC
        self.logger.debug(statistics(CMC[(CM == LPC)], 'CM LOW_PROBA_CLOUDS'))
        CM[(CMC >= T_CLOUD_MP) & (CMC < T_CLOUD_HP) & (CM == self._notClassified)] = MPC
        self.logger.debug(statistics(CMC[(CM == MPC)], 'CM MEDIUM_PROBA_CLOUDS'))
        CM[(CMC >= T_CLOUD_HP) & (CM == self._notClassified)] = HPC
        self.logger.debug(statistics(CMC[(CM == HPC)], 'CM HIGH_PROBA_CLOUDS'))
        # Cirrus updated + DEM condition if available:
        if (self.tables.hasBand(self.tables.DEM) == True):
            dem = self.tables.getBand(self.tables.DEM)
            T_dem = 1500 # cirrus detection is switched off above 1500m
            CM[(B10 > T1_B10) & (B10 < T2_B10) & (B02 < REFL_BLUE_MAX) & (dem < T_dem) & (CMC < T_CLOUD_MP) & (CM!=self._noData)] = CIR
        else:
            CM[(B10 > T1_B10) & (B10 < T2_B10) & (B02 < REFL_BLUE_MAX) & (CMC < T_CLOUD_MP) & (CM!=self._noData)] = CIR
        
        self.logger.debug(statistics(CMC[(CM == CIR)], 'CM THIN_CIRRUS'))
        #CM[(B10 >= T2_B10) & (CM == self._notClassified)]= MPC
        CM[(B10 >= T2_B10) & (CMC < T_CLOUD_HP) & (CM!=self._noData)]= MPC # assign medium probability clouds class to Thick cirrus >= T2_B10
        self.logger.debug(statistics(CMC[(CM == MPC)], 'CM MEDIUM_PROBA_CLOUDS, step2'))
        return

    def L2A_SHD(self):
        csd1 = self.L2A_CSHD_1()
        csd2 = self.L2A_CSHD_2()
        CSP = (csd1 * csd2 > 0.05)
        del csd1
        del csd2

        CM = self.classificationMask
        
        CM[(CSP == True)] = self._cloudShadows
        return

    def L2A_CSHD_1(self):
        # Cloud shadow detection part1: Radiometric input
        T_B02_B12 = self.config.T_B02_B12
        T_water = 6.0

        x = self.config.nrows
        y = self.config.nrows
        BX = zeros((6, x, y), float32)
        BX[0, :, :] = self.tables.getBand(self.tables.B02)
        BX[1, :, :] = self.tables.getBand(self.tables.B03)
        BX[2, :, :] = self.tables.getBand(self.tables.B04)
        BX[3, :, :] = self.tables.getBand(self.tables.B8A)
        BX[4, :, :] = self.tables.getBand(self.tables.B11)
        BX[5, :, :] = self.tables.getBand(self.tables.B12)
        #RV_MEAN = array([0.0696000, 0.0526667, 0.0537708, 0.0752000, 0.0545000, 0.0255000], dtype=float32)
        RV_MEAN = array([0.12000, 0.08, 0.06, 0.10000, 0.0545000, 0.0255000], dtype=float32)
        # Modification JL 20160216
        distance = zeros((6, x, y), float32)
        for i in range(0, 6):
            distance[i, :, :] = (BX[i, :, :] - RV_MEAN[i])

        msd_dark = mean(distance < 0, axis=0)     # identify pixels with spectrum always under the reference shadow curve
        msd_dark = median_filter(msd_dark, 3)

        water = (BX[0, :, :]/BX[4, :, :]) > T_water   # identify water pixels with B2/B11 > T_water
        del BX

        msd = mean(abs(distance), axis=0)
        del distance

        msd = median_filter(msd, 3)
        msd = 1.0 - msd
        T0 = 1.0 - T_B02_B12
        msd[msd < T0] = 0.0
        
        msd[msd_dark == 1.0] = 1.0    # add very dark pixel to potential cloud shadow
        msd[water == True] = 0.0      # exclude water pixels with B2/B11 > T_water
        
        CM = self.classificationMask
        msd[CM == self._thinCirrus] = 0.0  # exclude cirrus pixels using classification mask

        return msd

    def L2A_CSHD_2(self):
        # Cloud shadow detection part2: Geometric input
        def reverse(a): return a[::-1]

        y = self.confidenceMaskCloud.shape[0]
        x = self.confidenceMaskCloud.shape[1]

        # fix for SIIMPC-1038, UMW: avoid floats as index, deprecated for numpy > 1.11
        y_aa = int(y * 1.5 + 0.5)  # +50% to avoid FFT aliasing
        x_aa = int(x * 1.5 + 0.5)  # +50% to avoid FFT aliasing
        
        cloud_mask = self.confidenceMaskCloud
        filt_b = zeros([y_aa, x_aa], float32)
        cloud_mask_aa = zeros([y_aa, x_aa], float32)  # to avoid FFT aliasing

        # Read azimuth and elevation solar angles
        solar_azimuth = int(self.config.solaz + 0.5)  # modif JL20160208 original sun azimuth value
        solar_elevation = int(90.0 - self.config.solze + 0.5)
        
        # Median Filter
        #cloud_mask = median_filter(cloud_mask, (7,7)) 
        cloud_mask = median_filter(cloud_mask, (3, 3))  # modif JL20160216

        # Dilatation cross-shape operator
        shape = generate_binary_structure(2, 1)
        cloud_mask = binary_dilation(cloud_mask > 0.33, shape).astype(cloud_mask.dtype)

        # Create generic cloud height distribution for 30m pixel resolution and adapt it to 20m or 60m resolution (zoom)
        resolution = self.config.resolution
        distr_clouds = concatenate([reverse(1. / (1.0 + (arange(51) / 30.0) ** (2 * 5))), 1 / (1.0 + (arange(150) / 90.0) ** (2 * 5))])
        distr_clouds = zoom(distr_clouds,30./float(resolution))

        # Create projected cloud shadow distribution
        npts_shad = distr_clouds.size / tan(solar_elevation * pi / 180.)
        factor = npts_shad/distr_clouds.size

        # SIITBX-46: to suppress unwanted user warning for zoom:
        import warnings
        warnings.filterwarnings('ignore')
        distr_shad = zoom(distr_clouds, factor)

        # Create filter for convolution (4 cases)
        filt_b[0:distr_shad.size, 0] = distr_shad
        ys = float(y_aa/2.0)
        xs = float(x_aa/2.0)

        # Place into center for rotation:
        filt_b = roll(filt_b, int(ys), axis=0)        
        filt_b = roll(filt_b, int(xs), axis=1)        
        rot_angle = -solar_azimuth 
        filt_b = rotate(filt_b, rot_angle, reshape=False, order=0)

        # case A:
        if (solar_azimuth >= 0) & (solar_azimuth < 90):
            filt_b = roll(filt_b, int(-ys), axis=0)        
            filt_b = roll(filt_b, int(xs), axis=1)

        # case B:
        if (solar_azimuth >= 90) & (solar_azimuth < 180):
            filt_b = roll(filt_b, int(ys), axis=0)
            filt_b = roll(filt_b, int(xs), axis=1)
        
        # case C:
        if (solar_azimuth >= 180) & (solar_azimuth < 270):
            filt_b = roll(filt_b, int(ys), axis=0)
            filt_b = roll(filt_b, int(-xs), axis=1)
        
        # case D:
        if (solar_azimuth >= 270) & (solar_azimuth < 360):
            filt_b = roll(filt_b, int(-ys), axis=0)
            filt_b = roll(filt_b, int(-xs), axis=1)

        #Fill cloud_mask_aa with cloud_mask for the FFT computation
        cloud_mask_aa[0:y, 0:x] = copy(cloud_mask[:, :])

        # Now perform the convolution:
        fft1 = fft.rfft2(cloud_mask_aa).astype(complex64)
        del cloud_mask_aa

        fft2 = fft.rfft2(filt_b).astype(complex64)
        del filt_b

        # proposal for new implementation
        fft1fft2 = fft1 * fft2
        del fft1
        del fft2
        shadow_prob_aa = fft.irfft2(fft1fft2).astype(float32)
        del fft1fft2

        #shadow_prob_aa = fft.irfft2(fft1 * fft2).astype(float32)
        #del fft1
        #del fft2

        shadow_prob = copy(shadow_prob_aa[0:y, 0:x])
        del shadow_prob_aa

        # Normalisation:
        shadow_prob = clip(shadow_prob, 0.0, 1.0)

        # Remove cloud_mask from Shadow probability:
        shadow_prob = maximum((shadow_prob - cloud_mask), 0)
        del cloud_mask

        # Gaussian smoothing of Shadow probability
        value = 3
        shadow_prob = gaussian_filter(shadow_prob, value).astype(float32)

        # Remove data outside of interest:
        shadow_prob[self.classificationMask == self._noData] = 0.0

        return shadow_prob

    def L2A_DarkVegetationRecovery(self):
        B04 = self.tables.getBand(self.tables.B04)
        B8A = self.tables.getBand(self.tables.B8A)
        NDVI = (B8A - B04) / maximum((B8A + B04), self.LOWEST)
        del B04

        T2_NDVI = self.config.T2_NDVI
        F1 = NDVI > T2_NDVI
        del NDVI
        CM = self.classificationMask
        CM[F1 & (CM == self._darkFeatures)] = self._vegetation
        CM[F1 & (CM == self._notClassified)] = self._vegetation
        del F1
        T2_R_B8A_B03 = self.config.T2_R_B8A_B03
        B03 = self.tables.getBand(self.tables.B03)
        rb8b3 = B8A/maximum(B03,self.LOWEST)
        del B8A
        del B03
        F2 = rb8b3 > T2_R_B8A_B03
        del rb8b3

        CM[F2 & (CM == self._darkFeatures) & (CM!=self._noData)] = self._vegetation
        CM[F2 & (CM == self._notClassified) & (CM!=self._noData)] = self._vegetation
        return

    def L2A_WaterPixelRecovery(self):
        # modified 2015 18 12
        # Sentinel-2 B2/B11 ratio > 4.0 and Band 8 < Band 4
        # for unclassified addtional condition: Band8 < 0.3 (F4)
        B02 = self.tables.getBand(self.tables.B02)
        B04 = self.tables.getBand(self.tables.B04)
        B8A = self.tables.getBand(self.tables.B8A)
        B11 = self.tables.getBand(self.tables.B11)
        R_B02_B11 = B02/maximum(B11,self.LOWEST)

        T22_R_B02_B11 = self.config.T22_R_B02_B11  # 4.0
        F3 = R_B02_B11 > T22_R_B02_B11

        T_B8A = 0.3
        F4 = B8A < T_B8A
        CM = self.classificationMask
        CM[F3 & (B8A < B04) & (CM == self._darkFeatures) & (CM!=self._noData)] = self._water
        CM[F3 & (B8A < B04) & F4 & (CM == self._notClassified) & (CM!=self._noData)] = self._water
        return
        
    def L2A_WaterPixelRecoveryCCI(self):
        if (self.tables.hasBand(self.tables.WBI) == True):
            WBI = self.tables.getBand(self.tables.WBI)
            CM = self.classificationMask
            CM[(WBI == 2) & (CM == self._darkFeatures) & (CM!=self._noData)] = self._water
            CM[(WBI == 2) & (CM == self._cloudShadows) & (CM!=self._noData)] = self._water
            CM[(WBI == 2) & (CM == self._notClassified) & (CM!=self._noData)] = self._water
            CM[(WBI == 2) & (CM == self._lowProbaClouds) & (CM!=self._noData)] = self._water
        return

    def L2A_WaterPixelCleaningwithDEM(self):
        # modified 2015 18 12
        # clean water pixels detected in topographic shadow or teef slopes
        if (self.tables.hasBand(self.tables.DEM) == True):
            slope = self.tables.getBand(self.tables.SLP)
            shadow = self.tables.getBand(self.tables.SDW)
            T_Shadow = 128
            T_Slope = 15
            clean_area = (shadow < T_Shadow) & (slope > T_Slope)
            CM = self.classificationMask
            CM[clean_area & (CM == self._water) & (CM != self._noData)] = self._darkFeatures
        return
        
    def L2A_CloudShadowPixelCleaningwithDEM(self):
        # modified 2016 02 18
        # clean cloud shadow pixels detected in topographic shadow or teef slopes
        if (self.tables.hasBand(self.tables.DEM) == True):
            shadow = self.tables.getBand(self.tables.SDW)
            T_Shadow = 32
            clean_area = (shadow < T_Shadow) & (shadow != 0) # clean_area excludes shadow values of 0 corresponding to sea (no_data)
            CM = self.classificationMask
            CM[clean_area & (CM == self._cloudShadows) & (CM != self._noData)] = self._darkFeatures
        return
        
#    def L2A_TopographicShadowwithDEM(self):
#        # modified 2016 02 18
#        # change unclassified pixels in topographic shadow or teef slopes
#        if (self.tables.hasBand(self.tables.DEM) == True):
#            shadow = self.tables.getBand(self.tables.SDW)
#            T_Shadow = 128
#            clean_area = (shadow < T_Shadow)
#            CM = self.classificationMask
#            CM[clean_area & (CM == self._notClassified)]= self._darkFeatures
#        return 
        
    def L2A_TopographicShadowwithDEM(self):
        # Process potential topographic shadow over snow/mountainous area and discard water areas from ESA CCI water bodies
        if (self.tables.hasBand(self.tables.DEM) == True) & (self.tables.hasBand(self.tables.WBI) == True):
            # Get pixels of topographic shadows from DEM geometry
            shadow = self.tables.getBand(self.tables.SDW)
            T_Shadow = 128
            WBI = self.tables.getBand(self.tables.WBI)
            clean_area = (shadow < T_Shadow) & (WBI != 2) # clean_area excludes water bodies
            CM = self.classificationMask
            
            # Get potential pixels of topographic shadows over snow/mountainous area from radiometry
            B02 = self.tables.getBand(self.tables.B02)
            B8A = self.tables.getBand(self.tables.B8A)
            B04 = self.tables.getBand(self.tables.B04)
            T_B02 = 0.20 # modif JL water TOA reflectance shall be less than 20%
            T_24 = 0.034
            DIFF24_AMB = B02-B04
            F2 = (DIFF24_AMB > T_24) & (B8A < B04) & (B02 > T_B02) # potential topographic shadow over snow
            # Assign darkFeatures class to pixels intersecting geometric and radiometric areas
            potential_pixels = (CM == self._notClassified) | (CM == self._lowProbaClouds) | (CM == self._medProbaClouds)
            CM[clean_area & F2 & potential_pixels & (CM!=self._noData)] = self._darkFeatures
        return

    def L2A_SnowRecovery(self):
        B03 = self.tables.getBand(self.tables.B03)
        B11 = self.tables.getBand(self.tables.B11)
        CM = self.classificationMask
        snow_mask = (CM == self._snowIce)
        struct = iterate_structure(generate_binary_structure(2,1), 3)
        snow_mask_dil = binary_dilation(snow_mask, struct)
        CM[snow_mask_dil & (B11 < B03) & (CM == self._notClassified) & (CM!=self._noData)] = self._snowIce
        return

    def L2A_SoilRecovery(self):
        T4 = 0.65
        T_B11 = 0.080 #T4 is too restricitve and some agricultural fields are classified as dark features maybe additional class could be added.
        B02 = self.tables.getBand(self.tables.B02)
        B11 = self.tables.getBand(self.tables.B11)
        R_B02_B11 = B02/maximum(B11,self.LOWEST)
        #F4 = (R_B02_B11 < T4) | (B11 > T_B11) # enlarge soil recovery to B11 > T_B11
        F4 = (R_B02_B11 < T4) # T_B11 disturbs cloud edges over water. previous line disabled.
        CM = self.classificationMask
        CM[F4 & (CM == self._darkFeatures) & (CM!=self._noData)] = self._bareSoils
        #CM[(CM == self._notClassified)] = self._bareSoils # modified 2015 12 18
        return

    def L2A_LandPixelRecoveryB10B09B8A(self):
        if (self.tables.hasBand(self.tables.LCM) == True):
            LCM = self.tables.getBand(self.tables.LCM)
            CM = self.classificationMask
            B10 = self.tables.getBand(self.tables.B10)
            B8A = self.tables.getBand(self.tables.B8A)
            B09 = self.tables.getBand(self.tables.B09)

            T_B10_min = 0.0015

            resolution = self.config.resolution
            blob_size = (300.0/resolution)**2

            Land = (CM == self._bareSoils) | (CM == self._vegetation)

            # Compute a threshold on B10 band to discard low B10 brightness (close to ground) pixels without removing True clouds
            nrEntriesTotal = float32(size(CM[CM != self._noData]))
            HPC_percentage = float32(size(CM[CM == self._highProbaClouds])) / nrEntriesTotal * 100.0
            Land_percentage = float32(Land.sum()) / nrEntriesTotal * 100.0

            # Discard this routine if Land pixels percentage less than 5 % or Land area less than 121 km2 (11km x 11km)
            if (Land_percentage < 5.0) | (Land.sum() < 121e6/resolution**2):
                # Not enough Land reference pixels to perform Land pixel recovery
                return

            if HPC_percentage > 20.0:
                T_B10 = max(T_B10_min, min(B10[Land].mean(), B10[(CM == self._highProbaClouds)].mean() - 2 * B10[(CM == self._highProbaClouds)].std()))
                T_B10 = min(T_B10, B10[Land].mean())

            elif HPC_percentage < 2.0:
                T_B10 = B10[Land].mean()
            else:
                # B10 statistics are computed only on clouds larger than 300m x 300m (e.g. 25 pixels at 60 m resolution)
                mask_large_HP_clouds = zeros(CM.shape).astype('bool')
                label_cloud_mask, nb_labels = ndimage.label(CM == self._highProbaClouds)
                hist_label_cloud_mask, hist_label_cloud_mask_edges = histogram(label_cloud_mask, bins=arange(label_cloud_mask.max()+2)-0.5)
                list_of_large_blobs = where(hist_label_cloud_mask[1:] > blob_size)[0] + 1  # Discard background = 0 (first index of histogram)
                label_cloud_mask_ravel = ravel(label_cloud_mask.copy())
                ravel(mask_large_HP_clouds)[in1d(label_cloud_mask_ravel, list_of_large_blobs)] = True
                T_B10 = max(T_B10_min, min(T_B10_min, B10[Land].mean(), B10[mask_large_HP_clouds].mean() - 2 * B10[mask_large_HP_clouds].std()))
                T_B10 = min(T_B10, B10[Land].mean())

            Ratio_B09_B8A = zeros(CM.shape).astype(float32)
            Ratio_B09_B8A[(CM != self._noData)] = float32(B09[(CM != self._noData)]) / float32(B8A[(CM != self._noData)])
            mean_Ratio_B09_B8A = Ratio_B09_B8A[Land].mean()
            std_Ratio_B09_B8A = Ratio_B09_B8A[Land].std()
            T_Ratio_B09_B8A = mean_Ratio_B09_B8A + std_Ratio_B09_B8A

            # ESA CCI Land pixel recovery for cloudy pixels with low B10 brightness (close to ground)
            CM[(LCM != 210) & ((CM == self._medProbaClouds) | (CM == self._highProbaClouds)) & (B10 < T_B10) & (Ratio_B09_B8A < T_Ratio_B09_B8A) & (CM != self._noData)] = self._bareSoils  # all land pixels
        return

    def L2A_CirrusPixelRecoveryB10(self):
        if (self.tables.hasBand(self.tables.LCM) == True):
            LCM = self.tables.getBand(self.tables.LCM)
            CM = self.classificationMask
            B10 = self.tables.getBand(self.tables.B10)

            # Cirrus pixel recovery
            T2_B10_Land =  max(B10[(CM == self._bareSoils)].mean() + 3 * B10[(CM == self._bareSoils)].std(), 0.0030)
            T2_B10_Water = max(B10[(CM == self._water)].mean() + B10[(CM == self._water)].std(), 0.0020)
            Land = (CM == self._bareSoils) | (CM == self._vegetation)

            if (self.tables.hasBand(self.tables.DEM) == True):
                dem = self.tables.getBand(self.tables.DEM)
                T_dem = 1500  # cirrus detection is switched off above 1500m
                # ESA CCI Cirrus pixel recovery above Land with dem condition
                CM[Land & (B10 > T2_B10_Land) & (dem < T_dem)] = self._thinCirrus
                # ESA CCI Cirrus pixel recovery above Water with dem condition
                CM[(CM == self._water) & (B10 > T2_B10_Water) & (dem < T_dem) & (CM != self._medProbaClouds) & (CM != self._highProbaClouds) & (CM != self._noData)] = self._thinCirrus
            else:
                # ESA CCI Cirrus pixel recovery above Land
                CM[Land & (B10 > T2_B10_Land)] = self._thinCirrus # land condition CM?
                # ESA CCI Cirrus pixel recovery above Water
                CM[(CM == self._water) & (B10 > T2_B10_Water) & (CM != self._medProbaClouds) & (CM != self._highProbaClouds) & (CM != self._noData)] = self._thinCirrus
        return

    def L2A_FineMorphoRecovery(self, mask, CM, thre_ring_cloudy):
        # label of blobs of binary mask
        label_mask, nb_labels = ndimage.label(mask)

        del(mask)

        # dilatation square operator (3x3)
        label_mask_dil = grey_dilation(label_mask, size=(3, 3))

        # construct the boundary layer, i.e. ring, of each blob
        ring = label_mask_dil - label_mask

        # hist_1: Compute histogram of label values of rings only on valid data
        hist_ring, hist_ring_edges = histogram(ring, bins=arange(ring.max()+2)-0.5)

        # ring intersection with clouds and cloud shadows
        mask_clouds = ((CM == self._lowProbaClouds) | (CM == self._medProbaClouds) | (CM == self._highProbaClouds) | (CM == self._thinCirrus) | (CM == self._cloudShadows))
        ring_cloudy = int32(zeros(CM.shape))
        ring_cloudy[mask_clouds] = ring[mask_clouds]

        # hist_2: Histogram of label values of the cloudy part of the rings
        hist_ring_cloudy, hist_ring_cloudy_edges = histogram(ring_cloudy, bins=arange(ring.max()+2)-0.5)

        # percentage of cloudy part of the rings / total rings : Division of Hist 1 / Hist 2
        CloudRingPixelsPercentage = float32(hist_ring_cloudy) / float32(hist_ring) * 100.0

        # identify labeled blob for which the boundary layer (ring) is composed by more than thre_ring_cloudy percent.
        # exclude index = 0 with ([1:]) corresponding to the non-labelled blob (background).
        labels_clouds = array(where(CloudRingPixelsPercentage[1:] > thre_ring_cloudy)[0]) + 1

        label_mask_ravel = ravel(label_mask.copy())
        label_mask_ravel[in1d(label_mask_ravel, labels_clouds)] = 0
        label_mask = label_mask_ravel.reshape(label_mask.shape)
        return label_mask

    def L2A_UrbanBarePixelRecoveryCCI(self):
        if (self.tables.hasBand(self.tables.LCM) == True):
            LCM = self.tables.getBand(self.tables.LCM)
            CM = self.classificationMask

            # ESA CCI Urban pixel recovery step 1 for notClassified pixels
            CM[(LCM == 190) & (CM == self._notClassified) & (CM!=self._noData)] = self._bareSoils    # Urban class = 190

            # ESA CCI Urban pixel recovery step 2 for lowProbaClouds and medProbaClouds
            UrbanMask = ((LCM == 190) & ((CM == self._lowProbaClouds) | (CM == self._medProbaClouds)) & (CM!=self._noData)).astype('uint8')
            T_urban = 35.0
            Label_UrbanMask = self.L2A_FineMorphoRecovery(UrbanMask, CM, T_urban)
            CM[(Label_UrbanMask != 0) & (CM != self._noData)] = self._bareSoils

            # ESA CCI Bare pixel recovery step 1 for notClassified pixels
            CM[(LCM >= 200) & (LCM <= 202) & (CM == self._notClassified) & (CM != self._noData)] = self._bareSoils    # Bare classes = 200, 201, 202

            # ESA CCI Bare pixel recovery step 2 for lowProbaClouds and medProbaClouds
            BrightMask = ((LCM >= 200) & (LCM <= 202) & ((CM == self._lowProbaClouds) | (CM == self._medProbaClouds)) & (CM!=self._noData)).astype('uint8')
            T_bright = 35.0
            Label_BrightMask = self.L2A_FineMorphoRecovery(BrightMask, CM, T_bright)
            CM[(Label_BrightMask != 0) & (CM != self._noData)] = self._bareSoils
        return

    def average(self, oldVal, classifier, count):
        newVal = self.getClassificationPercentage(classifier)
        if count > 1:
            result = (float32(oldVal) * float32(count-1) + float32(newVal)) / float32(count)
        else:
            result = float32(newVal)
        return format('%f' % clip(result, 0, 100))

    def getClassificationPercentage(self, classificator):
        cm = self.classificationMask
        if(classificator == self._noData):
            # count all for no data pixels:
            nrEntriesTotal = float32(size(cm))
            nrEntriesClassified = float32(size(cm[cm == self._noData]))
            self._sumPercentage = 0.0
        elif(classificator == self._cloudCoverage):
            # count percentage of classified pixels:
            nrEntriesTotal = float32(size(cm[cm != self._noData]))
            nrEntriesClassified = float32(size(cm[(cm == self._medProbaClouds) | (cm == self._highProbaClouds) | (cm == self._thinCirrus)]))
        else:
            # count percentage of classified pixels:
            nrEntriesTotal = float32(size(cm[cm != self._noData]))
            nrEntriesClassified = float32(size(cm[cm == classificator]))

        if nrEntriesTotal > 0:
            fraction = nrEntriesClassified / nrEntriesTotal
        else:
            fraction = 0

        percentage = clip(fraction * 100.0, 0, 100)
        self._sumPercentage += percentage
        self._sumPercentage = clip(self._sumPercentage, 0,100)

        self.logger.info('Classificator: %d' % classificator)
        self.logger.info('Percentage: %f' % percentage)
        self.logger.info('Sum Percentage: %f' % self._sumPercentage)

        return percentage

    def getClearLandPixelsPercentage(self):
        cm = self.classificationMask
        # count percentage of clear land pixels (vegetation + not-vegetated + snow) vs all classified pixels except water
        nrEntriesTotal = float32(size(cm[(cm != self._noData) | (cm != self._water)]))
        nrEntriesClassified = float32(size(cm[(cm == self._vegetation) | (cm == self._bareSoils) | (cm == self._water) | (cm == self._snowIce)]))
        if nrEntriesTotal > 0:
            fraction = nrEntriesClassified / nrEntriesTotal
        else:
            fraction = 0
        percentage = clip(fraction * 100.0, 0, 100)

        return percentage

    def updateQualityIndicators(self, nrTilesProcessed, metadata):
        xp = L2A_XmlParser(self.config, metadata)
        test = True

        MEDIUM_PROBA_CLOUDS_PERCENTAGE = self.getClassificationPercentage(self._medProbaClouds)
        HIGH_PROBA_CLOUDS_PERCENTAGE = self.getClassificationPercentage(self._highProbaClouds)
        THIN_CIRRUS_PERCENTAGE = self.getClassificationPercentage(self._thinCirrus)
        CLOUD_COVERAGE_PERCENTAGE = clip(MEDIUM_PROBA_CLOUDS_PERCENTAGE + \
                                    HIGH_PROBA_CLOUDS_PERCENTAGE + THIN_CIRRUS_PERCENTAGE, 0,100)

        if metadata == 'T2A':
            imageContentQI = 'Image_Content_QI'

            try:
                icqi = xp.getTree('Quality_Indicators_Info', imageContentQI)
                if len(icqi) == 0:
                    qii = xp.getRoot('Quality_Indicators_Info')
                    icqi = objectify.Element(imageContentQI)
                    test = False

                icqi.NODATA_PIXEL_PERCENTAGE = format('%f' % self.getClassificationPercentage(self._noData))
                icqi.SATURATED_DEFECTIVE_PIXEL_PERCENTAGE = format('%f' % self.getClassificationPercentage(self._saturatedDefective))
                icqi.DARK_FEATURES_PERCENTAGE = format('%f' % self.getClassificationPercentage(self._darkFeatures))
                icqi.CLOUD_SHADOW_PERCENTAGE = format('%f' % self.getClassificationPercentage(self._cloudShadows))
                icqi.VEGETATION_PERCENTAGE = format('%f' % self.getClassificationPercentage(self._vegetation))
                icqi.NOT_VEGETATED_PERCENTAGE = format('%f' % self.getClassificationPercentage(self._bareSoils))
                icqi.WATER_PERCENTAGE = format('%f' % self.getClassificationPercentage(self._water))
                # fix for SIIMPC-963 JL.1:
                icqi.UNCLASSIFIED_PERCENTAGE = format('%f' % self.getClassificationPercentage(self._lowProbaClouds))
                icqi.MEDIUM_PROBA_CLOUDS_PERCENTAGE = format('%f' % MEDIUM_PROBA_CLOUDS_PERCENTAGE)
                icqi.HIGH_PROBA_CLOUDS_PERCENTAGE = format('%f' % HIGH_PROBA_CLOUDS_PERCENTAGE)
                icqi.THIN_CIRRUS_PERCENTAGE = format('%f' % THIN_CIRRUS_PERCENTAGE)

                icqi.CLOUDY_PIXEL_PERCENTAGE = format('%f' % CLOUD_COVERAGE_PERCENTAGE)
                icqi.SNOW_ICE_PERCENTAGE = format('%f' % self.getClassificationPercentage(self._snowIce))
                icqi.RADIATIVE_TRANSFER_ACCURACY = 0.0
                icqi.WATER_VAPOUR_RETRIEVAL_ACCURACY = 0.0
                icqi.AOT_RETRIEVAL_ACCURACY = 0.0

                if not test:
                    qii.insert(1, icqi)
                xp.export()
            except:
                self.logger.error('Update of quality metadata on tile level failed')

        elif metadata == 'UP2A':
            qualityIndictorsInfo = 'Quality_Indicators_Info'
            try:
                # this can have concurrent access:
                l.acquire()
                # this is common for all versions:
                cca = xp.getTree('Quality_Indicators_Info', 'Cloud_Coverage_Assessment')
                if nrTilesProcessed > 1:
                    cca._setText(self.average(cca.pyval, self._cloudCoverage, nrTilesProcessed))
                else:
                    cca._setText(format('%f' % CLOUD_COVERAGE_PERCENTAGE))

                qii = xp.getNode(qualityIndictorsInfo)
                icqi = xp.getTree(qualityIndictorsInfo, 'Image_Content_QI')
                if not icqi:
                    icqi = objectify.Element('Image_Content_QI')
                    test = False

                    icqi.NODATA_PIXEL_PERCENTAGE = 0
                    icqi.SATURATED_DEFECTIVE_PIXEL_PERCENTAGE = 0
                    icqi.DARK_FEATURES_PERCENTAGE = 0
                    icqi.CLOUD_SHADOW_PERCENTAGE = 0
                    icqi.VEGETATION_PERCENTAGE = 0
                    icqi.NOT_VEGETATED_PERCENTAGE = 0
                    icqi.WATER_PERCENTAGE = 0
                    # fix for SIIMPC-963 JL.2:
                    icqi.UNCLASSIFIED_PERCENTAGE = 0
                    icqi.MEDIUM_PROBA_CLOUDS_PERCENTAGE = 0
                    icqi.HIGH_PROBA_CLOUDS_PERCENTAGE = 0
                    icqi.THIN_CIRRUS_PERCENTAGE = 0
                    #icqi.CLOUDY_PIXEL_PERCENTAGE = 0
                    icqi.SNOW_ICE_PERCENTAGE = 0

                icqi.NODATA_PIXEL_PERCENTAGE = self.average(icqi.NODATA_PIXEL_PERCENTAGE, self._noData, nrTilesProcessed)
                icqi.SATURATED_DEFECTIVE_PIXEL_PERCENTAGE = self.average(icqi.SATURATED_DEFECTIVE_PIXEL_PERCENTAGE, self._saturatedDefective, nrTilesProcessed)
                icqi.DARK_FEATURES_PERCENTAGE = self.average(icqi.DARK_FEATURES_PERCENTAGE, self._darkFeatures, nrTilesProcessed)
                icqi.CLOUD_SHADOW_PERCENTAGE = self.average(icqi.CLOUD_SHADOW_PERCENTAGE, self._cloudShadows, nrTilesProcessed)
                icqi.VEGETATION_PERCENTAGE = self.average(icqi.VEGETATION_PERCENTAGE, self._vegetation, nrTilesProcessed)
                icqi.NOT_VEGETATED_PERCENTAGE = self.average(icqi.NOT_VEGETATED_PERCENTAGE, self._bareSoils, nrTilesProcessed)
                icqi.WATER_PERCENTAGE = self.average(icqi.WATER_PERCENTAGE, self._water, nrTilesProcessed)
                # fix for SIIMPC-963 JL.3:
                icqi.UNCLASSIFIED_PERCENTAGE = self.average(icqi.UNCLASSIFIED_PERCENTAGE, self._lowProbaClouds, nrTilesProcessed)
                icqi.MEDIUM_PROBA_CLOUDS_PERCENTAGE = self.average(icqi.MEDIUM_PROBA_CLOUDS_PERCENTAGE, self._medProbaClouds, nrTilesProcessed)
                icqi.HIGH_PROBA_CLOUDS_PERCENTAGE = self.average(icqi.HIGH_PROBA_CLOUDS_PERCENTAGE, self._highProbaClouds, nrTilesProcessed)
                icqi.THIN_CIRRUS_PERCENTAGE = self.average(icqi.THIN_CIRRUS_PERCENTAGE, self._thinCirrus, nrTilesProcessed)
                #icqi.CLOUDY_PIXEL_PERCENTAGE = self.average(icqi.CLOUDY_PIXEL_PERCENTAGE, self._cloudCoverage, nrTilesProcessed)
                icqi.SNOW_ICE_PERCENTAGE = self.average(icqi.SNOW_ICE_PERCENTAGE, self._snowIce, nrTilesProcessed)
                icqi.RADIATIVE_TRANSFER_ACCURACY = 0.0
                icqi.WATER_VAPOUR_RETRIEVAL_ACCURACY = 0.0
                icqi.AOT_RETRIEVAL_ACCURACY = 0.0

                if not test:
                    qii.append(icqi)
                xp.export()
            except:
                self.logger.error('Update of quality metadata on user level failed')
            finally:
                l.release()

    def process(self):
        self.config.timestamp('Pre process   ')
        if self.preprocess() == False:
            self.postprocess()
            self.config.timestamp('Post process  ')
            return True
        self.config.timestamp('L2A_SC init   ')
        self.L2A_CSND_1_1()
        self.config.timestamp('L2A_CSND_1_1  ')
        self.L2A_CSND_1_2()
        self.config.timestamp('L2A_CSND_1_2  ')
        if(self.tables.sceneCouldHaveSnow() == True):
            self.logger.info('Snow probability from climatology, detection will be performed')
            self.L2A_CSND_2_0()
            self.config.timestamp('L2A_CSND_2_0  ')
            self.L2A_CSND_2_1()
            self.config.timestamp('L2A_CSND_2_1  ')
            # JL 20180419  Updated because led to snow omission in low altitude region, e.g. Easton-MDE, 2018 March 22
            self.L2A_CSND_2_1bis()
            self.config.timestamp('L2A_CSND_2_1_2')
            # JL 20180419
            self.L2A_CSND_2_2()
            self.config.timestamp('L2A_CSND_2_2  ')
            self.L2A_CSND_2_3()
            self.config.timestamp('L2A_CSND_2_3  ')
            self.L2A_CSND_2_4()
            self.config.timestamp('L2A_CSND_2_4  ')
            self.L2A_CSND_2_5()
            self.config.timestamp('L2A_CSND_2_5  ')
            self.L2A_SnowPostProcessingCCI()
            self.config.timestamp('L2A_SnowPostProcessingCCI  ')
        else:
            self.logger.info('No snow probability from climatology, detection will be ignored')
        self.L2A_CSND_3()
        self.config.timestamp('L2A_CSND_3    ')
        #self.L2A_CSND_4()
        #self.config.timestamp('L2A_CSND_4    ')
        self.L2A_CSND_5_1()
        self.config.timestamp('L2A_CSND_5_1  ')
        self.L2A_CSND_5_2()
        self.config.timestamp('L2A_CSND_5_2  ')
        self.L2A_CSND_6()
        self.config.timestamp('L2A_CSND_6    ')
        # JL 20160219
        self.L2A_CSND_6bis()
        self.config.timestamp('L2A_CSND_6_2  ')
        # JL 20160219
        self.L2A_CSND_7()
        self.config.timestamp('L2A_CSND_7    ')
        if not self.config.TESTMODE:
            self.L2A_SHD()
        self.config.timestamp('L2A_SHD       ')
        self.L2A_DarkVegetationRecovery()
        self.config.timestamp('DV recovery   ')
        self.L2A_WaterPixelRecovery()
        self.config.timestamp('WP recovery   ')
        self.L2A_WaterPixelRecoveryCCI()
        self.config.timestamp('WP recovery with CCI Water Bodies at 150m  ')
        if (self.tables.hasBand(self.tables.DEM) == True):
            self.L2A_WaterPixelCleaningwithDEM()
            self.config.timestamp('Water Pixels cleaning with DEM')
            self.L2A_CloudShadowPixelCleaningwithDEM()
            self.config.timestamp('Cloud Shadow Pixels cleaning with DEM')
            self.L2A_TopographicShadowwithDEM()
            self.config.timestamp('Topographic shadows classification over snow in mountainous area with DEM')
        self.L2A_SnowRecovery()
        self.config.timestamp('Snow recovery ')
        self.L2A_SoilRecovery()
        self.config.timestamp('Soil recovery ')
        self.L2A_LandPixelRecoveryB10B09B8A()
        self.config.timestamp('Land recovery with B10, B09 and B8A ')
        self.L2A_CirrusPixelRecoveryB10()
        self.config.timestamp('Cirrus recovery with B10 ')
        self.L2A_UrbanBarePixelRecoveryCCI()
        self.config.timestamp('Urban and Bare pixel recovery with CCI Land Cover Map at 300 m ')
        self.config.timestamp('Post process  ')
        return self.postprocess()

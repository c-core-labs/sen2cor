#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

from numpy import *
import sys, fnmatch
import os, logging
import cPickle as pickle
from time import time
from datetime import datetime
# import cProfile, pstats, StringIO

from L2A_Tables import L2A_Tables
from L2A_SceneClass import L2A_SceneClass
from L2A_AtmCorr import L2A_AtmCorr
from L2A_XmlParser import L2A_XmlParser
from L2A_Logger import SubProcessLogHandler, getLevel

SUCCESS = 0
FAILURE = 1

class L2A_ProcessTile(object):

    def __init__(self, config):
        self._config = config
        self._logger = config.logger
        # fix for issue SIIMPC-1378:
        img_database_dir = config.img_database_dir
        res_database_dir = config.res_database_dir
        # https://jira.acri- cwa.fr/browse/SIIMPC-1331 :
        try:
            fp = open(config.picFn, 'rb')
        except:
            picFn = os.path.join(self.config.work_dir, self.config.L2A_TILE_ID + '.pic')
            fp = open(picFn, 'rb')
        try:
            self._config.logger = None
            resolution = config.resolution
            tEstimation = config.tEstimation
            self._config = pickle.load(fp)
            fp.close()
            self._config.logger = self._logger
            self._config.resolution = resolution
            self._config.tEstimation = tEstimation
            # fix for issue SIIMPC-1378:
            self.config.img_database_dir = img_database_dir
            self.config.res_database_dir = res_database_dir
        except:
            self._logger.error('cannot read configuration object')
            return False
        finally:
            if fp: fp.close()

    def get_logger(self):
        return self._logger

    def set_logger(self, value):
        self._logger = value

    def del_logger(self):
        del self._logger

    def get_tables(self):
        return self._tables

    def set_tables(self, value):
        self._tables = value

    def del_tables(self):
        del self._tables

    def get_config(self):
        return self._config

    def set_config(self, value):
        self._config = value

    def del_config(self):
        del self._config

    def __exit__(self):
        sys.exit(-1)

    config = property(get_config, set_config, del_config, "config's docstring")
    tables = property(get_tables, set_tables, del_tables, "tables's docstring")
    logger = property(get_logger, set_logger, del_logger, "logger's docstring")

    def process(self):
        logger = self.logger
        if self.config.resolution == 0:
            logger.stream('No resolution specified, will process 20 and 10 m resolution')
            if self.config.downsample20to60:
                logger.stream('20 m resolution will be downsampled to 60 m')
        else:
            logger.stream('Selected resolution: %s m' % self.config.resolution)

        if (self.config.resolution == 0):
            if not self.process_resolution(20):
                return False
            if not self.process_resolution(10):
                return False
            return True
        elif (self.config.resolution == 60):
            self.config.downsample20to60 = False
            if not self.process_resolution(60):
                return False
        elif (self.config.resolution == 20):
            if not self.process_resolution(20):
                return False
        elif (self.config.resolution == 10):
            if not self.process_resolution(10):
                return False
        return True

    def process_resolution(self, resolution):
        if not self.config.preprocess(resolution):
            self._msgQueue.put(FAILURE)
            return False

        self.tables = L2A_Tables(self.config)

        # fix for SIIMPC-794: reestablish the 20 m preprocessing ...
        # check existing processing of 20 m resolution, which is required for 10 m processing (except if sc_only):
        if resolution == 10 and not self.tables.checkAotMapIsPresent(20) and self.config.scOnly == False:
            self.config.timestamp('L2A_ProcessTile: 20 m resolution must be processed first')
            if not self.process_resolution(20):
                return False
            return self.process_resolution(10)
        return self._process()

    def _process(self):
        # pr = cProfile.Profile()
        # pr.enable()
        self.config.getEntriesFromDatastrip()
        self.config.readTileMetadata()
        if self.tables.checkAotMapIsPresent(self.config.resolution):
            self.config.timestamp('L2A_ProcessTile: resolution ' + str(self.config.resolution) + ' m already processed')
            return True

        astr = 'L2A_ProcessTile: processing with resolution ' + str(self.config.resolution) + ' m'
        self.config.timestamp(astr)
        self.config.timestamp('L2A_ProcessTile: start of pre processing')
        if self.preprocess() == False:
            self.logger.fatal('Module %s failed' % (self.config.L2A_TILE_ID))
            return False

        if self.config.resolution > 10:
            self.config.timestamp('L2A_ProcessTile: start of Scene Classification')
            sc = L2A_SceneClass(self.config, self.tables)
            self.logger.info('performing Scene Classification with resolution %d m' % self.config.resolution)
            if sc.process() == False:
                self.logger.fatal('Module %s failed' % (self.config.L2A_TILE_ID))
                return False

        scl = self.tables.getBand(self.tables.SCL)
        if scl.max() == 0:
            self.config.scOnly = True
        del scl
        if self.config.scOnly == False:
            ac = L2A_AtmCorr(self.config, self.tables)
            if (self.config.resolution > 10) and (self.config.aerosolType == 'AUTO'):
                self.config.timestamp('L2A_ProcessTile: start of Automatic Aerosol Type Detection')
                self.logger.info('performing aerosol type detection with resolution %d m' % self.config.resolution)
                ac.automaticAerosolDetection()

            self.config.timestamp('L2A_ProcessTile: start of Atmospheric Correction')
            self.logger.info('performing atmospheric correction with resolution %d m' % self.config.resolution)
            if ac.process() == False:
                self.logger.fatal('Module %s failed' % (self.config.L2A_TILE_ID))
                return False

        self.config.timestamp('L2A_ProcessTile: start of post processing')
        if self.postprocess() == False:
            self.logger.fatal('Module %s failed' % (self.config.L2A_TILE_ID))
            return False
        # else:
        #     pr.disable()
        #     s = StringIO.StringIO()
        #     sortby = 'cumulative'
        #     ps = pstats.Stats(pr, stream=s).sort_stats(sortby).print_stats(.25, 'L2A_')
        #     ps.print_stats()
        #     profile = s.getvalue()
        #     s.close()
        #     with open(os.path.join(self.config.logDir, 'runtime_profile.log'), 'w') as textFile:
        #         textFile.write(profile)
        #         textFile.close()

        return True

    def setupLogger(self):
        # assign the logger to use.
        logger = logging.getLogger('sen2cor.subprocess')
        self._logger = logger

        return

    def preprocess(self):
        self.logger.info('pre-processing with resolution %d m', self.config.resolution)
        # this is to check the config for the L2A_AtmCorr in ahead.
        # This has historical reasons due to ATCOR porting.
        # Should be moved to the L2A_Config for better design:
        dummy = L2A_AtmCorr(self.config, self.logger)
        dummy.checkConfiguration()
        self.config._stat = {'read':{},'write':{}}
        # validate the meta data:
        xp = L2A_XmlParser(self.config, 'T1C')
        xp.validate()

        if (self.tables.checkBandCount() == False):
            self.logger.fatal('insufficient nr. of bands in tile: ' + self.config.L2A_TILE_ID)
            return False

        if self.config.midLatitude == 'AUTO':
            self.config.setMidLatitude()

        if self.config.ozoneSetpoint == 0:
            try:
                ozoneMeasured = self.tables.getAuxData(self.tables.OZO)
                ozoneSetpoint = ozoneMeasured[ozoneMeasured > 0].mean()
            except:
                self.logger.warning('no ozone values found in input data, default (331) will be used')
                ozoneSetpoint = 331

            self.config.setOzoneContentFromMetadata(ozoneSetpoint)

        elif self.config.aerosolType != 'AUTO':
            self.config.createAtmDataFilename()

        if (self.tables.importBandList() == False):
            self.logger.fatal('import of band list failed')
            return False

        return True

    def postprocess(self):
        self.logger.info('post-processing with resolution %d m', self.config.resolution)
        res = True
        if not self.tables.exportBandList():
            res = False
        if self.config.resolution == 20 and self.config.downsample20to60 == True:
            self.tables.downsampleBandList_20to60_andExport()
        self.config = self.tables.config
        if not self.config.postprocess():
            res = False
        return res

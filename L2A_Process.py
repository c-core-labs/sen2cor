#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

### This module creates the central structure of the L2A_Product and calls the L2A_Schedule module

from numpy import *
from tables import *
import sys, os, logging, fnmatch, warnings, platform, multiprocessing
from shutil import copyfile
from datetime import datetime
from time import time
from L2A_Logger import L2A_Logger,getLevel
from L2A_Config import L2A_Config, getScriptDir
from L2A_XmlParser import L2A_XmlParser
from L2A_ProcessTileToolbox import SUCCESS, FAILURE, ALREADY_PROCESSED
from multiprocessing import Lock
l = Lock()


def readNamingConvention(input_dir, mode, logger = None):
        # step 1 test the standard naming format:
        if mode == 'generate_datastrip':
            userProduct = os.path.basename(input_dir)
            S2A_L1C_mask_safe_standard = 'S2?_OPER_MSI_L1C_DS*'
            S2A_L1C_mask_safe_compact  = 'DS_MPS*'
        elif mode == 'process_tile':
            userProduct = os.path.basename(input_dir)
            S2A_L1C_mask_safe_standard = 'S2?_OPER_MSI_L1C_TL*'
            S2A_L1C_mask_safe_compact  = 'L1C_T*'
        else:
            userProduct = input_dir
            S2A_L1C_mask_safe_standard = 'S2?_OPER_PRD_MSIL1C*.SAFE'
            S2A_L1C_mask_safe_compact  = 'S2?_MSIL1C*.SAFE'

        if (fnmatch.fnmatch(userProduct, S2A_L1C_mask_safe_standard) == True):
            nc = 'SAFE_STANDARD'
        elif(fnmatch.fnmatch(userProduct, S2A_L1C_mask_safe_compact) == True):
            nc = 'SAFE_COMPACT'
        else:
            nc = 'UNKNOWN'
            if logger:
                logger.error('L1C user product directory must match the following mask: %s' % S2A_L1C_mask_safe_compact)
                logger.error('but is: %s' % userProduct)

        return nc


def parseCommandLine(args):

    config = L2A_Config(None)
    logger = L2A_Logger('sen2cor',operation_mode = args.mode)
    selectedTile = None

    try:
        test = args.input_dir
        input_dir = os.path.normpath(test)
        if not (os.path.isabs(input_dir)):
            cwd = os.getcwd()
            input_dir = os.path.join(cwd, input_dir)
        if os.path.exists(input_dir):
            # check if input_dir argument contains a tile. If yes, split the tile from path,
            # put the tile in the config object created below as selected tile,
            # create the new path for the user directory.
            if 'GRANULE' in input_dir:
                dirname, dummy = os.path.split(input_dir)
                input_dir = dirname
            userProduct = os.path.basename(input_dir)
            config = L2A_Config(None, input_dir)
            config.operationMode = 'TOOLBOX'
            config.namingConvention = readNamingConvention(userProduct, args.mode, logger = config.logger)
            if config.namingConvention == 'UNKNOWN':
                return config
        else:
            logger.error('Input user product does not exist.')
            return config
    except:
        pass

    if (args.mode != 'generate_datastrip' and \
        args.mode != 'process_tile') and \
            config.operationMode != 'TOOLBOX':
        logger.error('wrong operation mode: %s' % args.mode)
        config.operationMode = 'INVALID'
        return config

    if args.mode == 'generate_datastrip':
        work_dir = os.path.normpath(args.work_dir)
        if not os.path.exists(work_dir):
            os.mkdir(work_dir)
        if args.datastrip == None:
            logger.error('No argument for datastrip present.')
            return config
        elif args.processing_centre == None:
            logger.error('No argument for processing centre present.')
            return config
        elif args.archiving_centre == None:
            logger.error('No argument for archiving centre present.')
            return config
        datastrip = os.path.normpath(args.datastrip)
        if os.path.exists(datastrip):
            input_dir = os.path.dirname(datastrip)
            config = L2A_Config(None, input_dir)
            config.datastrip = os.path.basename(datastrip)
            config.datastrip_root_folder = datastrip
            config.processing_centre = args.processing_centre
            config.archiving_centre = args.archiving_centre
            config.work_dir = work_dir
            config.operationMode = 'GENERATE_DATASTRIP'
            config.namingConvention = readNamingConvention(config.datastrip, args.mode, logger = config.logger)
        else:
            logger.error('Input datastrip does not exist.')
            return config
    # end generate_datastrip

    elif args.mode == 'process_tile':
        work_dir = os.path.normpath(args.work_dir)
        if not os.path.exists(work_dir):
            os.mkdir(work_dir)
        if args.datastrip == None:
            logger.error('No argument for datastrip present.')
            return config            
        elif args.tile == None:
            logger.error('No argument for tile present.')
            return config
        elif args.work_dir == None:
            logger.error('No argument for work directory present.')
            return config
        datastrip = os.path.normpath(args.datastrip)
        if not os.path.exists(datastrip):
            logger.error('Input datastrip does not exist.')
            return config
        tile = os.path.normpath(args.tile)
        if os.path.exists(tile):
            input_dir = os.path.dirname(tile)
            config = L2A_Config(None, input_dir)
            config.datastrip = os.path.basename(datastrip)
            config.datastrip_root_folder = datastrip
            config.L2A_DS_ID = config.datastrip
            config.L2A_DS_DIR = datastrip
            config.L2A_DS_MTD_XML = os.path.join(datastrip, 'MTD_DS.xml')
            config.tile = os.path.basename(tile)
            config.selectedTile = tile
            config.nrTiles = 1
            config.L1C_TILE_ID = os.path.basename(tile)
            config.work_dir = work_dir
            config.operationMode = 'PROCESS_TILE'
            config.namingConvention = readNamingConvention(config.tile, args.mode, logger = logger)
        else:
            logger.error('Input tile does not exist.')
            return config
    # end process_tile

    if config.operationMode != 'TOOLBOX':
        if args.output_dir == None:
            logger.error('No argument for output directory present.')
            config.operationMode = 'INVALID'
            return config
        output_dir = os.path.normpath(args.output_dir)
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        config.output_dir = output_dir
        # SIIMPC-1327:
        if args.img_database_dir == None:
            img_database_dir = config.work_dir
        else:
            img_database_dir = os.path.normpath(args.img_database_dir)
        if args.res_database_dir == None:
            res_database_dir = config.work_dir
        else:
            res_database_dir = os.path.normpath(args.res_database_dir)
        if img_database_dir == None:
            logger.error('No argument for image database directory present.')
            config.operationMode = 'INVALID'
            return config
        if not os.path.exists(img_database_dir):
            os.mkdir(img_database_dir)
        if not os.path.exists(res_database_dir):
            os.mkdir(res_database_dir)
        if res_database_dir == None:
            logger.error('No argument for result database directory present.')
            config.operationMode = 'INVALID'
            return config
        config.img_database_dir = img_database_dir
        config.res_database_dir = res_database_dir
    # end ! TOOLBOX

    else: # TOOLBOX:
        directory = os.path.normpath(args.input_dir)
        if not (os.path.isabs(directory)):
            cwd = os.getcwd()
            directory = os.path.join(cwd, directory)
        else:
            cwd = os.path.dirname(directory)

        config.work_dir = directory
        if args.output_dir:
            output_dir = os.path.normpath(args.output_dir)
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            config.output_dir = output_dir
        else:
            config.output_dir = cwd

        if args.processing_centre:
            config.processing_centre = args.processing_centre

        if args.processing_baseline:
            config.processingBaseline = float32(args.processing_baseline)

        # check if directory argument contains a tile. If yes, split the tile from path,
        # put the tile in the config object created below as selected tile,
        # create the new path for the user directory.
        if 'GRANULE' in config.work_dir:
            dirname, selectedTile = os.path.split(config.work_dir)
            directory = os.path.dirname(config.work_dir)

        test = os.path.basename(directory)
        # step 1 test the standard naming format:
        S2A_L1C_mask = 'S2?_OPER_PRD_MSIL1C*.SAFE'
        if (fnmatch.fnmatch(test, S2A_L1C_mask) == True):
            nc = 'SAFE_STANDARD'
        else:
            S2A_L1C_mask = 'S2?_MSIL1C*.SAFE'
            if (fnmatch.fnmatch(test, S2A_L1C_mask) == True):
                nc = 'SAFE_COMPACT'
            else:
                logger.error('L1C user product directory must match the following mask: %s, but is %s' % [S2A_L1C_mask, test])
                return FAILURE
    # end TOOLBOX

    # common args:
    CFG = 'cfg'
    if args.GIP_L2A:
        # is it an absolute path?
        isFile = False
        test = os.path.normpath(args.GIP_L2A)
        if os.path.isfile(test):
            config.configFn = test
            isFile = True
        else:  # is it located in the config home dir?
            test = os.path.join(config.home, CFG, args.GIP_L2A)
            if os.path.isfile(test):
                config.configFn = test
                isFile = True
        if not isFile:
            logger.error('File does not exist: %s' % test)
            config.operationMode = 'INVALID'
            return config
    # same procedure for GIP_SC:
    if args.GIP_L2A_SC:
        isFile = False
        test = os.path.normpath(args.GIP_L2A_SC)
        if os.path.isfile(test):
            config.configSC = test
            isFile = True
        else:
            test = os.path.join(config.home, CFG, args.GIP_L2A_SC)
            if os.path.isfile(test):
                config.configSC = test
                isFile = True
        if not isFile:
            logger.error('File does not exist: %s' % test)
            config.operationMode = 'INVALID'
            return config
    # same procedure for GIP_AC:
    if args.GIP_L2A_AC:
        isFile = False
        test = os.path.normpath(args.GIP_L2A_AC)
        if os.path.isfile(test):
            config.configAC = test
            isFile = True
        else:
            test = os.path.join(config.home, CFG, args.GIP_L2A_AC)
            if os.path.isfile(test):
                config.configAC = test
                isFile = True
        if not isFile:
            logger.error('File does not exist: %s' % test)
            config.operationMode = 'INVALID'
            return config
    # same procedure for GIP_L2A_PB:
    if args.GIP_L2A_PB:
        isFile = False
        test = os.path.normpath(args.GIP_L2A_PB)
        if os.path.isfile(test):
            config.configPB = test
            isFile = True
        else:
            test = os.path.join(config.home, CFG, args.GIP_L2A_PB)
            if os.path.isfile(test):
                config.configPB = test
                isFile = True
        if not isFile:
            logger.error('File does not exist: %s' % test)
            config.operationMode = 'INVALID'
            return config

    if args.resolution == None:
        config.resolution = 0
    else:
        config.resolution = args.resolution

    config.scOnly  = args.sc_only
    config.crOnly  = args.cr_only
    config.raw     = args.raw
    config.tif     = args.tif
    config.logger  = logger
    return config


def postprocess(config):
    # SIMPC-1152, third remark: report file is unwanted.
    return True
    # fix for SIIMPC-555.1, UMW
    fnLogBase = os.path.basename(config.fnLog)
    try:
        fnLogIn = config.fnLog
        f = open(fnLogIn, 'a')
        f.write('</Sen2Cor_Level-2A_Report_File>')
        f.flush()
        f.close()
        fnLogOut = os.path.join(config.L2A_UP_DIR, fnLogBase)
        copyfile(fnLogIn, fnLogOut)
        return True
    except:
        config.logger.error('cannot copy report file: %s' % fnLogIn)
        return False

def main(args=None):
    import argparse
    config = L2A_Config(None)
    processorId = config.processorName +'. Version: '+ config.processorVersion + ', created: '+ config.processorDate + \
    ', supporting Level-1C product version 14.2 - ' + str(config.productVersion)
     
    parserPDGS = argparse.ArgumentParser(description=processorId  + '.',add_help=False)
    parserPDGS.add_argument('--mode', help='Mode: generate_datastrip, process_user_product, process_tile')
    namespace,dummy = parserPDGS.parse_known_args()
    parser = argparse.ArgumentParser(description=processorId  + '.',add_help=True)
    if namespace.mode == 'TOOLBOX' or namespace.mode is None:
        parser.add_argument('input_dir', help='Directory of Level-1C input')

    parser.add_argument('--mode', help='Mode: generate_datastrip, process_tile')
    parser.add_argument('--resolution', type=int, choices=[10, 20, 60], help='Target resolution, can be 10, 20 or 60m. If omitted, only 20 and 10m resolutions will be processed')
    parser.add_argument('--datastrip', help='Datastrip folder')
    parser.add_argument('--tile', help='Tile folder')
    parser.add_argument('--output_dir', help='Output directory')
    parser.add_argument('--work_dir', help='Work directory')
    parser.add_argument('--img_database_dir', help='Database directory for L1C input images')
    parser.add_argument('--res_database_dir', help='Database directory for results and temporary products')
    parser.add_argument('--processing_centre', help='Processing centre as regex: ^[A-Z_]{4}$, e.g "SGS_"')
    parser.add_argument('--archiving_centre', help='Archiving centre as regex: ^[A-Z_]{4}$, e.g. "SGS_"')
    parser.add_argument('--processing_baseline', help='Processing baseline in the format: "dd.dd", where d=[0:9]')
    parser.add_argument('--raw', action='store_true', help='Export raw images in rawl format with ENVI hdr')
    parser.add_argument('--tif', action='store_true', help='Export raw images in TIFF format instead of JPEG-2000')
    parser.add_argument('--sc_only', action='store_true', help='Performs only the scene classification at 60 or 20m resolution')
    parser.add_argument('--cr_only', action='store_true', help='Performs only the creation of the L2A product tree, no processing')
    parser.add_argument('--debug', action='store_true', help='Performs in debug mode')
    #parser.add_argument('--profile', action='store_true', help='Profiles the processor\'s performance')
    parser.add_argument('--GIP_L2A', help='Select the user GIPP')
    parser.add_argument('--GIP_L2A_SC', help='Select the scene classification GIPP')
    parser.add_argument('--GIP_L2A_AC', help='Select the atmospheric correction GIPP')
    parser.add_argument('--GIP_L2A_PB', help='Select the processing baseline GIPP')
    args = parser.parse_args()
    config = parseCommandLine(args)
    if args.debug:
            config.logLevel = 'DEBUG'
            
    if config.operationMode == 'INVALID':
        return FAILURE

    dt = datetime.utcnow().strftime('%Y%m%dT%H%M%S')
    config.setSchemes()

    # this is to set the version info from the L1C User Product:
    if not config.setProductVersion():
        return FAILURE

    if config.operationMode == 'TOOLBOX':
        if not config.readPreferences():
            return FAILURE
        if config.create_L2A_UserProduct():
            config.configure_L2A_UP_metadata()
        L2A_UP_ID =  os.path.join(config.logDir, config.L2A_UP_ID)
        logName = L2A_UP_ID + '_' + dt + '_report.xml'
        logDir = config.logDir
        if not os.path.exists(logDir):
            os.mkdir(logDir)
        config.fnLog = os.path.join(logDir, logName)

    elif config.operationMode == 'PROCESS_TILE':
        config.create_L2A_Tile()
        L2A_TILE_ID = os.path.join(config.work_dir, config.L2A_TILE_ID)
        logName = L2A_TILE_ID + '_' + dt + '_report.xml'
        config.fnLog = os.path.join(config.work_dir, logName)
 
    elif config.operationMode == 'GENERATE_DATASTRIP':
        L2A_DS_ID = os.path.join(config.work_dir, config.datastrip.replace('L1C', 'L2A'))
        logName = L2A_DS_ID + '_'+ dt + '_report.xml'
        config.fnLog = os.path.join(config.work_dir, logName)
        if not config.readPreferences():
            return FAILURE
 
    # create and initialize the base log system:
    logger = L2A_Logger('sen2cor', fnLog = config.fnLog\
                        , logLevel = config.logLevel\
                        , operation_mode = config.operationMode)
    config.logger = logger
    config.logger.stream('%s started ...' % processorId)
    if float32(config.productVersion) < 14.5:
        config.logger.stream('Old product version %2.1f detected, - will be updated to 14.5' % config.productVersion)
        config.logger.stream('Processing baseline will also be updated')
    else:
        config.logger.stream('Product version: %2.1f' % (config.productVersion))
    config.logger.stream('Operation mode: %s' % (config.operationMode))

    try:
        f = open(config.processingStatusFn, 'w')
        f.write('0.0\n')
        f.close()
    except:
        config.logger.error('cannot create process status file: %s' % config.processingStatusFn)
        return FAILURE

    if config.processingBaseline:
        config.logger.stream('Processing baseline: %05.2f' % config.processingBaseline)
    else:
        config.logger.error('No Processing baseline found.')    
    config.tStart = time()
    try:
        f = open(config.fnLog, 'w')
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<Sen2Cor_Level-2A_Report_File>\n')
        f.close()
    except:
        config.logger.error('cannot update the report file: %s' % config.fnLog)
        return FAILURE
    
    if config.operationMode == 'GENERATE_DATASTRIP':
        from L2A_ProcessDataStrip import L2A_ProcessDataStrip
        ds = L2A_ProcessDataStrip(config)
        if ds.generate():
            result = SUCCESS
        else:
            result = FAILURE
    else:
        # prepare the processing:
        if args.resolution == None:
            resolution = 0
        else:
            resolution = args.resolution
        config.setTimeEstimation(resolution)
        try:
            f = open(config.processingStatusFn, 'w')
            f.write('0.0\n')
            f.close()  
        except:
            config.logger.error('cannot create process status file: %s' % config.processingStatusFn)        
            return FAILURE
        
        if config.operationMode == 'PROCESS_TILE':
            from L2A_ProcessTilePdgs import L2A_ProcessTile
            config.resolution = resolution
            try:
                tile = L2A_ProcessTile(config)
                if tile.process() == True:
                    result = SUCCESS
                else:
                    result = FAILURE
            except Exception as e:
                if e.args[0] == 2:
                    if 'pic' in e.filename:
                        result = ALREADY_PROCESSED
                else:
                    logger = L2A_Logger('sen2cor', operation_mode=config.operationMode)
                    logger.stream(e, exc_info=True)
                    sys.exit(1)

        elif config.operationMode == 'TOOLBOX':
            from L2A_ProcessTileToolbox import L2A_ProcessTile
            config.resolution = resolution
            config.create_L2A_Datastrip()
            L2A_TILES = config.updateTiles()
            if not L2A_TILES:
                config.logger.error('no tile in GRANULE folder found')
                result = FAILURE
            else:
                S2A_mask = '*L2A_T*'
                for tile in L2A_TILES:
                    if (fnmatch.fnmatch(tile, S2A_mask) == False):
                        continue

                config.L2A_TILE_ID = tile
                try:
                    tile = L2A_ProcessTile(config)
                    if tile.process() == True:
                        result = SUCCESS
                    else:
                        result = FAILURE
                    if not postprocess(config):
                        result = FAILURE
                except Exception as e:
                    logger = L2A_Logger('sen2cor', operation_mode=config.operationMode)
                    logger.stream(e, exc_info=True)
                    result = FAILURE

                # final cleanup in Toolbox mode:
                import glob
                try:
                    files = glob.glob(os.path.join(config.L2A_UP_DIR, 'GRANULE', 'L2A_*', '*.h5'))
                    for f in files:
                        os.remove(f)
                except:
                    pass
                try:
                    os.remove(config.picFn)
                except:
                    pass

    if not config.logger:
        logger = L2A_Logger('sen2cor',operation_mode = config.operationMode)
        config.logger = logger

    if result == FAILURE:
        config.logger.stream('Progress[%]: 100.00 : Application terminated with at least one error.\n')
    elif result == ALREADY_PROCESSED:
        config.logger.stream('Progress[%]: 100.00 : Product already processed.\n')
        result = SUCCESS
    else:
        config.logger.stream('Progress[%]: 100.00 : Application terminated successfully.\n')
    
    return result

if __name__ == "__main__":
    sys.exit(main())

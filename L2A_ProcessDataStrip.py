#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys, os, logging, inspect
import fnmatch
from L2A_Manifest import L2A_Manifest
from L2A_XmlParser import L2A_XmlParser
from time import time,strftime
from datetime import datetime
from shutil import copyfile, copytree
from fileinput import filename
from lxml import etree, objectify
SUCCESS = 0
FAILURE = 1

class L2A_ProcessDataStrip(): 
    
    def __init__(self, config):
        self._logger = config.logger
        self._localTimestamp = time()
        
        self._L1C_DS_DIR = config.L1C_DS_DIR
        self._L1C_DS_ID = config.L1C_DS_ID
        self._L1C_DS_MTD_XML = config.L1C_DS_MTD_XML
        self._L2A_DS_DIR = None
        self._L2A_DS_ID = None
        self._L2A_DS_MTD_XML = None
        self._processing_centre = config.processing_centre
        self._archiving_centre  = config.archiving_centre
        self._scOnly = config.scOnly
        self._namingConvention = config.namingConvention
        self._operationMode = config.operationMode
        self.config = config
        self._pbStr = 'N%05.2f' % self.config.processingBaseline
        self._sensingStart = None
        self._time = None   
  
    def get_sc_only(self):
        return self._scOnly
 
    def set_sc_only(self, value):
        self._scOnly = value
 
    def del_sc_only(self):
        del self._scOnly
 
    def get_operation_mode(self):
        return self._operationMode
 
    def set_operation_mode(self, value):
        self._operationMode = value
 
    def del_operation_mode(self):
        del self._operationMode

    def get_naming_convention(self):
        return self._namingConvention
 
    def set_naming_convention(self, value):
        self._namingConvention = value
 
    def del_naming_convention(self):
        del self._namingConvention

    def get_processing_centre(self):
        return self._processing_centre
 
    def set_processing_centre(self, value):
        self._processing_centre = value
 
    def del_processing_centre(self):
        del self._processing_centre     

    def get_archiving_centre(self):
        return self._archiving_centre
 
    def set_archiving_centre(self, value):
        self._archiving_centre = value
 
    def del_archiving_centre(self):
        del self._archiving_centre
         
    def get_time(self):
        return self._time

    def set_time(self, value):
        self._time = value
        
    def del_time(self):
        del self._time
                 
    def get_logger(self):
        return self._logger
 
    def set_logger(self, value):
        self._logger = value
 
    def del_logger(self):
        del self._logger

    def get_l2a_ds_dir(self):
        return self._L2A_DS_DIR
 
    def set_l2a_ds_dir(self, value):
        self._L2A_DS_DIR = value
 
    def del_l2a_ds_dir(self):
        del self._L2A_DS_DIR
        
    def get_l2a_ds_id(self):
        return self._L2A_DS_ID
 
    def set_l2a_ds_id(self, value):
        self._L2A_DS_ID = value
 
    def del_l2a_ds_id(self):
        del self._L2A_DS_ID
        
    def get_l1c_ds_dir(self):
        return self._L1C_DS_DIR
 
    def set_l1c_ds_dir(self, value):
        self._L1C_DS_DIR = value
 
    def del_l1c_ds_dir(self):
        del self._L1C_DS_DIR
        
    def get_l1c_ds_id(self):
        return self._L1C_DS_ID
 
    def set_l1c_ds_id(self, value):
        self._L1C_DS_ID = value
 
    def del_l1c_ds_id(self):
        del self._L1C_DS_ID
        
    def get_l1c_ds_mtd_xml(self):
        return self._L1C_DS_MTD_XML
 
    def set_l1c_ds_mtd_xml(self, value):
        self._L1C_DS_MTD_XML = value
 
    def del_l1c_ds_mtd_xml(self):
        del self._L1C_DS_MTD_XML
        
    def get_l2a_ds_mtd_xml(self):
        return self._L2A_DS_MTD_XML
 
    def set_l2a_ds_mtd_xml(self, value):
        self._L2A_DS_MTD_XML = value
 
    def del_l2a_ds_mtd_xml(self):
        del self._L2A_DS_MTD_XML

    def get_config(self):
        return self.config
 
    def set_config(self, value):
        self.config = value
  
    def del_config(self):
        del self.config
        
    def __exit__(self):
            sys.exit(-1)

    #PROPERTY
    scOnly = property(get_sc_only, set_sc_only, del_sc_only, "scOnly's docstring")
    operationMode = property(get_operation_mode, set_operation_mode, del_operation_mode, "operation mode's docstring")
    namingConvention = property(get_naming_convention, set_naming_convention, del_naming_convention, "naming convention's docstring")
    logger = property(get_logger, set_logger, del_logger, "logger's docstring")
    processing_centre= property(get_processing_centre, set_processing_centre, del_processing_centre, "processing centre docstring")
    archiving_centre= property(get_archiving_centre, set_archiving_centre, del_archiving_centre, "archiving centre's docstring")
    L2A_DS_DIR= property(get_l2a_ds_dir, set_l2a_ds_dir, del_l2a_ds_dir, "L2A_DS_DIR's docstring")
    L2A_DS_ID= property(get_l2a_ds_id, set_l2a_ds_id, del_l2a_ds_id, "L2A_DS_ID's docstring")
    L2A_DS_MTD_XML = property(get_l2a_ds_mtd_xml, set_l2a_ds_mtd_xml, del_l2a_ds_mtd_xml, "L2A_DS_MTD_XML's docstring")
    L1C_DS_DIR= property(get_l1c_ds_dir, set_l1c_ds_dir, del_l1c_ds_dir, "L2A_DS_DIR's docstring")
    L1C_DS_ID= property(get_l1c_ds_id, set_l1c_ds_id, del_l1c_ds_id, "L1C_DS_ID's docstring")
    L2A_DS_MTD_XML = property(get_l2a_ds_mtd_xml, set_l2a_ds_mtd_xml, del_l2a_ds_mtd_xml, "L2A_DS_MTD_XML's docstring")
    config = property(get_config, set_config, del_config, "config's docstring")
    time = property(get_time, set_time, del_time, "config's docstring")

    def get_processing_centre_from_L1C_metadata(self):
        xp = L2A_XmlParser(self.config, 'DS1C')
        pi1c = xp.getTree('General_Info', 'Processing_Info')
        self.processing_centre = pi1c.PROCESSING_CENTER.text

        return self.processing_centre
    
    def add_new_features(self):
        xp = L2A_XmlParser(self.config, 'DS2A')
        if (xp.convert() == False):
            self.logger.fatal('error in converting datastrip metadata to level 2A')
        ti = xp.getTree('Image_Data_Info', 'Tiles_Information')
        for tile in ti.Tile_List.iterchildren():
            tileIdStr = tile.items()[0][1].replace('L1C','L2A')
            tileIdLst = [i for i in tileIdStr.split("_") if i != ""]
            if self.processing_centre:
                tileIdLst[5] = self.processing_centre
            else:
                tileIdLst[5] = self.get_processing_centre_from_L1C_metadata()

            tileIdLst[6] = strftime('%Y%m%dT%H%M%S', self.time.timetuple())
            tileIdLst[-1] = self._pbStr
            tileIdStr = "_".join(tileIdLst)
            newElement = etree.Element('Tile', tileId = tileIdStr)
            ti.Tile_List.replace(tile,newElement)
            
        # writing features in L2A datastrip metadata
        pi2a = xp.getTree('General_Info', 'Processing_Info')
        pi2a.UTC_DATE_TIME = strftime('%Y-%m-%dT%H:%M:%S.', self.time.timetuple()) + str(self.time.microsecond)[:3]+'Z'
        if self.processing_centre:    
            pi2a.PROCESSING_CENTER = self.processing_centre

        auxdata = xp.getTree('Auxiliary_Data_Info', 'GIPP_List')
        if self.config.configSC:
            dummy, configSC = os.path.split(self.config.configSC)
            gippFn = etree.Element('GIPP_FILENAME', type='GIP_L2ACSC',
                                   version='%04d' % long(self.config.processorVersion.replace('.', '')))
            gippFn.text = configSC.split('.')[0]
            auxdata.append(gippFn)

        if self.config.configAC:
            dummy, configAC = os.path.split(self.config.configAC)
            gippFn = etree.Element('GIPP_FILENAME', type='GIP_L2ACAC',
                                   version='%04d' % long(self.config.processorVersion.replace('.', '')))
            gippFn.text = configAC.split('.')[0]
            auxdata.append(gippFn)

        if self.config.processingBaseline:
            baseline = '{:05.2f}'.format(float(self.config.processingBaseline))
            gippFn = etree.Element('GIPP_FILENAME', type='GIP_PROBA2', version=self._pbStr[1:].replace('.', ''))
        elif self.config.configPB:
            pb = L2A_XmlParser(self.config, 'PB_GIPP')
            baseline = pb.getTree('DATA', 'Baseline_Version')
            gippFn = etree.Element('GIPP_FILENAME', type='GIP_PROBA2', version=baseline.text.replace('.', ''))

        dummy, configPB = os.path.split(self.config.configPB)
        gippFn.text = configPB.split('.')[0]
        auxdata.append(gippFn)

        pi2a.PROCESSING_BASELINE = baseline
        # SIIMPC-1255 , GNR : update of Sen2cor processing baseline in the datatake ID
        di2a = xp.getTree('General_Info', 'Datatake_Info')
        datatakeIdentifier = di2a.attrib['datatakeIdentifier'].split('_N')
        datatakeIdentifier[-1] = '{:05.2f}'.format(float(baseline))
        di2a.attrib['datatakeIdentifier'] = '_N'.join(datatakeIdentifier)
                
        #  SIIMPC-1152 RBS Patch: Fix GNR           
        pic = xp.getTree('Image_Data_Info', 'Radiometric_Info')
        pic.QUANTIFICATION_VALUE.tag = 'QUANTIFICATION_VALUES_LIST'
        qvl = objectify.Element('QUANTIFICATION_VALUES_LIST')
        qvl.BOA_QUANTIFICATION_VALUE = str(int(self.config._dnScale))
        qvl.BOA_QUANTIFICATION_VALUE.attrib['unit'] = 'none'
        qvl.AOT_QUANTIFICATION_VALUE = str(self.config.L2A_AOT_QUANTIFICATION_VALUE)
        qvl.AOT_QUANTIFICATION_VALUE.attrib['unit'] = 'none'
        qvl.WVP_QUANTIFICATION_VALUE = str(self.config.L2A_WVP_QUANTIFICATION_VALUE)
        qvl.WVP_QUANTIFICATION_VALUE.attrib['unit'] = 'cm'
        pic.QUANTIFICATION_VALUES_LIST = qvl

        lfn = etree.Element('LUT_FILENAME')
        lfn.text = 'None'
        auxinfo = xp.getRoot('Auxiliary_Data_Info')

        if (xp.getTree('Auxiliary_Data_Info', 'GRI_List')) == False:
            gfn = xp.getTree('Auxiliary_Data_Info', 'GRI_FILENAME')
            del gfn[:]
            gl = objectify.Element('GRI_List')
            gl.append(gfn)
            auxinfo.append(gl)
        try:
            ll = xp.getTree('Auxiliary_Data_Info', 'LUT_List')
            ll.append(lfn)
        except:
            ll = objectify.Element('LUT_List')
            ll.append(lfn)
            auxinfo.append(ll)
        try:
            esacciWaterBodies = self.config.esacciWaterBodiesReference
            esacciWaterBodies = os.path.join(self.config.auxDir, esacciWaterBodies)
            esacciLandCover = self.config.esacciLandCoverReference
            esacciLandCover = os.path.join(self.config.auxDir, esacciLandCover)
            esacciSnowConditionDir = self.config.esacciSnowConditionDirReference
            esacciSnowConditionDir = os.path.join(self.config.auxDir, esacciSnowConditionDir)
            item = xp.getTree('Auxiliary_Data_Info', 'SNOW_CLIMATOLOGY_MAP')
            item._setText(self.config.snowMapReference)
            item = xp.getTree('Auxiliary_Data_Info', 'ESACCI_WaterBodies_Map')
            if ((os.path.isfile(esacciWaterBodies)) == True):
                item._setText(self.config.esacciWaterBodiesReference)
            else:
                item._setText('None')
            item = xp.getTree('Auxiliary_Data_Info', 'ESACCI_LandCover_Map')
            if ((os.path.isfile(esacciLandCover)) == True):
                item._setText(self.config.esacciLandCoverReference)
            else:
                item._setText('None')
            item = xp.getTree('Auxiliary_Data_Info', 'ESACCI_SnowCondition_Map_Dir')
            if (os.path.isdir(esacciSnowConditionDir)) == True:
                item._setText(self.config.esacciSnowConditionDirReference)
            else:
                item._setText('None')
        except:
            item = etree.Element('SNOW_CLIMATOLOGY_MAP')
            item.text = self.config.snowMapReference
            auxinfo.append(item)
            item = etree.Element('ESACCI_WaterBodies_Map')
            if ((os.path.isfile(esacciWaterBodies)) == True):
                item.text = self.config.esacciWaterBodiesReference
            else:
                item.text = 'None'
            auxinfo.append(item)
            item = etree.Element('ESACCI_LandCover_Map')
            if ((os.path.isfile(esacciLandCover)) == True):
                item.text = self.config.esacciLandCoverReference
            else:
                item.text = 'None'
            auxinfo.append(item)
            item = etree.Element('ESACCI_SnowCondition_Map_Dir')
            if (os.path.isdir(esacciSnowConditionDir)) == True:
                item.text = self.config.esacciSnowConditionDirReference
            else:
                item.text = 'None'
            auxinfo.append(item)

        # Addon UMW 180326: set production DEM with L2A info:
        item = xp.getTree('Auxiliary_Data_Info', 'PRODUCTION_DEM_TYPE')
        if self.config.demDirectory == 'NONE':
            item._setText('None')
        else:
            item._setText(self.config.demReference)

        self.time = datetime.utcnow()        
        ai2a = xp.getTree('General_Info', 'Archiving_Info')
        ai2a.ARCHIVING_TIME = strftime('%Y-%m-%dT%H:%M:%S.', self.time.timetuple()) + str(self.time.microsecond)[:3]+'Z'
        if self.archiving_centre:    
            ai2a.ARCHIVING_CENTRE = self.archiving_centre 

        xp.export()
        xp.validate()
            
        return True

    def generate(self):    
        DATASTRIP = 'DATASTRIP' 
        self.logger.stream('Progress[%]:  0.00 : Generating datastrip metadata')
        self.L1C_DS_MTD_XML = self.config.L1C_DS_MTD_XML
        
        #input
        try:
            xp = L2A_XmlParser(self.config, 'DS1C')
            if not xp.validate():
                self.logger.fatal('Incorrect datastrip L1C xml format')
                return FAILURE        
            tr = xp.getTree('General_Info', 'Datastrip_Time_Info')
            sensingStart = tr.DATASTRIP_SENSING_START.text.split('Z')[0]
            sensingStart = datetime.strptime(sensingStart,'%Y-%m-%dT%H:%M:%S.%f')
            self._sensingStart = strftime('%Y%m%dT%H%M%S', sensingStart.timetuple())
        except:
            self.logger.fatal('no sensing start in datastrip')
            
        self.time = datetime.utcnow()
        generationTimeStr = strftime('%Y%m%dT%H%M%S', self.time.timetuple())
        
        if self.operationMode == 'GENERATE_DATASTRIP':
            self.L2A_DS_DIR = self.config.output_dir
            self.L2A_DS_ID = '_'.join(['S'+self.config.spacecraftName.split('-')[-1],'OPER_MSI_L2A_DS',self.processing_centre, \
                            generationTimeStr,'S'+self._sensingStart,self._pbStr])
        else: #TOOLBOX
            if self.processing_centre == None:
                ai = xp.getTree('General_Info','Processing_Info')
                self.processing_centre = ai.PROCESSING_CENTER.text
            self.L2A_DS_DIR = os.path.join(self.config.L2A_UP_DIR, DATASTRIP)
            self.L2A_DS_ID = 'DS_' + self.processing_centre + '_' + generationTimeStr + '_S' + self._sensingStart
            self.L2A_DS_MTD_XML = os.path.join(self.L2A_DS_DIR, self.L2A_DS_ID, 'MTD_DS.xml')

        #output
        newdir = os.path.join(self.L2A_DS_DIR, self.L2A_DS_ID)
        if self.operationMode == 'GENERATE_DATASTRIP':
            olddir = os.path.join(self.L1C_DS_DIR, self.L1C_DS_ID)
            if not os.path.isdir(newdir):
                os.makedirs(newdir)
                os.makedirs(os.path.join(newdir,'QI_DATA'))
            copyfile(self.L1C_DS_MTD_XML,os.path.join(newdir,os.path.basename(self.L1C_DS_MTD_XML)))
   
        else: #TOOLBOX
            olddir = os.path.join(self.L2A_DS_DIR,self.L1C_DS_ID)
            if os.path.exists(olddir):
                os.rename(olddir,newdir)
                self.logger.info('datastrip directory is: ' + newdir)
                qiDir = os.path.join(newdir, 'QI_DATA')
                filelist = sorted(os.listdir(qiDir))
                mask = '*.xml'
                for fnIn in filelist:
                    if fnmatch.fnmatch(fnIn, mask):
                        os.remove(os.path.join(qiDir, fnIn))

            if not os.path.isdir(newdir):
                self.logger.error('cannot find L2A datastrip directory')
                return False
        
        #find L2A datastrip metadada, rename and change it:
        L2A_DS_SUBDIR = newdir
        L1C_DS_MTD_XML = os.path.basename(self.L1C_DS_MTD_XML)
        L2A_DS_MTD_XML = 'MTD_DS.xml'
        oldfile = os.path.join(L2A_DS_SUBDIR, L1C_DS_MTD_XML)
        newfile = os.path.join(L2A_DS_SUBDIR, L2A_DS_MTD_XML)
        
        self.config.L2A_DS_MTD_XML = newfile
        self.L2A_DS_MTD_XML = newfile
        os.rename(oldfile, newfile)

        found = self.add_new_features()
        if not found:
            self.logger.fatal('no subdirectory in datastrip')
        else:
            self.logger.stream('L1C datastrip found, L2A datastrip successfully generated')

        return found
from numpy import *
import os, sys
from lxml import etree, objectify
from multiprocessing import Lock
l = Lock()

class L2A_XmlParser(object):
    def __init__(self, config, product):
        self._config = config
        self._logger = config.logger
        self._product = product
        self._xmlFn = None
        self._xmlName = None
        self._root = None
        self._tree = None
        self._scheme = None

        if (product == 'GIPP'):
            self._xmlFn = config.configFn
            self._scheme = config.gippScheme2a
        elif (product == 'PB_GIPP'):
            self._xmlFn = config.configPB
            self._scheme = config.gippSchemePb
        elif (product == 'SC_GIPP'):
            self._xmlFn = config.configSC
            self._scheme = config.gippSchemeSc
        elif (product == 'AC_GIPP'):
            self._xmlFn = config.configAC
            self._scheme = config.gippSchemeAc
        elif (product == 'UP1C'):
            self._xmlFn = config.L1C_UP_MTD_XML
            self._scheme = config.upScheme1c
        elif (product == 'UP2A'):
            self._xmlFn = config.L2A_UP_MTD_XML
            self._scheme = config.upScheme2a
        elif (product == 'DS1C'):
            self._xmlFn = config.L1C_DS_MTD_XML
            self._scheme = config.dsScheme1c
        elif (product == 'DS2A'):
            self._xmlFn = config.L2A_DS_MTD_XML
            self._scheme = config.dsScheme2a
        elif (product == 'T1C'):
            self._xmlFn = config.L1C_TILE_MTD_XML
            self._scheme = config.tileScheme1c
        elif (product == 'T2A'):
            self._xmlFn = config.L2A_TILE_MTD_XML
            self._scheme = config.tileScheme2a
        elif (product == 'Manifest'):
            self._xmlFn = config.L2A_MANIFEST_SAFE
            self._scheme = config.manifestScheme
        elif (product == 'HTML'):
            self._xmlFn = config.UP_INDEX_HTML
        elif (product == 'INSPIRE'):
            self._xmlFn = config.INSPIRE_XML
        else:
            self._logger.fatal('wrong identifier for xml structure: ' + product)

        if product != 'HTML':
            self.setRoot()

    def setRoot(self):
        l.acquire()
        try:
            doc = objectify.parse(self._xmlFn)
            self._root = doc.getroot()
            return True
        except:
            return False
        finally:
            l.release()

    def getRoot(self, key=None):
        l.acquire()
        try:
            if key == None:
                root = self._root
            else:
                root = self._root[key]
            return root
        except:
            return False
        finally:
            l.release()

    def getNode(self, key):
        l.acquire()
        try:
            tree = self._root[key]
            return tree
        except:
            return False
        finally:
            l.release()

    def getTree(self, key, subkey):
        l.acquire()
        try:
            tree = self._root[key]    
            return tree['{}' + subkey]
        except:
            return False
        finally:
            l.release()

    def validate(self):
        """ Validator for the metadata.

            :return: true, if metadata are valid.
            :rtype: boolean

        """
        fn = os.path.basename(self._xmlFn)
        self._logger.info('validating metadata file %s against scheme' % fn)
        l.acquire()
        try:
            schema = etree.XMLSchema(file = os.path.join(self._config.configDir, self._scheme))
            parser = etree.XMLParser(schema = schema)
            objectify.parse(self._xmlFn, parser)
            self._logger.info('metadata file is valid')                
            ret = True
        except etree.XMLSyntaxError, err:
            self._logger.stream('Syntax error in metadata, see report file for details.')
            self._logger.error('Schema file: %s' % self._scheme)
            self._logger.error('Details: %s' % str(err))
            ret = False
        except etree.XMLSchemaError, err:
            self._logger.stream('Error in xml schema, see report file for details.')
            self._logger.error('Schema file: %s' % self._scheme)
            self._logger.error('Details: %s' % str(err))
            ret = False
        except etree.XMLSchemaParseError, err:
            self._logger.stream('Error in parsing xml schema, see report file for details.')
            self._logger.error('Schema file: %s' % self._scheme)
            self._logger.error('Details: %s' % str(err))
            ret = False
        except etree.XMLSchemaValidateError, err:
            self._logger.stream('Error in validating scheme, see report file for details.')
            self._logger.error('Schema file: %s' % self._scheme)
            self._logger.error('Details: %s' % str(err))
            ret = False
        except:
            self._logger.stream('Unspecific Error in metadata.\n')
            self._logger.error('unspecific error in metadata')
            err = 'not available';
            ret = False
        finally:                  
            l.release()

        if not ret:
            self._logger.debug('Parsing error:')
            self._logger.debug('Schema file: %s' % self._scheme)
            self._logger.debug('Details: %s' % str(err))

        return ret

    def append(self, key, value):
        ret = False
        l.acquire()
        try:
            e = etree.Element(key)
            e.text = value
            self._tree.append(e)
            return True
        except:
            return False
        finally:
            l.release()

    def export(self):
        l.acquire()
        try:
            outfile = open(self._xmlFn, 'w')
            objectify.deannotate(self._root, xsi_nil=True, cleanup_namespaces=True)
            outstr = etree.tostring(self._root, pretty_print=True, encoding='UTF-8', xml_declaration=True)
            outfile.write(outstr)        
            outfile.close()
        finally:
            l.release()

        return self.setRoot()            

    def convert(self):
        import codecs, re
        objectify.deannotate(self._root, xsi_nil=True, cleanup_namespaces=True)
        outstr = etree.tostring(self._root, pretty_print=True)
        outstr = outstr.replace('-1C', '-2A')
        outstr = outstr.replace('psd-12', 'psd-14')
        outstr = outstr.replace('<SENSOR_QUALITY_FLAG>',
                                '<quality_check checkType="SENSOR_QUALITY">')
        outstr = outstr.replace('<GEOMETRIC_QUALITY_FLAG>',
                                '<quality_check checkType="GEOMETRIC_QUALITY">')
        outstr = outstr.replace('<GENERAL_QUALITY_FLAG>',
                                '<quality_check checkType="GENERAL_QUALITY">')
        outstr = outstr.replace('<FORMAT_CORRECTNESS_FLAG>',
                                '<quality_check checkType="FORMAT_CORRECTNESS">')
        outstr = outstr.replace('<RADIOMETRIC_QUALITY_FLAG>',
                                '<quality_check checkType="RADIOMETRIC_QUALITY">')
        outstr = outstr.replace('</SENSOR_QUALITY_FLAG>', '</quality_check>')
        outstr = outstr.replace('</GEOMETRIC_QUALITY_FLAG>', '</quality_check>')
        outstr = outstr.replace('</GENERAL_QUALITY_FLAG>', '</quality_check>')
        outstr = outstr.replace('</FORMAT_CORRECTNESS_FLAG>', '</quality_check>')
        outstr = outstr.replace('</RADIOMETRIC_QUALITY_FLAG>', '</quality_check>')
        outstr = outstr.replace('GRANULE/L1C', 'GRANULE/L2A')
        if self._product == 'UP2A':
            outstr = outstr.replace('QUANTIFICATION_VALUE', 'QUANTIFICATION_VALUES_LIST')
        elif self._product == 'INSPIRE':
            try: # replace user product string:
                strBegin = outstr.find('L1C_')
                strEnd = outstr.find('.SAFE')
                if (strBegin != -1) and (strEnd != -1):
                    outstr[strBegin-7:strEnd+5] = self._config.L2A_UP_ID
            except:
                pass
        l.acquire()
        try:
            outfile = codecs.open(self._xmlFn, 'w', 'utf-8')
            outfile.write('<?xml version="1.0"  encoding="UTF-8"?>\n')
            outfile.write(outstr)
            outfile.close()
        except:
            self._logger.stream('Error in writing file: %s' % self._xmlFn)
            self._logger.error('error in writing file: %s' % self._xmlFn)
        finally:
            l.release()
 
        return self.setRoot()

    def convertUpIndex(self):
        fn_inp = self._config.UP_INDEX_HTML
        fn_out = self._config.UP_INDEX_HTML + '_out'
        fp_out = open(fn_out, 'w')
        with open(fn_inp, 'r') as fp_inp:
            try:
                lines = iter(fp_inp.readlines())
                for current_line in lines:
                    strBegin = current_line.find('L1C_')
                    strEnd = current_line.find('.SAFE')
                    if (strBegin != -1) and (strEnd != -1):
                        current_line = current_line.replace(current_line[strBegin - 7:strEnd + 5],
                           self._config.L2A_UP_ID)
                    current_line = current_line.replace('L1C_', 'L2A_')
                    current_line = current_line.replace('Level-1C', 'Level-2A')
                    current_line = current_line.replace('S2MSI1C', 'S2MSI2A')
                    fp_out.writelines(current_line)
            except:
                self._logger.stream('Error in converting file: %s' % fn_inp)
                self._logger.error('error in converting file: %s' % fn_inp)
                return False

        fp_inp.close()
        fp_out.close()
        try:
            os.rename(fn_out, fn_inp)
        except:
            self._logger.stream('Error in updating file: %s' % fn_inp)
            self._logger.error('error in updating file: %s' % fn_inp)
            return False

        return True
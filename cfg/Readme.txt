Changes Log:

***********
          *
XSD V7.1  *
          * 
***********
Inventory_Metadata.xsd - V7.1
***************************
Removed "Acquisition_Stationold" metadata.



***********
          *
XSD V7.2  *
          * 
***********
dimap.xsd - V7.2
***************************
A_PRODUCT_OPTION complexType:
1. added maxOccurs="13"to the "BAND_NAME" element
2. moved "productLevel" attribute from "Aux" node to "Aux_List" node
3. defined DATASTRIP_ID element as "item:DATASTRIP_ID"
4. added A_QUALITY_INDICATORS_INFO_USER_PROD_L1B complexType
5. added A_QUALITY_SUMMARY_L1B_USER complexType

DIMAP_user_product_Level-1B.xsd - V7.2
*************************************
Re-defined Quality_Indicators_Info element as type="dimap:A_QUALITY_INDICATORS_INFO_USER_PROD_L1B

SAFE_user_product_Level-1B.xsd - V7.2
*************************************
Re-defined Quality_Indicators_Info element as type="dimap:A_QUALITY_INDICATORS_INFO_USER_PROD_L1B



***********
          *
XSD V7.3  *
          * 
***********
dimap.xsd - V7.3
***************************
1. AN_ANCILLARY_DATA_DSL0: modified to have a unique Satellite_Ancillary_Data_Info description for all level of processing (L0/L1A/L1B/L1C)
2. A_SOURCE_PACKET_DESCRIPTION_GRL0: replaced detector_Id and band_Id with detectorId and bandId

DIMAP_Level-1A_DataStrip.xsd - v7.3
****************************************
Replaced <xs:element name="Satellite_Ancillary_Data_Info" type="dimap:AN_ANCILLARY_DATA_DSL1AL1B"/> with
<xs:element name="Satellite_Ancillary_Data_Info" type="dimap:AN_ANCILLARY_DATA_DSL0"/>

DIMAP_Level-1B_DataStrip.xsd - v7.3
****************************************
Replaced <xs:element name="Satellite_Ancillary_Data_Info" type="dimap:AN_ANCILLARY_DATA_DSL1AL1B"/> with
<xs:element name="Satellite_Ancillary_Data_Info" type="dimap:AN_ANCILLARY_DATA_DSL0"/>



***********
          *
XSD V8.0  *
          * 
***********
Renamed the schemas filename:
User_Product_S2_Level-0.xsd -> S2_User_Product_Level-0_Structure.xsd
User_Product_S2_Level-1A.xsd -> S2_User_Product_Level-1A_Structure.xsd
User_Product_S2_Level-1B.xsd -> S2_User_Product_Level-1B_Structure.xsd
User_Product_S2_Level-1C.xsd -> S2_User_Product_Level-1C_Structure.xsd
DIMAP_User_Product_S2_Level-0.xsd -> S2_User_Product_Level-0_Metadata.xsd
DIMAP_User_Product_S2_Level-1A.xsd -> S2_User_Product_Level-1A_Metadata.xsd
DIMAP_User_Product_S2_Level-1B.xsd -> S2_User_Product_Level-1B_Metadata.xsd
DIMAP_User_Product_S2_Level-1C.xsd -> S2_User_Product_Level-1C_Metadata.xsd

dimap.xsd - V8.0
***************************
2. AN_IMAGE_DATA_INFO_DSL1A: added Product_Compression node
3. AN_IMAGE_DATA_INFO_DSL1B: added Product_Compression node
4. AN_IMAGE_DATA_INFO_DSL1C: added Product_Compression node
5. A_RADIOMETRIC_DATA_L1A: added Spectral_Information_List node
6. A_RADIOMETRIC_DATA_L1B: added Spectral_Information_List node
7. A_RADIOMETRIC_DATA_L1C: added Spectral_Information_List node
8. A_PRODUCT_INFO_USERL1AL1B: added Spectral_Information_List node
9. A_PRODUCT_INFO_USERL1C: added Spectral_Information_List node
10. A_PRODUCT_INFO_USERL1AL1B_SAFE: added Spectral_Information_List node
11. A_PRODUCT_INFO_USERL1C_SAFE: added Spectral_Information_List node
13. A_MSI_OPERATION_MODE: modified to add all missing operation modes
14. A_PRODUCT_OPTIONS: modified to align DO to SAD
15. A_GENERAL_INFO_L1C: modified SENSING_TIME type (date_time:AN_UTC_DATE_TIME_TWO)
16. Product_Organisation: modified to have a User product description based on granules instead on datastrip 
17. AN_AUXILIARY_DATA_INFO_USERL0L1A: removed IERS_Bulletin_Info and DataStrip_Generation_Info nodes
18. AN_AUXILIARY_DATA_INFO_USERL0L1A: Moved PHYSICAL_GAINS and REFERENCE_BAND from Auxiliary_Data_Info to General_Info
19. AN_AUXILIARY_DATA_INFO_USERL1B: removed IERS_Bulletin_Info and DataStrip_Generation_Info nodes
20. renamed A_PRODUCT_INFO_USERL1AL1B as A_PRODUCT_INFO_USERL1B
21. AN_AUXILIARY_DATA_INFO_USERL1B: moved Restoration_Parameters and Equalization_Parameters from Auxiliary_Data_Info to General_Info 
22. AN_AUXILIARY_DATA_INFO_USERL1C: removed IERS_Bulletin_Info and DataStrip_Generation_Info nodes
23. AN_AUXILIARY_DATA_INFO_USERL1C: Moved PHYSICAL_GAINS and REFERENCE_BAND from Auxiliary_Data_Info to General_Info
24. A_PRODUCT_INFO: modified Product_Organization node


Inventory_Metadata.xsd - V8.0
********************************
Removed "List_Of_Gaps" metadata
Added pattern in Validity_Start/Validity_Sop metadata


PDI_S2_Level-1C_Tile.xsd - v8.0
*************************************
Added AUX_DATA


item.xsd
*************************************
Added pattern in DATASTRIP_ID definition

data_time
*************************************
Added AN_UTC_DATE_TIME_TWO simpleType



***********
          *
XSD V9.0  *
          * 
***********
dimap.xsd - V9.0
***************************
1. A_MASK_LIST: modified annotations and added possible values for mask file types
2. NUMBER_OF_T00_DEGRADED_PACKETS renamed as NUMBER_OF_TOO_DEGRADED_PACKETS
3. A_PRODUCT_INFO_USERL0: added ON_BOARD_COMPRESSION_MODE metadata
4. AN_ACQUISITION_CONFIGURATION: changed metadataLevel on Active_Detectors_List from Expertise to Brief
5. moved Image_Display_Order node and QUANTIFICATION_VALUE field from AN_AUXILIARY_DATA_INFO_DSL1C to A_RADIOMETRIC_DATA_L1C
6. moved Image_Display_Order node from AN_AUXILIARY_DATA_INFO_DSL1A to A_RADIOMETRIC_DATA_L1A
7. moved Image_Display_Order node from AN_AUXILIARY_DATA_INFO_DSL1B to A_RADIOMETRIC_DATA_L1B
8. added A_QUALITY_SUMMARY_L0_L1A_USER
9. added A_QUALITY_SUMMARY_L1A_L1B_USER
10. added A_PROCESSING_SPECIFIC_PARAMETERS complexType
11.A_GENERAL_INFO_L0_L1A_L1B: added Processing_Specific_Parameters field
12.A_GENERAL_INFO_L1C: added Processing_Specific_Parameters field
13.A_GENERAL_INFO_DS: added Processing_Specific_Parameters field
14.AN_IMAGE_DATA_INFO_DSL1C: moved TileId attribute from Tile_List to Tile


item.xsd
*************************************
1. Modified DATATAKE_ID definition to include the Processing Baseline sub-field
2. added IMAGE_ID simpleType


S2_User_Product_Level-1C_Structure.xsd - V9.0
***********************************************
Modified to add AUX_DATA at GRANULE level


S2_User_Product_Level-0_Metadata.xsd - V9.0
***********************************************
modified Quality_Indicator_Info element as "dimap:A_QUALITY_INDICATORS_INFO_USER_PROD_L0_L1A" type


S2_User_Product_Level-1A_Metadata.xsd - V9.0
***********************************************
modified Quality_Indicator_Info element as "dimap:A_QUALITY_INDICATORS_INFO_USER_PROD_L0_L1A" type


S2_User_Product_Level-1B_Metadata.xsd - V9.0
***********************************************
modified Quality_Indicator_Info element as "dimap:A_QUALITY_INDICATORS_INFO_USER_PROD_L1B_L1C" type


S2_User_Product_Level-1C_Metadata.xsd - V9.0
***********************************************
modified Quality_Indicator_Info element as "dimap:A_QUALITY_INDICATORS_INFO_USER_PROD_L1B_L1C" type


Renamed the schemas filename:
PDI_S2_Level-0_Granule.xsd -> S2_PDI_Level-0_Granule_Structure.xsd
PDI_S2_Level-1A_Granule.xsd -> S2_PDI_Level-1A_Granule_Structure.xsd
PDI_S2_Level-1B_Granule.xsd -> S2_PDI_Level-1B_Granule_Structure.xsd
PDI_S2_Level-1C_Tile.xsd -> S2_PDI_Level-1C_Tile_Structure.xsd
DIMAP_S2_Level-0_Granule.xsd -> S2_PDI_Level-0_Granule_Metadata.xsd
DIMAP_S2_Level-1A_Granule.xsd -> S2_PDI_Level-1A_Granule_Metadata.xsd
DIMAP_S2_Level-1B_Granule.xsd -> S2_PDI_Level-1B_Granule_Metadata.xsd
DIMAP_S2_Level-1C_Tile.xsd -> S2_PDI_Level-1C_Tile_Metadata.xsd
PDI_S2_Level-0_Datastrip.xsd -> S2_PDI_Level-0_Datastrip_Structure.xsd
PDI_S2_Level-1A_Datastrip.xsd -> S2_PDI_Level-1A_Datastrip_Structure.xsd
PDI_S2_Level-1B_Datastrip.xsd -> S2_PDI_Level-1B_Datastrip_Structure.xsd
PDI_S2_Level-1C_Datastrip.xsd -> S2_PDI_Level-1C_Datastrip_Structure.xsd
DIMAP_S2_Level-0_Datastrip.xsd -> S2_PDI_Level-0_Datastrip_Metadata.xsd
DIMAP_S2_Level-1A_Datastrip.xsd -> S2_PDI_Level-1A_Datastrip_Metadata.xsd
DIMAP_S2_Level-1B_Datastrip.xsd -> S2_PDI_Level-1B_Datastrip_Metadata.xsd
DIMAP_S2_Level-1C_Datastrip.xsd -> S2_PDI_Level-1C_Datastrip_Metadata.xsd


***********
          *
XSD V10   *
          * 
***********
dimap.xsd - V10
***************************
1.A_PRODUCT_OPTIONS: removed "<xs:choice>" step to have more of one User download options 
2.AN_AUXILIARY_DATA_INFO_DSL1C: added PRODUCTION_DEM_TYPE, IERS_BULLETIN_FILENAME, GRI_FILENAME to aligns PSD-XSD to PSD 
3.A_GENERAL_INFO_L1C: added metadataLevel attribute to SENSING_TIME tag to aligns PSD-XSD to PSD
4.A_PRODUCT_INFO_USERL1A, A_PRODUCT_INFO_USERL1B, A_PRODUCT_INFO_USERL1C: put Spectral_Information_List as optional node 
because it is optional at PDI level  


item.xsd - V10
*************************************
1.Removed Processing Baseline from PVI_ID definition to align the schema to the PSD
2.Added GRANULE_TILE_ID to have a single ID to be used to reference at User Product level all Granules/Tiles
3.Modified IMAGE_ID regex to include Band Index = 8A
4.Modified GIPP_ID, DEM_ID, GRI_ID, IERS_ID and ECMWF_ID regex to include the "S2_" mission ID applicable for satellite independent files


***********
          *
XSD V11   *
          * 
***********
dimap.xsd - V11
***************************
1. Updated annotation for SENSING_TIME metadata (A_GENERAL_INFO_L0_L1A_L1B): "Time stamp of the first line of the Granule" (see R18 PSD PIRN)
2. Removed additional blank in "A_MSI_OPERATION_MODE" definition (see R20 PSD PIRN)
3. Added QL_FOOTPRINT element in A_GRANULE_POSITION complexType (see R23 PSD PIRN)
4. Removed DATATAKE_SENSING_STOP element from A_DATATAKE_IDENTIFICATION complexType (see R24 PSD PIRN)
5. Added PRODUCT_START_TIME & PRODUCT_STOP_TIME to A_PRODUCT_INFO complexType (see R24 PSD PIRN)
6. Added Area_Of_Inteest element into A_PRODUCT_OPTIONS complexType (see R25 PSD PIRN)
7. Updated annotation for REF_QL_IMAGE in A_QUICKLOOK_DESCRIPTOR (see R26 PSD PIRN)
8. Updated ANC_DATA_REF definition (see R27 PSD PIRN)


item.xsd - V11
*************************************
1.Updated POD_ID definition to be compliant to the current applicable POD-ICD (removed orbit from pattern, see R19 PSD PIRN)

Inventory_Metadata.xsd - V11
*****************************
1. Update to remove all references to SAD PDI (see R21 PSD PIRN)


SAFE xfdu.xsd - see S2-PDGS-TAS-DI-PSD-V11_SAFE.zip (see R22 PSD PIRN)
**********************************************************
Updated xfdu.xsd (and related examples of manifest.safe) specific for:
- L1A/L1B/L1C GR/Tile PDI
- L1A/L1B/L1C User Products
in order to correctly reference the mask files (band dependent) in the products



***********
          *
XSD V12   *
          * 
***********
dimap.xsd - V12
***************************
1.(see R28 in the PIRN) 
added "Reflectance_Conversion" node in "A_RADIOMETRIC_DATA_L1C" complexType and "A_PRODUCT_INFO_USERL1C" complexType.

2.(see R29 in the PIRN) 
Removed additional blank at the end of "FULL_SWATH_DATATAKE" element 

3.(see R30 in the PIRN) 
updated definition of "SPACECRAFT_NAME" element of "A_DATATAKE_IDENTIFICATION" complexType
<xs:element name="SPACECRAFT_NAME">
	<xs:annotation>
		<xs:documentation>Sentinel-2 Spacecraft name</xs:documentation>
	</xs:annotation>
	<xs:simpleType>
		<xs:restriction base="xs:string">
		<xs:enumeration value="Sentinel-2A"/>
		<xs:enumeration value="Sentinel-2B"/>
		</xs:restriction>
	</xs:simpleType>
</xs:element>

4.(see R31 in the PIRN)
Updated Processing_Info node in "A_GENERAL_INFO_DS"

5.(see R32 in the PIRN)
put "metadataLevel = Expertise" on "ACTIVE_DETECTOR" element

6.(see R34 in the PIRN)
- Removed "QUATERINION_STATUS" from "A_RAW_ATTITUDE" complexType
- Removed "QUATERINION_STATUS" from "AN_ATTITUDE_DATA_INV" complexType
- Renamed "ATTITUDE_QUALITY_INDICATOR" as "ATTITUDE_QUALITY" in "A_RAW_ATTITUDE" and changed the possible values 
("NOATTITUDE", "APRIORIATT", "COARSEATT", "UNCONFATT", "VALIDATT")
- added "ATTITUDE_QUALITY_INDICATOR" in "AN_ATTITUDE_DATA_INV" complexType

7.(see R35 in the PIRN)
Added the OPTIONAL node "Other_Ancillary_Data" in "AN_ANCILLARY_DATA_DSL0" complexType

8.(see R36 in the PIRN)
Put FPA_List node as OPTIONAL

9.(see R37 in the PIRN)
Updated as UNBOUNDED the node Image_Refining/Correlation_Quality in "A_GEOMETRIC_REFINING_QUALITY_L1B_L1C" complexType

10.(see R39 in the PIRN)
Updated the schema according to the CGS and PAC ID defined in [EOFFS-PDGS] V1.2

11.(see R40 in the PIRN)
Updated as UNBOUNDED the node VNIR_SWIR_Registration/Correlation_Quality in "A_GEOMETRIC_REFINING_QUALITY_L1B_L1C" complexType


misc.xsd - V12
**************************
1.(see R33 in the PIRN)
Updated A_NSM definition according to the applicable SAD-ICD


center.xsd - V12
**************************
1.(see R39 in the PIRN)
Updated "A_S2_ARCHIVING_CENTRE", "A_S2_ACQUISITION_CENTER" and "A_S2_PROCESSING_CENTRE" definitions 
according to the CGS and PAC ID defined in [EOFFS-PDGS] V1.2


S2-PDGS-TAS-DI-PSD-V12_SAFE
************************************
1. Replaced in the schema the path "~\resources\xsd\int\esa\safe\sentinel\1.1" instead of "\resources\xsd\int\esa\safe\sentinel-1.0"
2. Replaced namespace="http://www.esa.int/safe/sentinel/1.0" invece di namespace="http://www.esa.int/safe/sentinel-1.1"
3. Replaced in the Manifest examples the code "PAC1" with "EPA_"
4. Removed in the Manifest examplex the tag <resource><software>


************************************
CHANGE OF NAMESPACES (see R41 in the PIRN)
In the schemas listed hereafter all instance of:
"http://pdgs.s2.esa.int/" and "http://gs2.esa.int/"
have been replaced with:
"https://psd-12.sentinel2.eo.esa.int/"


1. item.xsd
2. center,xsd
3. data_time.xsd
4. image.xsd
5. orbital.xsd
6. misc.xsd
7. geographical.xsd
8. platform.xsd
9. spatio.xsd
10. tile.xsd
11. representation.xsd
12. dimap.xsd
13. Inventory.xsd
14. S2_PDI_Level-0_Datastrip_Metadata.xsd
15. S2_PDI_Level-0_Granule_Structure.xsd
16. S2_PDI_Level-1A_Granule_Structure.xsd
17. S2_PDI_Level-1B_Granule_Structure.xsd
18. S2_PDI_Level-1C_Tile_Structure.xsd
19. S2_PDI_Level-0_Granule_Metadata.xsd
20. S2_PDI_Level-1A_Granule_Metadata.xsd
21. S2_PDI_Level-1B_Granule_Metadata.xsd
22. S2_PDI_Level-1C_Tile_Metadata.xsd
23. S2_PDI_Level-0_Datastrip_Structure.xsd
24. S2_PDI_Level-1A_Datastrip_Structure.xsd
25. S2_PDI_Level-1B_Datastrip_Structure.xsd
26. S2_PDI_Level-1C_Datastrip_Structure.xsd
27. S2_PDI_Level-0_ Datastrip_Metadata.xsd
28. S2_PDI_Level-1A_Datastrip_Metadata.xsd
29. S2_PDI_Level-1B_Datastrip_Metadata.xsd
30. S2_PDI_Level-1C_Datastrip_Metadata.xsd
31. S2_User_Product_Level-0_Structure.xsd
32. S2_User_Product_Level-1A_Structure.xsd
33. S2_User_Product_Level-1B_Structure.xsd
34. S2_User_Product_Level-1C_Structure.xsd
35. S2_User_Product_Level-0_Metadata.xsd
36. S2_User_Product_Level-1A Metadata.xsd
37. S2_User_Product_Level-1B Metadata.xsd
38. S2_User_Product_Level-1C Metadata.xsd












































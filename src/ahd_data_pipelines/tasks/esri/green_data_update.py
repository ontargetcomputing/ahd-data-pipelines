from ahd_data_pipelines.tasks.legacy.pipeline import Pipeline
import ahd_data_pipelines.common.ArcGIS.OverwriteFS.OverwriteFS as OverwriteFS
from requests.auth import HTTPBasicAuth
import pandas as pd
import geopandas as gpd
import numpy as np

import time
import fiona.crs
import os
import json
import requests
import logging
import base64
import yaml
import zipfile
import math
from enum import Enum
from pytz import timezone

from shapely.geometry import Polygon as Shapely_Polygon
from shapely.geometry import MultiPolygon

from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry import Point, Geometry

class GreenDataUpdate(Pipeline):
    def __init__(self, dbutils=None, conf=None, stage="DEV", development=False, log4j_logger=None):
        super(GreenDataUpdate, self).__init__(dbutils=dbutils, stage=stage, log4j_logger=log4j_logger)
        
        self.conf = {
            "environment": "dev",
            "view_item_id": "8c0b584653c34e398d81477e76d6c431",
            "target_update_layer_A": "63e51b09032c47cdad9bbec840af6bd3",
            "target_update_view": "2a16eda792b04d3d9bffbd3193232be8",
            "target_update_layer_B": "bf5bae6b4bac4d25ac2181b40433ed54",
            "master_upload_item_id_a": "9553477ac4bf45b2abc36ac1779f5fd2",
            "master_upload_item_id_b": "b3789a9c9e3b4f2ca4d2876a39da8ad1",
            "master_upload_item_schema": "dashboardData.json",
            "schema_path": "/arcgis/home/schema",
            "work_path": "/arcgis/home/work",
            "mncd_item_id": "d45f80eb53c748e7aa3d938a46b48836",
            "out_sr": 102100,
            "data_layers": {
                "PSPS_Events": {
                "url": "https://services.arcgis.com/BLN4oKB0N1YSgvY8/arcgis/rest/services/Statewide_PSPS_Dataset_(Partner_Share)/FeatureServer/0",
                "where": "1=1",
                "out_fields": "GlobalID,County,Status,OutageID",
                "return_geometry": True,
                "layerName": "PSPS_Events"
                },
                "Fire": {
                "itemId": "62878a1d7da34638a16a8a908ebcfdaf",
                "url": "https://services7.arcgis.com/vA61DZby76ncaItU/arcgis/rest/services/DevPipeline_california_wildfire_perim_ReadOnly/FeatureServer/0",
                "where": "irwinid is not null",
                "out_fields": "irwinid,incident_name",
                "return_geometry": True,
                "layerName": "Fire"
                },
                "Earthquake": {
                "url": "https://services7.arcgis.com/vA61DZby76ncaItU/arcgis/rest/services/DevPipeline_california_earthquake_poly_ReadOnly/FeatureServer/0",
                "where": "grid_code>=5 and eventTime BETWEEN CURRENT_TIMESTAMP - 7 AND CURRENT_TIMESTAMP",
                "out_fields": "id, OBJECTID, grid_code, grid_value",
                "return_geometry": True,
                "layerName": "Earthquake"
                },
                "Evacuation": {
                "url": "https://services3.arcgis.com/uknczv4rpevve42E/arcgis/rest/services/CA_EVACUATIONS_PROD/FeatureServer/0",
                "where": "STATUS='Clear to Repopulate' OR STATUS='Evacuation Order' OR STATUS='Order' OR STATUS='Evacuation Warning' OR STATUS = 'Warning' OR STATUS = 'Shelter in Place'",
                "out_fields": "ZONE_NAME,STATUS,OBJECTID,COUNTY,ZONE_ID",
                "return_geometry": True,
                "layerName": "Evacuation"
                },
                "Population": {
                "url": "https://services7.arcgis.com/vA61DZby76ncaItU/arcgis/rest/services/DevPipeline_CHHS_Integrated_Populations_Dataset_ReadOnly/FeatureServer/0",
                "where": "1=1",
                "out_fields": "*",
                "return_geometry": False,
                "layerName": "Population"
                },
                "Facilities": {
                "url": "https://services7.arcgis.com/vA61DZby76ncaItU/arcgis/rest/services/DevPipeline_CHHS_Integrated_Facilities_Dataset_ReadOnly/FeatureServer/0",
                "where": "1=1",
                "out_fields": "*",
                "return_geometry": True,
                "layerName": "Facilities"
                },
                "Zipcodes": {
                "url": "https://services7.arcgis.com/vA61DZby76ncaItU/arcgis/rest/services/a68d06/FeatureServer/0",
                "where": "STATE='CA'",
                "out_fields": "STATE,ZIP_CODE,PO_NAME",
                "layerName": "Zipcodes",
                "return_geometry": True
                },
                "CA_Counties": {
                "url": "https://services7.arcgis.com/vA61DZby76ncaItU/arcgis/rest/services/CA_Counties_With_OES_Region/FeatureServer/0",
                "where": "1=1",
                "out_fields": "*",
                "return_geometry": True,
                "layerName": "CA_Counties"
                },
                "California": {
                "url": "https://services7.arcgis.com/vA61DZby76ncaItU/ArcGIS/rest/services/Master_CHHS_Dashboard_Upload_Copy/FeatureServer/9",
                "where": "1=1",
                "out_fields": "*",
                "return_geometry": True,
                "layerName": "California"
                },
                "Facility_Impact": {
                "id": "3f779e917b4241bba3db07f3b4b30ac0",
                "index": 14,
                "url": "https://services7.arcgis.com/vA61DZby76ncaItU/arcgis/rest/services/Mock_Events/FeatureServer/14",
                "where": "1=1",
                "out_fields": "datahub_facility_id,op_status,evac_status",
                "return_geometry": False,
                "layerName": "Facility_Impact"
                }
            },
            "result_layers": {
                "Population": {
                "index": 0,
                "layerName": "Population",
                "schemaFile": "dashboardData-0.json"
                },
                "Facilities": {
                "index": 2,
                "layerName": "Facilities",
                "schemaFile": "dashboardData-2.json"
                },
                "CA_CountiesAllEventsJoin": {
                "index": 4,
                "layerName": "CA_CountiesAllEventsJoin",
                "schemaFile": "dashboardData-4.json"
                },
                "AllEventsCA_CountiesJoinPoints": {
                "index": 5,
                "layerName": "AllEventsCA_CountiesJoinPoints",
                "schemaFile": "dashboardData-5.json"
                },
                "AllEventsWithPopulation": {
                "index": 1,
                "layerName": "AllEventsWithPopulation",
                "schemaFile": "dashboardData-1.json"
                },
                "FacilitiesAllEventsJoin": {
                "index": 3,
                "layerName": "FacilitiesAllEventsJoin",
                "schemaFile": "dashboardData-3.json"
                },
                "PSPS_Points": {
                "index": 6,
                "layerName": "PSPS_Points",
                "schemaFile": "dashboardData-6.json"
                },
                "Population_Score": {
                "index": 7,
                "layerName": "Population_Score",
                "schemaFile": "dashboardData-7.json"
                }
            },
            "layers_to_process": [
                "Population",
                "AllEventsWithPopulation",
                "Facilities",
                "FacilitiesAllEventsJoin",
                "CA_CountiesAllEventsJoin",
                "AllEventsCA_CountiesJoinPoints",
                "PSPS_Points",
                "Population_Score"
            ],
            "jira_config": {
                "api_url": "https://chhs-datahub-psps.atlassian.net/rest/api/2/issue",
                "comment_url": [
                "https://chhs-datahub-psps.atlassian.net/rest/api/3/issue/",
                "/comment"
                ],
                "jira_tag_list": [
                {
                    "type": "mention",
                    "attrs": {
                    "id": "6047a650116c460070e6bee6",
                    "text": "@Brandon Lammey",
                    "userType": "APP"
                    }
                },
                {
                    "type": "mention",
                    "attrs": {
                    "id": "5ff88cd11051d10075acae05",
                    "text": "@Jose Larios",
                    "userType": "APP"
                    }
                }
                ],
                "jira_ticket_details": {
                "assignee": "6047a650116c460070e6bee6",
                "project_key": "DEV",
                "issue_name": "Bug"
                }
            }
        }

        self.credentials = {
            "jira": {
                "user": "j.schneider@esri.com",
                "password": "ATATT3xFfGF0xE2q38290QmLIRw7BRxmjtqUa1Qg9dz15gV5p8oU1ro0vVuFFMfBM0vjwMOCuu0q_pVv3_NyxhSzf1GA3ERhQaE3ke0CV-G4g9o1TcZf383erIWHjEIATJSoNYH_o2PJh8uxlrRsWj-9kfx39G_mO7iR0ZicOHY4jghGbtespmQ=4EF7C52D"
            },
            "agol_dev": {
                "user": "AHD_DevelopmentDataOwner",
                "password": "meg9qyz-qrx-KPN5caw"
            }
        }

    def run(self):
        ## SETTINGS

        '''
        Configurations
        '''
        agol_user = self.credentials["agol_dev"]
        jira_user = self.credentials["jira"]

        environment = self.conf["environment"]

        master_upload_item_schema = self.conf["master_upload_item_schema"]
        schema_path = self.conf["schema_path"]
        workPath = self.conf["work_path"]
        mncd_item_id = self.conf["mncd_item_id"]

        out_sr = self.conf["out_sr"]
        data_layers = self.conf["data_layers"]
        result_layers = self.conf["result_layers"]
        view_item_id = self.conf["view_item_id"]
        target_update_layer_A = self.conf["target_update_layer_A"]
        target_update_view = self.conf["target_update_view"]
        target_update_layer_B = self.conf["target_update_layer_B"]
        master_upload_item_id_a = self.conf["master_upload_item_id_a"]
        master_upload_item_id_b = self.conf["master_upload_item_id_b"]

        layers_to_process = self.conf["layers_to_process"]

        jira_config = self.conf["jira_config"]


        '''
        ArcGIS 
        '''
        gis = GIS(username=agol_user["user"], password=agol_user["password"])


        '''
        Jira 
        '''

        log = logging.getLogger()
        Log_Errors = []
        Log_Warnings = []

        assignee = jira_config["jira_ticket_details"]["assignee"]
        project_key = jira_config["jira_ticket_details"]["project_key"]
        issue_name = jira_config["jira_ticket_details"]["issue_name"]

        tag_list = jira_config["jira_tag_list"]

        jira_username = jira_user["user"]
        jira_password = jira_user["password"]


        def get_auth_code(user, token):
            """
            To get the proper token, visit 
            https://id.atlassian.com/manage-profile/security/api-tokens
            and press Create API token. 
            Authorization code takes {user}:{token} in base64 form.
            """
            auth_string = f"{user}:{token}"
            auth_string_bytes = auth_string.encode()
            base64_bytes = base64.b64encode(auth_string_bytes)
            return base64_bytes.decode()

        def create_jira_ticket(jira_assignee=assignee, subject="AHD Task Error: Error in DashboardData Update Notebook.", message="Error in automated task.", jira_project_key = "DEV", jira_issue_name = "Bug"):

            authString = get_auth_code(jira_username, jira_password)
            jira_api_url = jira_config["api_url"]  
            
            headers = {'Content-Type': 'application/json', 'Authorization': f'Basic {authString}'}
            notify_data = {
                "fields": {
                    "assignee":
                    {
                        "accountId": jira_assignee
                    },
                    "project":
                    {
                    "key": jira_project_key
                    },
                    "summary": subject,
                    "description": message,
                    "issuetype": {
                        "name": jira_issue_name,
                    }
                }
            }
            
            
            response = requests.post(
                jira_api_url,
                headers=headers, 
                data=json.dumps(notify_data)
            )

            if response.reason == "Created":  
                response_json = json.loads(response.text)
                issue_id = response_json["id"]
                
                auth = HTTPBasicAuth(jira_username, jira_password)
                comment_url = "{0}{1}{2}".format(jira_config["comment_url"][0], issue_id, jira_config["comment_url"][1])
                comment_headers = {
                "Accept": "application/json",
                "Content-Type": "application/json"
                }
                
                comment_data = {
                    "body": {
                        "type": "doc",
                        "version": 1,
                        "content": [
                            {
                                "type": "paragraph",
                                "content": tag_list
                            }
                        ]
                    }
                }  
                    
                comment_response = requests.post(
                    comment_url,
                    headers=comment_headers, 
                    data=json.dumps(comment_data),
                    auth=auth
                )


        '''
        Layer and variable names
        '''
        layer_population = data_layers["Population"]["layerName"]
        layer_facilities = data_layers["Facilities"]["layerName"]
        layer_zipcodes = data_layers["Zipcodes"]["layerName"]
        layer_counties = data_layers["CA_Counties"]["layerName"]
        layer_california = data_layers["California"]["layerName"]
        layer_fire = data_layers["Fire"]["layerName"]
        layer_earthquake = data_layers["Earthquake"]["layerName"]
        layer_evacutation = data_layers["Evacuation"]["layerName"]
        layer_psps = data_layers["PSPS_Events"]["layerName"]
        layer_facility_impacted = data_layers["Facility_Impact"]["layerName"]

        layer_events_pop_join = "AllEventsWithPopulation"
        layer_facilities_events_join = "FacilitiesAllEventsJoin"
        layer_counties_events_join = "CA_CountiesAllEventsJoin"
        layer_events_counties_join_points = "AllEventsCA_CountiesJoinPoints"
        layer_psps_points = "PSPS_Points"
        layer_evacuation_points = "Evacuation_Points"
        layer_population_score = "Population_Score"

        population_types_list = ["L0_over55", "L0_over65", "L0_over75", "L0_over85", "L0_cdss_calfresh_recpts", "L0_cdss_ihss_recpts", 
                                "L0_cdph_wic_partipts", "L0_dhcs_medi_cal_benes", "L0_dhcs_medi_cal_dme", "L0_cdds_clients"] # TODO: handle 0 values in data "L0_cdph_medicare_benes", "L0_cdph_medicare_dme"

        pop_type_fieldname = "pop_type"
        pop_sub_fieldname = "sub_pop"
        pop_tot_fieldname = "tot_zip_pop"
        pop_zscore_fieldname = "zscore"
        pop_shape_fieldname = "SHAPE"
        pop_zipcode_fieldname = "L0_zip_code"
        pop_region_fieldname = "L0_oes_region"
        pop_total_fieldname = "L0_tot_zip_pop"

        facility_id_fieldname = "ade_facility_id"
        datahub_facility_id_fieldname = "datahub_facility_id"


        '''
        Value maps
        '''

        # The format for the events polygons intermediate data set consists of the fields:  event_id, event_name, event_type, and polygon SHAPE
        class Events_Field_Map(Enum):
            psps = {
                "Status": "event_type", 
                "GlobalID":"event_id", 
                "County":"event_name"
            }
            fire = {
                "event_type":"event_type", 
                "irwinid":"event_id", 
                "incident_name":"event_name" 
            }
            earthquake = {
                "event_type":"event_type", 
                "id":"event_id", 
                "id_left":"event_id", 
                "TARGET_FID": "FID"
            }
            evacuation = {
                "STATUS":"event_type", 
                "OBJECTID":"event_id", 
                "ZONE_NAME":"event_name",
                "ZONE_ID":"zone_id",
                "COUNTY":"county"
            }

        # For field values that need to be mapped to values used by Dashboard Data
        class Events_Value_Map(Enum):
            psps = {
                "Monitoring": "PSPS_Potential_Impacted", 
                "Downstream":"PSPS_Potential_Impacted", 
                "De-Energized":"PSPS_Active_Impacted",
                "Re-Energized":"PSPS_Reenergized"
            }
            evacuation = {
                "Clear to Repopulate": "Clear_to_Repopulate", 
                "Evacuation Order":"Evacuation_Order", 
                "EVACUATION ORDER":"Evacuation_Order", 
                "Order":"Evacuation_Order",
                "Evacuation Warning":"Evacuation_Warning",
                "Warning":"Evacuation_Warning",
                "Shelter in Place": "Shelter_in_Place",
                "Shelter In Place": "Shelter_in_Place"
            }   

        class Events_Type(Enum):
            fire = layer_fire
            psps = layer_psps
            earthquake = layer_earthquake
            evacuation = layer_evacutation


        ## HELPER FUNCTIONS

        no_errors = lambda error_list : len(error_list) == 0
        no_events = lambda events_df : len(events_df) == 0

        '''
        Request features and return custom data object
        '''
        def get_features(data):
            f_layer = FeatureLayer(data["url"])
            f_set = f_layer.query(where= data["where"], out_fields= data["out_fields"], return_geometry= data["return_geometry"], out_sr=out_sr)
            f_layer_df = f_set.sdf
            record_count = len(f_set.features)
            object_id_field_name = f_set.object_id_field_name
            
            result = dict(f_layer=f_layer, f_set=f_set, f_layer_df=f_layer_df, record_count=record_count, object_id_field_name=object_id_field_name)
            return(result)


        '''Remove the given list of rows from the given dataframe and return the resulting dataframe'''
        def remove_rows(df, row_list):
            result_df = df.drop(row_list, inplace=False)
            return result_df


        '''Remove the given list of columns from the given dataframe and return the resulting dataframe'''
        def remove_columns(df, column_list):
            df_ref = df
            errors = []
            for col in column_list:
                try:
                    df_ref = df_ref.drop(col, axis=1)
                except Exception as e: 
                    errors.append(e)
            return df_ref


        '''
        Remove empty geometries from data frame and add to warnings 
        '''
        def remove_invalid_geometries(data):
            '''Check for a geometry field on the given dataframe and then check for and remove any rows that contain an empty or "nan" geometry field and return the resulting dataframe'''
            if data["return_geometry"] and len(data["df"]) > 0: 
                print("Check for invalid geometries to remove.")
                
                index = 0
                remove = [] 
                geo_type = None
                try: 
                    geo_type = data["df"].spatial.geometry_type[0].lower()
                except Exception as e:
                    print("Geometry type not found.")
                    return
            
                try:
                    for geometry in data["df"]["SHAPE"]:
                        try:
                            if geometry == "NaN" or geometry == "nan" or type(geometry) is float or geometry.type.lower() != geo_type:
                                remove.append(index)
                            elif geo_type == "polygon" :
                                if type(geometry) is float:
                                    remove.append(index)    
                        except Exception as e:
                            remove.append(index)
                        index +=1
                    
                    if len(remove) > 0:
                        data["df"] = remove_rows(data["df"], remove)
                        data["df"] = data["df"][data["df"]['SHAPE'].notnull()]
                        print("Geometry type: {0}".format(geo_type))
                        print("{0} records found with invalid geometries and removed".format(len(remove)))
                        Log_Warnings.append({"message": "{0} records removed from dashboard data calculations due to missing geometry data in {1}.".format(len(remove), data["layerName"])})
                    else: 
                        print("No invalid geometries found.")
                        
                        
                except Exception as e:
                    print("Error in remove_invalid_geometries. View message below.")
                    print(e)

                return


        def calculate_psps_event_type(event):
            '''Map event values to the PSPS Events_Value_Map and return the resulting series'''
            calculated = []
            for index, value in event.items():
                calculated.append(Events_Value_Map.psps.value[value])
            return pd.Series(calculated)

        def calculate_evacuation_event_type(event):    
            '''Map event values to the Evacuation Events_Value_Map and return the resulting series'''
            calculated = []
            for index, value in event.items():
                calculated.append(Events_Value_Map.evacuation.value[value])
            return pd.Series(calculated)


        '''
        The given event dataframes are used to create a single events dataframe 
        The format for the events polygons intermediate data set consists of the fields:  event_id, event_name, event_type, and polygon SHAPE
        Event data is converted to the appropriate format and then the final event data frame is returned
        '''
        def calculate_events_data(psps_df,fire_df,earthquake_df,evacuation_df, oid):\

            '''PSPS'''
            if psps_df.size > 0: 
                psps_df = psps_df.rename(columns = Events_Field_Map.psps.value, inplace = False)
                # psps_df["event_type"] = calculate_psps_event_type(event=psps_df["event_type"])
                psps_df["event_name"] = psps_df["event_name"] + " - " +  psps_df["OutageID"]
                del psps_df["OutageID"]
                print("PSPS events found.")
            else: 
                psps_df = psps_df.rename(columns = Events_Field_Map.psps.value, inplace = False)
                print('No PSPS events found.')
            psps_df = psps_df.filter(['event_id', 'event_name', 'event_type', 'SHAPE']) 
            
            '''Fire'''  
            if fire_df.size > 0: 
                fire_df['event_type'] = 'Fire'
                fire_df = fire_df.rename(columns = Events_Field_Map.fire.value, inplace = False)
                print('Fire events found.')
            else:
                fire_df['event_type'] = 'Fire'
                fire_df = fire_df.rename(columns = Events_Field_Map.fire.value, inplace = False) 
                print('No Fire events found.') 
            fire_df = fire_df.filter(['event_id', 'event_name', 'event_type', 'SHAPE']) 
            
                
            '''
            Earthquake
            '''

            if earthquake_df.size > 0: 
                earthquake_df['event_type'] = 'Earthquake'
                earthquake_df = earthquake_df.rename(columns = Events_Field_Map.earthquake.value, inplace = False)
                earthquake_df['event_name'] = earthquake_df['event_id']
                print('Earthquake events found.')
            else:
                earthquake_df['event_type'] = 'Earthquake'
                earthquake_df = earthquake_df.rename(columns = Events_Field_Map.earthquake.value, inplace = False)
                print('No Earthquake events found.')
            earthquake_df = earthquake_df.filter(['event_id', 'event_name', 'event_type', 'SHAPE']) 
            
            
            '''Evacuation'''
            if evacuation_df.size > 0: 
                evacuation_df = evacuation_df.rename(columns = Events_Field_Map.evacuation.value, inplace = False)
                # evacuation_df['event_type'] = calculate_evacuation_event_type(event=evacuation_df['event_type'])
                evacuation_df['event_name'] = [evacuation_df.iloc[row[0]]['event_name']
                                            if evacuation_df.iloc[row[0]]['event_name'] != 'None' and evacuation_df.iloc[row[0]]['event_name'] != '' and evacuation_df.iloc[row[0]]['event_name'] != None
                                            else evacuation_df.iloc[row[0]]['zone_id']
                                            if evacuation_df.iloc[row[0]]['zone_id'] != 'None' and evacuation_df.iloc[row[0]]['zone_id'] != '' and evacuation_df.iloc[row[0]]['zone_id'] != None
                                            else evacuation_df.iloc[row[0]]['county']
                                            for row in evacuation_df.itertuples()]
                evacuation_df['event_id'] = evacuation_df['event_id'].map(str) + "-" +  evacuation_df['event_type'].map(str)
                print('Evacuation events found.')
            else: 
                evacuation_df = evacuation_df.rename(columns = Events_Field_Map.evacuation.value, inplace = False)
                print('No Evacuation events found.')
            evacuation_df = evacuation_df.filter(['event_id', 'event_name', 'event_type', 'SHAPE']) 
            
            non_empty = [df for df in [psps_df, fire_df, earthquake_df, evacuation_df] if len(df) > 0]
            
            if len(non_empty) == 0:
                all_events_polygons_df = psps_df
            else:
                all_events_polygons_df = pd.concat(non_empty)
                all_events_polygons_df.index = range(0,len(all_events_polygons_df))
                all_events_polygons_df[oid] = all_events_polygons_df.index
            
            return all_events_polygons_df


        '''
        Calculates psps impacted fields (PSPS_Active_Impacted, PSPS_Potential_Impacted, PSPS_Reenergized)
        Impacted fields are added to the given source dataframe and then the result dataframe is returned 
        '''
        def calculate_psps_impacted(source_df, psps_df, group_by_key):

            results = source_df.copy()
            
            if len(psps_df) < 1:
                results['PSPS_Active_Impacted'] = ''
                results['PSPS_Potential_Impacted'] = ''
                results['PSPS_Reenergized'] = ''
                return results
            
            print('Calculating PSPS fields')
            join_df = local_join_df(results, psps_df, 'left')
            
            '''
            if a result intersects with psps_df subsection status is 'De-Energized' =>  results['PSPS_Active_Impacted'] = 'YES'
            if a result intersects with psps_df subsection status is 'Monitoring' or 'Downstream' =>  results['PSPS_Potential_Impacted'] = 'YES'  
            if a result intersects with psps_df subsection status is 'Re-Energized'  =>  results['PSPS_Reenergized'] = 'YES'  
            '''
            
            results['PSPS_Active_Impacted'] = results[group_by_key].apply(lambda key: 'YES' if ('De-Energized' in list(join_df[join_df[group_by_key] == key]['Status'])) else '')

            results['PSPS_Potential_Impacted'] = results[group_by_key].apply(lambda key: 'YES' if (('Monitoring' in list(join_df[join_df[group_by_key] == key]['Status'])) | ('Downstream' in list(join_df[join_df[group_by_key] == key]['Status']))) else '')

            results['PSPS_Reenergized'] = results[group_by_key].apply(lambda key: 'YES' if ('Re-Energized' in list(join_df[join_df[group_by_key] == key]['Status']))  else '')
            
            return results


        '''
        Calculates fire impacted fields (Fire_Buffer_Distance) with a distance of 0 (impacted), 1 mile, and 5 miles away
        Impacted fields are added to the given source dataframe and then the result dataframe is returned 
        '''
        def calculate_fire_impacted(source_df, fire_df, oid):

            results = source_df.copy()

            if len(fire_df) < 1:
                results["Fire_Buffer_Distance"] = pd.Series(dtype='float')
                return results
            
            print('Calculating fire fields')
            
            try: 

                distance_0 = fire_df.copy()

                join_df_0 = local_join_df(distance_0, results, 'inner')

                results["Fire_Buffer_Distance"] = pd.Series(dtype='float')
                
                ''' This will check if the fire is 0, 1, or 5 miles away using the joined df's above '''
                results["Fire_Buffer_Distance"] = results[oid].apply(lambda key: 0 if len(join_df_0[join_df_0[oid] == key]) > 0 else np.NaN)

            except Exception as e:  
                results["Fire_Buffer_Distance"] = None
                print("Error calculating fire buffers.")
                print(e)
                Log_Errors.append({"message": "Error calculating fire buffers. {0}.".format(e)})
            
            return results


        '''
        Calculates earthquake impacted fields (grid_code, grid_value)
        Impacted fields are added to the given source dataframe and then the result dataframe is returned 
        '''
        def calculate_earthquake_impacted(source_df, earthquake_df, group_by_key):

            results = source_df.copy()

            if len(earthquake_df) < 1:
                results["grid_code"] = pd.Series(dtype='int')
                results["grid_value"] = pd.Series(dtype='int')
                return results
            
            print('Calculating earthquake fields')

            join_df = local_join_df(results, earthquake_df, 'left')
            
            join_df.drop_duplicates(subset=[group_by_key])

            results = join_df
            
            return results


        '''
        Calculates evacuation impacted fields (Evacuation_Order, Evacuation_Warning, Shelter_in_Place, Clear_to_Repopulate)
        Impacted fields are added to the given source dataframe and then the result dataframe is returned 

        Evacuation_Order: ['Evacuation Order', 'Order']
        Evacuation_Warning: ['Evacuation Warning',  'Warning']
        Shelter_in_Place: ['Shelter In Place']
        Clear_to_Repopulate: ['Clear to Repopulate']
        '''
        def calculate_evacuation_impacted(source_df, evacuation_df, group_by_key):
            results = source_df.copy()

            if len(evacuation_df) < 1:
                results['Evacuation_Order'] = ''
                results['Evacuation_Warning'] = ''
                results['Shelter_in_Place'] = ''
                results['Clear_to_Repopulate'] = ''
                return results
            
            print('Calculating evacuation fields')
            
            join_df = local_join_df(left_df=source_df, right_df=evacuation_df, join_type="left")

            results['Evacuation_Order'] = results[group_by_key].apply(lambda key: 'YES' if ('Evacuation Order' in list(join_df[join_df[group_by_key] == key]['STATUS'])) | ('Order' in list(join_df[join_df[group_by_key] == key]['STATUS'])) else '')
            results['Evacuation_Warning'] = results[group_by_key].apply(lambda key: 'YES' if ('Evacuation Warning' in list(join_df[join_df[group_by_key] == key]['STATUS'])) | ('Warning' in list(join_df[join_df[group_by_key] == key]['STATUS'])) else '')
            results['Shelter_in_Place'] = results[group_by_key].apply(lambda key: 'YES' if ('Shelter In Place' in list(join_df[join_df[group_by_key] == key]['STATUS'])) else '')
            results['Clear_to_Repopulate'] = results[group_by_key].apply(lambda key: 'YES' if ('Clear to Repopulate' in list(join_df[join_df[group_by_key] == key]['STATUS'])) else '')
            
            
            return results


        def local_join_df(left_df, right_df, join_type):
            '''
            Join the given dataframes using the intersect and the given join type
            Use the ArcGIS API spatial join which uses Shapely internally 
            Remove columns created by the join process and return the resulting dataframe 
            '''
            print('Running local join')
            
            left_df = remove_columns(left_df, ['index_left','index_right'])
            right_df = remove_columns(right_df, ['index_left','index_right'])
            
            join_df = left_df.spatial.join(
                right_df=right_df, 
                how=join_type,
                op='intersects'
            )
            
            join_df = remove_columns(join_df, ['index_left','index_right'])  

            return join_df

        def spatial_join_features(target_sdf, join_features_sdf, how, op, drop_dups=False, record_id=None):
            
            # If one of the dataframe inputs is empty then a dataframe is returned dependent on the how parameter (to prevent error in spatial.join) 
            if len(target_sdf.index) == 0 or len(join_features_sdf.index) == 0:
                if how == 'left' :
                    return target_sdf
                if how == 'right' :
                    return join_features_sdf
                df = pd.DataFrame()
                return df
            
            join_sdf = target_sdf.spatial.join(
                right_df=join_features_sdf,
                how=how,
                op=op,
                left_tag = '',
                right_tag='right'
            )
            
            # If multiple join features are found that have the same spatial relationship with a single target feature the duplicates are removed (determined by key)    
            if drop_dups == True and record_id != None:
                join_sdf = join_sdf.drop_duplicates(subset=[record_id], keep='first')
            
            return join_sdf


        ## INGEST

        tic = time.perf_counter()


        for key, data in data_layers.items():
            print("Collecting Data: {0}".format(data["layerName"]))
            try: 
                result=get_features(data)
                data_layers[key]["f_set"] = result["f_set"]
                data_layers[key]["df"] = result["f_layer_df"]
                data_layers[key]["count"] = result["record_count"]
                data_layers[key]["object_id_field_name"] = result["object_id_field_name"]

                print("Received {0} records".format(data_layers[key]["count"]))
                remove_invalid_geometries(data)

            except Exception as e:
                print('An error occured during data request. Sleeping and retrying. View error message below.')
                print(e)
                time.sleep(2)
                
                try:
                    '''
                    some failures are due to complex polygon geometries and therefore require batching to request features 
                    f_layer = FeatureLayer(data["url"])
                    if f_layer.properties.geometryType == 'esriGeometryPolygon': get_batch_features()
                    '''
                    result=get_features(data)        
                    data_layers[key]["f_set"] = result["f_set"]
                    data_layers[key]["df"] = result["f_layer_df"]
                    data_layers[key]["count"] = result["record_count"]
                    data_layers[key]["object_id_field_name"] = result["object_id_field_name"]

                    print("Received {0} records".format(data_layers[key]["count"]))
                    remove_invalid_geometries(data)
                    
                except Exception as e: 
                    print('Data collection failed. View error message below.')
                    print(e)
                    Log_Errors.append({'message': 'Error collecting data for layer {0}. {1}. 504 error recovery method failed.'.format(data["layerName"], e)})


        toc = time.perf_counter()
        print(f"Returned data in {toc - tic:0.4f} seconds")


        ## CALCULATIONS

        tic = time.perf_counter()


        '''
        Dashboard data schema
        '''
        dashboard_data_fs = gis.content.get(master_upload_item_id_a)

        result_fields = {}
        for f_layers in dashboard_data_fs.layers: 
            properties = f_layers.properties

            for field in properties.fields:
                if field.type == "esriFieldTypeOID":
                    oid = field.name
                    break
                else:
                    oid = None

            result_fields[properties.name] = {}
            result_fields[properties.name]["object_id_field_name"] = oid
            result_fields[properties.name]["fields"] = properties.fields


        ## EVENTS

        all_events_df = None
        if no_errors(Log_Errors): 

            try: 
                psps_data = data_layers[Events_Type.psps.value]["df"].copy()
                fire_data = data_layers[Events_Type.fire.value]["df"].copy()
                earthquake_data = data_layers[Events_Type.earthquake.value]["df"].copy()
                evacuation_data = data_layers[Events_Type.evacuation.value]["df"].copy()

                '''The name of the oid field is chosen arbitrarily as this is an intermediate product'''
                all_events_oid = data_layers[Events_Type.evacuation.value]["object_id_field_name"]

                all_events_df = calculate_events_data(psps_df=psps_data, fire_df=fire_data, earthquake_df=earthquake_data, evacuation_df=evacuation_data, oid=all_events_oid)
            except Exception as e:
                print("Error: view message below.")
                print(e)
                Log_Errors.append({"message": "Error preparing all events data."})


        ## DASHBOARD DATA

        calculated = {}
        Layer_Errors = {}


        # calculated[layer_population]["data"] = None
        if no_errors(Log_Errors): 
            try: 
                print("Processing {0} data".format(layer_population))

                population_df = data_layers[layer_population]["df"].copy()
                zipcodes_df = data_layers[layer_zipcodes]["df"].copy()
                result_oid = result_fields[layer_population]["object_id_field_name"]

                result_df = pd.merge(
                    zipcodes_df, 
                    population_df, 
                    left_on='zip_code',
                    right_on='zip_code',
                    how='inner' 
                )

                field_map = {
                    'zip_code':'L0_zip_code',
                    'cdss_calfresh_recpts': 'L0_cdss_calfresh_recpts',
                    'cdss_ihss_recpts': 'L0_cdss_ihss_recpts',
                    'cdph_wic_partipts': 'L0_cdph_wic_partipts',
                    'cdph_medicare_benes': 'L0_cdph_medicare_benes',
                    'cdph_medicare_dme': 'L0_cdph_medicare_dme',
                    'dhcs_medi_cal_benes': 'L0_dhcs_medi_cal_benes',
                    'dhcs_medi_cal_dme': 'L0_dhcs_medi_cal_dme',
                    'cdds_clients': 'L0_cdds_clients',
                    'over55': 'L0_over55',
                    'over65': 'L0_over65',
                    'over75': 'L0_over75',
                    'over85': 'L0_over85',
                    'tot_zip_pop': 'L0_tot_zip_pop',
                    'oes_region': 'L0_oes_region',
                    'ObjectId': 'L0_ObjectId',
                    'county': 'L0_county'
                }

                remove = ['ZIP_CODE', 'PO_NAME_x', 'STATE_x', 'POPULATION_x',
                'SQMI_x', 'Shape__Area_x', 'Shape__Length_x', 'ZIP_CODE_1',
                'PO_NAME_y', 'STATE_y', 'POPULATION_y', 'SQMI_y', 'Shape__Area_1',
                'Shape__Length_1', 'ZIP_CODE_12', 'PO_NAME_1', 'STATE_1',
                'POPULATION_1', 'SQMI_1', 'Shape__Area_12', 'Shape__Length_12']

                result_df = result_df.rename(columns=field_map)
                result_df = remove_columns(result_df, remove)

                result_df.index = range(0,len(result_df))
                result_df[result_oid] = result_df.index
                result_df["ROW"] = result_df.index
                calculated[layer_population] = { "data": result_df }

                print("Adding impacted fields to layer: {0}".format(layer_population))

                result_df = calculated[layer_population]["data"]
                result_df = calculate_fire_impacted(source_df=result_df, fire_df=data_layers[Events_Type.fire.value]["df"], oid=result_oid)
                result_df = calculate_psps_impacted(source_df=result_df, psps_df=data_layers[Events_Type.psps.value]["df"], group_by_key="ROW")
                result_df = calculate_earthquake_impacted(source_df=result_df, earthquake_df=data_layers[Events_Type.earthquake.value]["df"], group_by_key="ROW")
                result_df = calculate_evacuation_impacted(source_df=result_df, evacuation_df=data_layers[Events_Type.evacuation.value]["df"], group_by_key="ROW")

                print("Cleanup data")
                field_map={
                    'Shape__Area_y': 'Shape__Area',
                    'Shape__Length_y': 'Shape__Length',
                }
                remove = ['OBJECTID_right', 'OBJECTID_left', 'id']
                result_df = result_df.rename(columns=field_map)
                result_df = remove_columns(result_df, remove)
                result_df[result_oid] = result_df.index

                calculated[layer_population]["data"] = result_df

                print("ADE population data count: {0}".format(data_layers[layer_population]["count"]))
                print('Population zipcode join data count: {0}'.format(len(calculated[layer_population]["data"].index)))

            except Exception as e:
                print("Error: view message below.")
                print(e)
                Log_Errors.append({"message": "Error processing {0} data.".format(layer_population)})
                Layer_Errors[layer_population] = {"message": "Error processing {0} data.".format(layer_population)}


        # TODO: Parameterize all fields 
        def impactedFacilities (facility):
                impacted = facility["PSPS_Active_Impacted"] == "YES" or facility["Evacuation_Order"] == "YES" or facility["Fire_Buffer_Distance"] == 0 or (math.isnan(facility["grid_code"])== False and facility["grid_code"] >= 5)
                
                return impacted

        def calculateFacilityType (facility):    

            if facility["ade_source"] == 'DSH':
                return "DSH-All";
            elif facility["ade_source"] == 'CDDS': 
                return "DDS-All";
            elif facility["ade_source"] == 'DHCS': 
                return "DHCS-All";
            elif facility["ade_source"] == 'CDPH':
                if facility["facility_type"] == 'General Acute Care Hospital':
                    return "CDPH-GACH";
                elif facility["facility_type"] == 'Skilled Nursing Facility':
                    return "CDPH-SNF";
                else:
                    return "CDPH-Other";
            elif facility["ade_source"] == 'CDSS':
                if facility["program_type"] == 'Adult And Senior':  
                    return "CDSS-ASC"
                elif facility["program_type"] == 'Childrens Reside':
                    return "CDSS-Child Res"
                elif facility["program_type"] == 'Child Care': 
                    return "CDSS-Child Care";
                elif facility["program_type"] != 'Child Care':
                    return "CDSS-Other";
                else:
                    return ''
            else:
                return ''

        def calculateFacilitiesFields (facilities_df):
            facilities_df['ade_source_facility_type'] = facilities_df.apply(lambda x: calculateFacilityType(x),axis=1)
            facilities_df['facility_impacted'] = facilities_df.apply(lambda x: 1 if impactedFacilities(x) == True else 0,axis=1)
            facilities_df['licensed_capacity_impacted'] = facilities_df.apply(lambda x: x["licensed_capacity"] if impactedFacilities(x) == True else 0,axis=1)
            
            return facilities_df

        ''' Join facilties layer with facilities impacted table'''
        def calculateImpactedFacilities (facilities_df, impacted_df):
            
            try: 
                impacted_facilities_sdf = pd.merge(
                    facilities_df,
                    impacted_df,
                    how="left",
                    on=None,
                    left_on=facility_id_fieldname,
                    right_on=datahub_facility_id_fieldname,
                    left_index=False,
                    right_index=False,
                    sort=False,
                    suffixes=("", "_right"),
                    copy=True,
                    indicator=False,
                    validate="one_to_many",
                )
                
                impacted_facilities_sdf['operational_int'] = impacted_facilities_sdf.apply(lambda x: 1 if x['op_status'] == "Operational" else 0,axis=1)
                impacted_facilities_sdf['nonoperational_short_term_int'] = impacted_facilities_sdf.apply(lambda x: 1 if x['op_status'] == "Non-Operational Short Term" else 0,axis=1)
                impacted_facilities_sdf['nonoperational_long_term_int'] = impacted_facilities_sdf.apply(lambda x: 1 if x['op_status'] == "Non-Operational Long Term" else 0,axis=1)
                impacted_facilities_sdf['operational_with_impacts_int'] = impacted_facilities_sdf.apply(lambda x: 1 if x['op_status'] == "Operational With Impacts" else 0,axis=1)

                impacted_facilities_sdf['fully_evacuated_int'] = impacted_facilities_sdf.apply(lambda x: 1 if x['evac_status'] == "Fully Evacuated" else 0,axis=1)
                impacted_facilities_sdf['partially_evacuated_int'] = impacted_facilities_sdf.apply(lambda x: 1 if x['evac_status'] == "Partially Evacuated" else 0,axis=1)
                impacted_facilities_sdf['repopulating_int'] = impacted_facilities_sdf.apply(lambda x: 1 if x['evac_status'] == "Repopulating" else 0,axis=1)
                
                impacted_facilities_sdf['fire_int'] = impacted_facilities_sdf.apply(lambda x: 1 if x['Fire_Buffer_Distance'] >= 0 else 0,axis=1)
                impacted_facilities_sdf['earthquake_int'] = impacted_facilities_sdf.apply(lambda x: 1 if x['grid_code'] >=5 else 0,axis=1)
                impacted_facilities_sdf['active_psps_int'] = impacted_facilities_sdf.apply(lambda x: 1 if x['PSPS_Active_Impacted'] == "YES" else 0,axis=1)
                impacted_facilities_sdf['evac_area_int'] = impacted_facilities_sdf.apply(lambda x: 1 if x['Evacuation_Order'] == "YES" else 0,axis=1)
                
            except Exception as e: 
                print('Failed to join features: {}'.format(e))
                impacted_facilities_sdf = facilities_df
            
            return impacted_facilities_sdf


        def Calculate_AllEventsPopJoin(events_df, population_df):
            
            if events_df.empty == True or population_df.empty == True:
                return events_df
                
            '''
            The given population dataframe is joined to the event dataframe in a 1:Many relationship 
            The results are flattened using a summation of the population data fields
            The resulting dataframe is returned 
            TODO: move field map to enum class and get objectid fields dynamically
            '''

            field_map = {
                'L0_cdss_calfresh_recpts': 'cdss_calfresh_recpts',
                'L0_cdss_ihss_recpts': 'cdss_ihss_recpts',

                'L0_cdds_clients': 'cdds_clients',

                'L0_cdph_wic_partipts': 'cdph_wic_partipts',
                'L0_cdph_medicare_benes': 'cdph_medicare_benes',
                'L0_cdph_medicare_dme': 'cdph_medicare_dme',

                'L0_dhcs_medi_cal_benes': 'dhcs_medi_cal_benes',
                'L0_dhcs_medi_cal_dme': 'dhcs_medi_cal_dme',

                'L0_over55': 'over55',
                'L0_over65': 'over65',
                'L0_over75': 'over75',
                'L0_over85': 'over85',
                'L0_tot_zip_pop': 'tot_zip_pop',
                
                'objectid_join': 'OBJECTID'
            }
                
            
            remove = ['objectid', 'OBJECTID']

            result_df = None
            try: 
                population = population_df.copy()
                events_df = events_df.copy()
                events_df['objectid_join'] = events_df.index

                print("Running Join")
                result_df = local_join_df(events_df, population, "left")
                print("Calculating Sums")
                sum_sdf = result_df.groupby('objectid_join')["L0_over55","L0_over65","L0_over75","L0_over85","L0_tot_zip_pop","L0_cdss_calfresh_recpts","L0_cdss_ihss_recpts","L0_cdph_wic_partipts","L0_cdph_medicare_benes","L0_cdph_medicare_dme","L0_dhcs_medi_cal_benes","L0_dhcs_medi_cal_dme","L0_cdds_clients"].sum()
                print("Flattening Results")
                result_df = pd.merge(events_df, sum_sdf, on=['objectid_join'], how='left')

                result_df= remove_columns(result_df, remove)
                result_df = result_df.rename(columns=field_map) 
                
            except Exception as e:
                print('Error joining data.')
                print(e)
                Log_Errors.append({'message': 'Error joining population data to events data.'})
                Layer_Errors[layer_events_pop_join] = {'message': 'Error joining population data to events data.'}
                
            return result_df


        calculated[layer_facilities] = { "data": None }
        if no_errors(Log_Errors): 
            try:
                print("Processing {0} data".format(layer_facilities))

                result_df = data_layers[layer_facilities]["df"].copy()
                result_oid = result_fields[layer_facilities]["object_id_field_name"]

            #     result_df = remove_columns(result_df, [data_layers[layer_facilities]["object_id_field_name"]])
                result_df.index = range(0,len(result_df))
                result_df[result_oid] = result_df.index
                result_df["ROW"] = result_df.index

                '''Calculate event impacts'''
                result_df = calculate_fire_impacted(source_df=result_df, fire_df=data_layers[Events_Type.fire.value]["df"], oid=result_oid)
                result_df = calculate_psps_impacted(source_df=result_df, psps_df=data_layers[Events_Type.psps.value]["df"], group_by_key="ROW")
                result_df = calculate_earthquake_impacted(source_df=result_df, earthquake_df=data_layers[Events_Type.earthquake.value]["df"], group_by_key="ROW")
                result_df = calculate_evacuation_impacted(source_df=result_df, evacuation_df=data_layers[Events_Type.evacuation.value]["df"], group_by_key="ROW")

                '''Calculate additional impacted fields'''
                result_df = calculateFacilitiesFields(result_df)
                facility_impacted_df = data_layers[layer_facility_impacted]["df"]
                result_df = calculateImpactedFacilities(result_df, facility_impacted_df)

                calculated[layer_facilities]["data"] = result_df

            except Exception as e:
                print("Error: view message below.")
                print(e)
                Log_Errors.append({"message': 'Error processing {0} data.".format(layer_facilities)})
                Layer_Errors[layer_facilities] = {"message": "Error processing {0} data.".format(layer_facilities)}


        field_map = {
            'L0_cdss_calfresh_recpts': 'cdss_calfresh_recpts',
            'L0_cdss_ihss_recpts': 'cdss_ihss_recpts',

            'L0_cdds_clients': 'cdds_clients',

            'L0_cdph_wic_partipts': 'cdph_wic_partipts',
            'L0_cdph_medicare_benes': 'cdph_medicare_benes',
            'L0_cdph_medicare_dme': 'cdph_medicare_dme',

            'L0_dhcs_medi_cal_benes': 'dhcs_medi_cal_benes',
            'L0_dhcs_medi_cal_dme': 'dhcs_medi_cal_dme',

            'L0_over55': 'over55',
            'L0_over65': 'over65',
            'L0_over75': 'over75',
            'L0_over85': 'over85',
            'L0_tot_zip_pop': 'tot_zip_pop',

            'objectid_join': 'OBJECTID'
        }

        remove = ['objectid', 'OBJECTID']

        calculated[layer_events_pop_join] = { "data": None }
        if no_errors(Log_Errors): 
            try: 
                print("Processing {} data".format(layer_events_pop_join))

                events_df = all_events_df.copy()
                population = calculated[layer_population]["data"].copy()

                result_df = Calculate_AllEventsPopJoin(events_df, calculated[layer_population]["data"])

                if events_df.empty == True or population_df.empty == True:
                    result_df = events_df
                else:
                    try:
                        ''' Join and sum data '''
                        events_df['objectid_join'] = events_df.index
                        result_df = local_join_df(events_df, population, "left")
                        sum_sdf = result_df.groupby('objectid_join')["L0_over55","L0_over65","L0_over75","L0_over85","L0_tot_zip_pop","L0_cdss_calfresh_recpts","L0_cdss_ihss_recpts","L0_cdph_wic_partipts","L0_cdph_medicare_benes","L0_cdph_medicare_dme","L0_dhcs_medi_cal_benes","L0_dhcs_medi_cal_dme","L0_cdds_clients"].sum()
                        result_df = pd.merge(events_df, sum_sdf, on=['objectid_join'], how='left')

                        result_df= remove_columns(result_df, remove)
                        result_df = result_df.rename(columns=field_map) 

                    except Exception as e:
                        print('Error joining data.')
                        print(e)
                        Log_Errors.append({'message': 'Error joining population data to events data.'})
                        Layer_Errors[layer_events_pop_join] = {'message': 'Error joining population data to events data.'}

                calculated[layer_events_pop_join]["data"] = result_df

            except Exception as e:
                print("Error: view message below.")
                print(e)
                Log_Errors.append({"message": "Error processing {0} data.".format(layer_events_pop_join)})
                Layer_Errors[layer_events_pop_join] = {"message": "Error processing {0} data.".format(layer_events_pop_join)}


        calculated[layer_facilities_events_join] = { "data": None }
        if no_errors(Log_Errors): 
            try: 
                print("Processing {0} data".format(layer_facilities_events_join))

                if all_events_df.empty == True:
                    result_df = calculated[layer_facilities]["data"]

                else:
                    facilities_df = calculated[layer_facilities]["data"]
                    all_events_df_copy = all_events_df.copy()
                    result_df = local_join_df(left_df= facilities_df, right_df=all_events_df_copy, join_type="left")
                    result_oid = result_fields[layer_facilities_events_join]["object_id_field_name"]

                    result_df = remove_columns(result_df, ['OBJECTID_left', 'OBJECTID_right'])
                    result_df.index = range(0,len(result_df))
                    result_df[result_oid] = result_df.index
                    result_df['ROW'] = result_df.index
                    result_df['Join_Count'] = 1


                calculated[layer_facilities_events_join]["data"] = result_df


            except Exception as e:
                print("Error: view message below.")
                print(e)
                Log_Errors.append({"message": "Error processing {0} data.".format(layer_facilities_events_join)})
                Layer_Errors[layer_facilities_events_join] = {"message": "Error processing {0} data.".format(layer_facilities_events_join)}


        '''
        Dummy row to address ExB Filter widget not clearing data
        For impacted facilities add the following dummy rows
        Create a row in each county for all unique values in [ade_source, setting, event_type, facility_type, urgency(Critical,Elective,Standard)] with county and oes_region and all other values 0
        ade_facility_id = None
        '''
        if no_errors(Log_Errors): 
            try:
                source_df = calculated[layer_facilities_events_join]['data'].copy()
                dummy_df = calculated[layer_facilities_events_join]['data'].copy()

                if all_events_df.empty == True:
                    dummy_rows = pd.DataFrame(data=None, columns=dummy_df.columns, index=dummy_df.index).drop(dummy_df.index)    

                else:
                    ''' Filter on impacted counties '''
                    dummy_df = dummy_df.dropna(subset=['event_type'])
                    dummy_df = dummy_df.drop_duplicates(subset = ["county"])

                    ''' Duplicate each row for unique values in [ade_source, setting, event_type, facility_type, urgency(Critical,Elective,Standard)] with county and oes_region and all other values 0 and ade_facility_id = None'''
                    cols = ['ade_source', 'setting', 'facility_type', 'urgency']
                    event_types = ['PSPS_Active_Impacted', 'PSPS_Potential_Impacted', 'PSPS_Reenergized' 'Fire', 'Earthquake', 'Evacuation_Order', 'Evacuation_Warning', 'Clear_to_Repopulate', 'Shelter_in_Place']
                    update_list = []
                    for col in cols:
                        for val in source_df[col].unique():
                            if val != None and val != '' and val!= 'nan':
                                item = { 'attribute': col, 'value': val}
                                update_list.append(item)
                    for val in event_types:
                        item = { 'attribute': 'event_type', 'value': val}
                        update_list.append(item)

                    dummy_rows = pd.DataFrame(data=None, columns=dummy_df.columns, index=dummy_df.index).drop(dummy_df.index)
                    for index, row in dummy_df.iterrows():
                        for update in update_list: 
                            # Create a row and add it to a new dataframe 
                            tmp_df = {'SHAPE': row['SHAPE'], 'county': row['county'], 'oes_region': row['oes_region'], update['attribute']: update['value'] }
                            dummy_rows = dummy_rows.append(tmp_df, ignore_index=True)

                # dummy_rows = dummy_rows.fillna(0)
                # dummy_rows['ade_facility_id'] = None
                if not (dummy_rows.empty):
                    calculated[layer_facilities_events_join]['data'] = calculated[layer_facilities_events_join]['data'].append(dummy_rows, ignore_index=True)
                    calculated[layer_facilities_events_join]['data'].index = range(0,len(calculated[layer_facilities_events_join]['data']))
                    calculated[layer_facilities_events_join]['data']['OBJECTID'] = calculated[layer_facilities_events_join]['data'].index 

                if not calculated[layer_facilities_events_join]['data'].empty:
                    blank_series = calculated[layer_facilities_events_join]['data'].iloc[0]
                    for index, value in blank_series.items():
                        if index == "SHAPE":
                            continue
                        elif index == "OBJECTID":
                            blank_series[index] = len(calculated[layer_facilities_events_join]['data'].index) 
                        elif index == "ade_source":
                            blank_series[index] = "All"    
                        else: 
                            blank_series[index] = None

                    calculated[layer_facilities_events_join]['data'] = calculated[layer_facilities_events_join]['data'].append(blank_series)
                    
            except Exception as e:
                print("Error: view message below.")
                print(e)
                Log_Errors.append({"message": "Error processing {0} data dummy rows.".format(layer_facilities_events_join)})
                Layer_Errors[layer_facilities_events_join] = {"message": "Error processing {0} data.".format(layer_facilities_events_join)}


        calculated[layer_counties_events_join] = { "data": None }
        if no_errors(Log_Errors): 
            try: 
                print("Processing {0} data".format(layer_counties_events_join))

                counties_df = data_layers[layer_counties]["df"].copy()
                if all_events_df.empty: 
                    result_df = all_events_df

                else: 
                    all_events_df_copy = all_events_df.copy()
                    result_df = local_join_df(left_df=counties_df, right_df=all_events_df_copy, join_type="inner")
                    result_oid = result_fields[layer_counties_events_join]["object_id_field_name"]

                    result_df = remove_columns(result_df, ['OBJECTID_1', 'objectid_12'])
                    result_df.index = range(0,len(result_df))
                    result_df[result_oid] = result_df.index
                    result_df['ROW'] = result_df.index

                calculated[layer_counties_events_join]["data"] = result_df

            except Exception as e:
                print("Error: view message below.")
                print(e)
                Log_Errors.append({"message": "Error processing {0} data.".format(layer_counties_events_join)})


        calculated[layer_events_counties_join_points] = { "data": None }
        if no_errors(Log_Errors): 
            try:
                print("Processing {0} data".format(layer_events_counties_join_points))

                if all_events_df.empty: 
                    result_df = all_events_df

                else: 
                    counties_df = data_layers[layer_counties]["df"].copy()
                    all_events_df_copy = all_events_df.copy()

                    result_df = local_join_df(left_df=all_events_df_copy, right_df=counties_df, join_type="left")
                    result_oid = result_fields[layer_events_counties_join_points]["object_id_field_name"]

                    # Convert geometry
                    sr = result_df.spatial.sr
                    result_df["SHAPE"] = result_df["SHAPE"].apply(lambda geometry: Point({"x" :  geometry.centroid[0], "y" :  geometry.centroid[1], "spatialReference": sr}))

                    remove = ['shape_leng', 'Shape_Length_1', 'Shape_Area_1', 'objectid_12', 'objectid', 'OBJECTID_1', 'OBJECTID_left', 'OBJECTID_right']
                    result_df = remove_columns(result_df, remove)
                    result_df.index = range(0,len(result_df))
                    result_df[result_oid] = result_df.index
                    result_df['ROW'] = result_df.index

                calculated[layer_events_counties_join_points]["data"] = result_df

            except Exception as e:
                print("Error: view message below.")
                print(e)
                Log_Errors.append({"message": "Error processing {0} data.".format(layer_events_counties_join_points)})
                Layer_Errors[layer_events_counties_join_points] = {"message": "Error processing {0} data.".format(layer_events_counties_join_points)}


        calculated[layer_psps_points] = { "data": None }
        if no_errors(Log_Errors): 
            try: 
                print("Processing {0} data".format(layer_psps_points))

                result_df = data_layers[layer_psps]["df"].copy()

                if result_df.size > 0: 
                    sr = result_df.spatial.sr
                    result_df["SHAPE"] = result_df["SHAPE"].apply(lambda geometry: Point({"x" :  geometry.centroid[0], "y" :  geometry.centroid[1], "spatialReference": sr}))

                calculated[layer_psps_points]["data"] = result_df

            except Exception as e:
                print("Error: view message below.")
                print(e)
                Log_Errors.append({"message": "Error processing {0} data.".format(layer_psps_points)})
                Layer_Errors[layer_psps_points] = {"message": "Error processing {0} data.".format(layer_psps_points)}

        if result_df.size > 0: 
            result_df["geometry"] = result_df["SHAPE"].apply(lambda x: x.as_shapely if x is not None else None)
        sdf_dic = result_df.to_dict('list')
        result_gdf = gpd.GeoDataFrame(sdf_dic)
        cwd = os.getcwd()

        view_item = gis.content.get(target_update_view)
        target = OverwriteFS.getFeatureServiceTarget(view_item, verbose=True)

        if "success" in target and (not target["success"] or not target["items"]):
            # -AddRelated A/B services
            print("No target found. Adding A/B related items.")
            OverwriteFS.updateRelationships(
                view=view_item, 
                relateIds=[target_update_layer_A, target_update_layer_B], 
                verbose=True)
            target = OverwriteFS.getFeatureServiceTarget(view_item, verbose=True)

        target_filename = target['filename']
        target_filepath = os.path.join(cwd, target_filename)

        result_gdf.to_file(filename=target_filepath, driver='GPKG')


        # Swap view
        response = False
        if not (result_df is None): 
            try:
                # Determine which layer is Current and which layer is Target at the moment
                outcome = OverwriteFS.getFeatureServiceTarget(view_item, verbose=None, outcome=None, ignoreDataItemCheck=False)
                #  will check for error here and action it

                # Check results
                dry_run = False  # Set it to true to run it in test mode where it just veries everything without making an overwrite or swapping
                outcome = OverwriteFS.swapFeatureViewLayers(view_item, updateFile=target_filepath, touchItems=True, verbose=True, touchTimeSeries=False, outcome=None, noIndexes=False, preserveProps=True, noWait=False, noProps=False, converter=None, dryRun=dry_run, noSwap=False, ignoreAge=False)

                # Check results
                if outcome["success"]:
                    print( "Service Overwrite was a Success!")

                elif outcome["success"] == False:
                    print( "Service Overwrite Failed!")
            except Exception as e:
                response = e
                print('Failed to update: {}'.format(e))


        ''' Calculation helpers '''
        def normalize_v1(row):
            if row[pop_sub_fieldname] == 0:
                return 0
            else:
                return row[pop_sub_fieldname] / row['pop_total']
            
        def normalize_zscore(row):
            rate = row[pop_sub_fieldname]
            #https://stackoverflow.com/questions/23451244/how-to-zscore-normalize-pandas-column-with-nans      
            z_score = (rate - pop_score_df[pop_sub_fieldname].mean())/pop_score_df[pop_sub_fieldname].std(ddof=0)
            return z_score

        def rate(row):
            if row[pop_sub_fieldname] == 0:
                return 0
            else:
                rate = (row[pop_sub_fieldname] / population_total) * 100000
                return rate
            
        calculated[layer_population_score] = { "data": None }
        if no_errors(Log_Errors): 
            try: 
                ''' 
                Create Population_Score dataframe
                Use copy of Population and keep zipcode, oes region, and total population 
                Create new row for each population type in each population with value (sub population) 
                '''
                pop_df = calculated[layer_population]["data"].copy()

                population_total = pop_df[pop_total_fieldname].sum()

                pop_score_df = pop_df.melt(["OBJECTID",pop_zipcode_fieldname, pop_region_fieldname, pop_total_fieldname, pop_shape_fieldname],
                                    var_name=pop_type_fieldname, 
                                    value_vars= population_types_list,
                                    value_name=pop_sub_fieldname)
                pop_score_df.sort_values(by=[pop_zipcode_fieldname])

                ''' Iterate through to add population variable statewide sums for each zip (Note: subpop is population for a given zipcode) '''
                pop_score_df.insert(1,"normalized_pop", np.nan)
                result = pop_score_df.groupby(by=[pop_type_fieldname]).sum() 
                pop_score_df['pop_total'] = pop_score_df.apply(lambda row : result.loc[row[pop_type_fieldname]][pop_sub_fieldname], axis = 1)

                ''' Calculate the zscore, normalized, and rate'''
                pop_score_df = pop_score_df.rename(columns={pop_total_fieldname: pop_tot_fieldname})
                pop_score_df[pop_zscore_fieldname] = pop_score_df.apply(normalize_zscore, axis=1)
                pop_score_df['normalized_pop'] = pop_score_df.apply(normalize_v1, axis=1)
                pop_score_df['rate'] = pop_score_df.apply(rate, axis=1)

                ''' Join event data '''
                if not all_events_df.empty: 
                    pop_score_df = local_join_df(pop_score_df, all_events_df, "left")
                    pop_score_df = pop_score_df.rename(columns={'OBJECTID_left': 'id', 'OBJECTID_right': 'OBJECTID'})

                ''' Field mapping '''
                var_map_dict = {
                "L0_over85": "Over 85 Population", 
                "L0_over75": "Over 75 Population",
                "L0_over65": "Over 65 Population",
                "L0_over55": "Over 55 Population",
                "L0_dhcs_medi_cal_dme": "DHCS Medi-Cal DME",
                "L0_dhcs_medi_cal_benes": "DCHS Medical Beneficiaries",
                "L0_cdss_ihss_recpts": "CDSS IHSS Recipients",
                "L0_cdss_calfresh_recpts": "CDSS CalFresh Recipients", 
                "L0_cdph_wic_partipts":  "CDPH WIC Participants", 
                "L0_cdph_medicare_dme":  "CDPH Medicare DME",
                "L0_cdds_clients": "CDDS Clients",
                "total_pop" : "Total Population"
                }
                pop_score_df['pop_type_alias'] = pop_score_df[pop_type_fieldname].map(var_map_dict)

                ''' Add blank row with tot_zip_pop'''
                blank = pop_score_df.copy()
                if not all_events_df.empty: 
                    blank = blank.drop_duplicates(subset = ["L0_zip_code", 'event_name'])
                else:
                    blank = blank.drop_duplicates(subset = ["L0_zip_code"])

                blank.loc[:,'pop_type'] = 'tot_zip_pop'
                blank.loc[:,'pop_type_alias'] = 'Total Zipcode Population'
                blank.loc[:,'sub_pop'] = blank.loc[:,'tot_zip_pop']
                blank.loc[:,'pop_total'] = population_total

                blank_fields = ['zscore', 'rate']
                # blank_fields = ['pop_total', 'zscore', 'rate', 'event_id', 'event_name', 'event_type']

                for field in blank_fields:
                    blank.loc[:,field] = np.nan

                pop_score_df = pop_score_df.append(blank)
                pop_score_df.index = range(0,len(pop_score_df))
                pop_score_df['OBJECTID'] = pop_score_df.index

                ''' Results '''
                calculated[layer_population_score]["data"] = pop_score_df
                
            except Exception as e:
                print("Error: view message below.")
                print(e)
                Log_Errors.append({"message": "Error processing {0} data.".format(layer_population_score)})


        if no_errors(Log_Errors): 
            try: 
                pop_df = calculated[layer_population]["data"].copy()
                pop_county_df = calculated[layer_population]["data"].copy().drop_duplicates(subset='L0_county', keep="first")
                print("Appending {} dummy records to {} with {} records.".format(len(pop_county_df.index), layer_population, len(pop_df.index)))

                temp_series = []
                mod_fields = [{"name": "PSPS_Active_Impacted", "value": "YES"},
                            {"name": "Evacuation_Order", "value": "YES"},
                            {"name": "grid_value", "value": 6},
                            {"name": "Fire_Buffer_Distance", "value": 0}, 
                            {"name": "L0_cdss_calfresh_recpts", "value": 0}, 
                            {"name": "L0_cdss_ihss_recpts", "value": 0}, 
                            {"name": "L0_cdph_wic_partipts", "value": 0}, 
                            {"name": "L0_cdph_medicare_benes", "value": 0}, 
                            {"name": "L0_cdph_medicare_dme", "value": 0}, 
                            {"name": "L0_dhcs_medi_cal_benes", "value": 0}, 
                            {"name": "L0_dhcs_medi_cal_dme", "value": 0}, 
                            {"name": "L0_cdds_clients", "value": 0}, 
                            {"name": "L0_over55", "value": 0}, 
                            {"name": "L0_over65", "value": 0}, 
                            {"name": "L0_over75", "value": 0}, 
                            {"name": "L0_tot_zip_pop", "value": 0}, 
                            {"name": "L0_over85", "value": 0}] 

                for idx, val in enumerate(pop_county_df.itertuples(index=False)):
                    dup_row = list(val)
                    columns = pop_county_df.iloc[[idx]].columns

                    for f in mod_fields: 
                        col_idx = columns.get_loc(f["name"])
                        dup_row[col_idx] = f["value"]

                    temp_series.append(dup_row)

                pop_dummy_df = pd.DataFrame(temp_series, columns=pop_county_df.columns)
                pop_df = pop_df.append(pop_dummy_df, ignore_index=True)

                print("Records appended. Result record count: {}.".format(len(pop_df.index)))
                calculated[layer_population]["data"] = pop_df
                
            except Exception as e:
                print("Error: view message below.")
                print(e)
                Log_Errors.append({"message": "Error adding dummy rows to {0} data.".format(layer_population)})


        toc = time.perf_counter()
        print(f"Processed data in {toc - tic:0.4f} seconds")

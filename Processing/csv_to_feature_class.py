import arcpy
import os

def create_gdb(gdb_path, gdb_name):
    """
    Make a file geodatabase if one does not already exist
    :param gdb_path: directory path where file geodatabase will be saved
    :param gdb_name: name of output file geodatabase
    :return: path of output or existing file geodatabase
    """
    if not arcpy.Exists(os.path.join(gdb_path, gdb_name)):
        arcpy.management.CreateFileGDB(gdb_path, gdb_name)

    return os.path.join(gdb_path, gdb_name)

def XYtable_to_fc(input_table, output_fc, longitude_fld, latitude_fld, espg_number):
    """
    Converts csv or txt file containing coordinates to a feature class
    :param input_table the input .csv file
    :param output_fc the name of the output feature class
    :param longitude_fld the name of the longitude field in the input data
    :param latitude_fld the name of the latitude field in the input data
    :param espg_number the espg number of the original data
    """
    arcpy.env.workspace = output_fc.split('.gdb')[0] + '.gdb'
    arcpy.env.overwriteOutput = True
    arcpy.management.XYTableToPoint(input_table, output_fc, longitude_fld, latitude_fld,
                                    coordinate_system=arcpy.SpatialReference(espg_number))


def project_to_wgs84(workspace, input_FC, output_FC, epsg=None):
    """
    Projects the input file into WGS84
    :param workspace the path containing the layer that needs the srs changed
    :param input_FC the name of the input layer
    :param output_FC the name of the outplut layer
    :param espg_number the espg number of the target srs
    """
    arcpy.env.workspace = workspace
    arcpy.env.overwriteOutput = True

    if epsg == None:
        arcpy.Project_management(input_FC, output_FC, arcpy.SpatialReference(4326))
    else:
        transformations = arcpy.ListTransformations(arcpy.SpatialReference(epsg), arcpy.SpatialReference(4326))
        complete = False
        t_index = 0
        while t_index < len(transformations) and complete == False:
            try:
                arcpy.Project_management(input_FC, output_FC, arcpy.SpatialReference(4326),
                                         in_coor_system=arcpy.SpatialReference(epsg),
                                         transform_method=transformations[t_index])
                complete = True
            except Exception as e:
                print(f'\tWARNING: Unable to reproject using datum shift - {transformations[t_index]}')
                t_index += 1
        if complete == False:
            try:
                arcpy.Project_management(input_FC, output_FC, arcpy.SpatialReference(4326),
                                         in_coor_system=arcpy.SpatialReference(epsg))
            except Exception as e:
                print(f"\t\t\tERROR:") 


def csv_to_fc(file_path, process_file_path, original_dataset_name, name, working_gdb, epsg_num, orig_lat, orig_lon, processed_fc_name):
    """
    Convert csv to feature class

    Args:
        file_path: path to original csv
        process_file_path: path to processed data
        original_dataset_name: name of original csv
        name: name of feature class
        working_gdb: name of output gdb
        epsg_num: orignal spatial reference system EPSG number
        orig_lat: latitude field name
        orig_lon: longitude field name
        processed_fc_name: name of processed feature class
    Returns:

    """

    # Set path
    csvfile = os.path.join(file_path, original_dataset_name)

    # Make a gdb
    file_gdb = create_gdb(process_file_path, working_gdb)

    # Temporary feature class
    temp_file = f"{name.replace(' ', '_')}_tbd"

    # Convert csv to fc
    arcpy.env.workspace = file_gdb
    feature_class = os.path.join(file_gdb, temp_file)
    XYtable_to_fc(csvfile, feature_class, orig_lon, orig_lat, epsg_num)

    # Project original feature class to wgs84
    project_to_wgs84(file_gdb, feature_class, os.path.join(file_gdb, processed_fc_name), epsg=epsg_num)




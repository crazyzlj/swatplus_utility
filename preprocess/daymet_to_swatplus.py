# -*- coding: utf-8 -*-
import math
import os, sys
from pygeoc.utils import FileClass, UtilClass, MathClass, StringClass

YEAR_LINE = "years:"
VAR_LINE  = "variables:"
DAYMET_VARIABLES = ['dayl', 'prcp', 'srad', 'swe', 'tmax', 'tmin', 'vp']

def write_swatplus_stationdata(fname, title_str, site_str, data):
    fname = FileClass.get_file_fullpath_string(fname)
    name = fname.split(os.sep)[-1]
    with open(fname, 'w', encoding='utf-8') as f:
        f.write(name+'\n')
        f.write(title_str+'\n')
        f.write(site_str+'\n')
        for l in data:
            f.write('    '.join(map(str, l))+'\n')
    print('write %s done!' % fname)

def write_swatplus_stationdata_indexfile(fname, title_str, data):
    fname = FileClass.get_file_fullpath_string(fname)
    name = fname.split(os.sep)[-1]
    with open(fname, 'w', encoding='utf-8') as f:
        f.write(name+'\n')
        f.write(title_str+'\n')
        for l in data:
            f.write(l+'\n')
    print('write %s done!' % fname)

def parse_params(line, param_list):
    start_idx = line.index(":") + 1
    line_split = line[start_idx:].split(",")
    requested_params = []
    for elem in line_split:
        if elem in param_list:
            requested_params.append(elem)
    return requested_params

def main(data_file, out_dir,pcp_cli_list,slr_cli_list,temp_cli_list,hum_cli_list):
    corename = FileClass.get_core_name_without_suffix(data_file)
    inF = open(data_file)
    lines = inF.read().lower().replace(" ", "").split("\n")
    inF.close()
    yr_idx = -1
    yday_idx = -1
    dayl_idx = -1
    pcp_idx = -1
    # Average daily solar radiation MJ/m^2/day: srad (W/m^2) * dayl (s) * 0.000001
    slr_idx = -1
    tmp_max_idx = -1
    tmp_min_idx = -1
    vp_idx = -1
    # Saturated vapor pressure (satp): 0.6108 * exp(17.27 * temp / (temp + 237.3))
    # Relative humidity: vp (kPa) / satp (kPa)
    pcp_data = list()
    slr_data = list()
    tmp_data = list()
    hum_data = list()
    nbyr = 0
    tstep = 0
    lat = -1
    lon = -1
    elev = -1
    cur_year = -1
    data_len = 0
    for line in lines:
        if line == '':
            continue
        if 'latitude' in line:
            values = StringClass.extract_numeric_values_from_string(line)
            if len(values) == 2:
                lat = values[0]
                lon = values[1]
        elif 'x&y' in line:
            continue
        elif 'elevation' in line:
            values = StringClass.extract_numeric_values_from_string(line)
            if len(values) == 1:
                elev = values[0]
        elif 'allyears' in line:
            continue
        elif 'howtocite' in line:
            continue
        elif 'year,yday' in line:
            fields = line.split(",")
            for i, fld in enumerate(fields):
                if 'year' in fld:
                    yr_idx = i
                elif 'yday' in fld:
                    yday_idx = i
                elif 'dayl' in fld:
                    dayl_idx = i
                elif 'prcp' in fld:
                    pcp_idx = i
                elif 'srad' in fld:
                    slr_idx = i
                elif 'vp' in fld:
                    vp_idx = i
                elif 'tmax' in fld:
                    tmp_max_idx = i
                elif 'tmin' in fld:
                    tmp_min_idx = i
                else:
                    pass
        else:
            if yr_idx < 0 or yday_idx < 0:
                continue
            values = StringClass.extract_numeric_values_from_string(line)
            if values is None or len(values) < 3:
                print(line, " does not have enough data!")
                continue
            data_len += 1

            yr = values[yr_idx]
            if yr > cur_year:
                cur_year = yr
                nbyr += 1
            yday = values[yday_idx]
            if pcp_idx > 0:
                pcp_data.append([yr, yday, values[pcp_idx]])
            if slr_idx > 0 and dayl_idx > 0:
                slr_data.append([yr, yday, values[slr_idx] * values[dayl_idx] * 0.000001])
            if tmp_max_idx > 0 and tmp_min_idx > 0:
                tmp_data.append([yr, yday, values[tmp_max_idx], values[tmp_min_idx]])
                if vp_idx > 0:
                    # satvp = math.pow(10, 8.1 - 1731 / (233 + (values[tmp_max_idx] + values[tmp_min_idx]) / 2))
                    tmp_avg = (values[tmp_max_idx] + values[tmp_min_idx]) / 2
                    satvp = 0.6108 * math.exp(17.27 * tmp_avg / (tmp_avg + 237.3))
                    ratio = values[vp_idx] * 0.001 / satvp
                    if ratio < 0:
                        ratio = 0
                    if ratio > 1:
                        ratio = 1.0
                    hum_data.append([yr, yday, ratio])
    print("total: %d" % data_len)
    title_str = 'nbyr     tstep       lat       lon      elev'
    site_str = '%d     %d       %f       %f      %f' % (nbyr, tstep, lat, lon, elev)
    print(site_str)
    if pcp_idx > 0 and len(pcp_data) == data_len:
        fname = corename + 'pcp.pcp'
        write_swatplus_stationdata(out_dir+os.sep+fname, title_str, site_str, pcp_data)
        pcp_cli_list.append(fname)
    if slr_idx > 0 and len(slr_data) == data_len:
        fname = corename + 'slr.slr'
        write_swatplus_stationdata(out_dir+os.sep+fname, title_str, site_str, slr_data)
        slr_cli_list.append(fname)
    if tmp_max_idx > 0 and tmp_min_idx > 0 and len(tmp_data) == data_len:
        fname = corename + 'temp.tem'
        write_swatplus_stationdata(out_dir+os.sep+fname, title_str, site_str, tmp_data)
        temp_cli_list.append(fname)
        if vp_idx > 0 and len(hum_data) == data_len:
            fname = corename + 'hmd.hmd'
            write_swatplus_stationdata(out_dir+os.sep+fname, title_str, site_str, hum_data)
            hum_cli_list.append(fname)



if __name__ == "__main__":
    daymet_dir = r'd:\data_m\manitowoc\weather\1013\daymet'
    site_file = 'latlon.txt'
    out_dir = r'd:\data_m\manitowoc\weather\1013\swatplus_TxtInOut'
    inF = open(daymet_dir + os.sep + site_file)
    lines = inF.read().lower().replace(" ", "").split("\n")
    inF.close()
    pcp_cli_list = []
    slr_cli_list = []
    temp_cli_list = []
    hum_cli_list = []
    for line in lines:
        line = line.lower()
        if line:
            if VAR_LINE in line:
                requested_vars = parse_params(line, DAYMET_VARIABLES)
            elif YEAR_LINE in line:
                continue
            else:
                line_split = line.split(",")
                if (len(line_split) > 2):
                    fname = line_split[0]
                    lat = line_split[1]
                    lon = line_split[2]
                    main(daymet_dir+os.sep+fname, out_dir,pcp_cli_list,slr_cli_list,temp_cli_list,hum_cli_list)
                else:
                    continue
    if len(pcp_cli_list) > 0:
        write_swatplus_stationdata_indexfile(out_dir+os.sep+'pcp.cli', 'filename', pcp_cli_list)
    if len(slr_cli_list) > 0:
        write_swatplus_stationdata_indexfile(out_dir + os.sep + 'slr.cli', 'filename', slr_cli_list)
    if len(temp_cli_list) > 0:
        write_swatplus_stationdata_indexfile(out_dir + os.sep + 'tmp.cli', 'filename', temp_cli_list)
    if len(hum_cli_list) > 0:
        write_swatplus_stationdata_indexfile(out_dir + os.sep + 'hmd.cli', 'filename', hum_cli_list)

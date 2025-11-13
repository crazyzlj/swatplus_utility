import pandas as pd
import dataretrieval.nwis as nwis

if __name__ == "__main__":
    # usgs_site_id = '04085427'
    usgs_site_id = '435950087572701'
    basic_info = nwis.get_info(sites=usgs_site_id)
    stime = '1998-01-01'
    etime = '2024-12-31'
    service = 'dv'  # daily mean value
    param = '00060'
    fld_value = param + '_Mean'
    cfs_to_cms = 0.0283168466
    out_csv = r'D:\data_m\manitowoc\observed\flow_cms_usgs%s.csv' % usgs_site_id
    q_data = nwis.get_record(sites=usgs_site_id, service=service,
                             start=stime, end=etime, parameterCd=param)
    # print(q_data)

    output_df = pd.DataFrame()

    output_df['Date'] = q_data.index.strftime('%Y/%m/%d')
    output_df['Value'] = q_data[fld_value].values * cfs_to_cms

    output_df.to_csv(out_csv, index=False, float_format='%.8f')

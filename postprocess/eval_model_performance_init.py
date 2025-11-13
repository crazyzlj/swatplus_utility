import os
import numpy
from typing import Union, List
from pygeoc.utils import MathClass

# 请你用Python实现模型模拟性能指标的计算，并利用matplotlib绘制各变量的模拟与实测对比图，具体要求和流程如下：
# 1. 以eval_model_performance.py中定义的输入数据和参数为基础进行代码实现，该代码中包括：
#    （1）计算模型模拟性能指标的函数（NSE、R_square、RMSE、PBIAS和RSR）；
#    （2）输入数据的定义，sim_dir中保存各变量的模拟数据，obs_dir中保存各变量的实测数据，precip_file是降水实测数据；
#    （3）输入参数，plot_stime和plot_etime是应用于所有变量绘图横坐标的起止时间；
#    （4）输入参数conf是一个字典，各变量的名称是key，value是一个字典，包括模拟数据sim_file，实测数据obs_file，
#        单位unit（名称和unit组合作为绘图左Y轴名），率定期cali_stime和cali_etime，验证期vali_stime和vali_etime，
#        绘图的样式plot_style（点线图为dotline，柱状图为bar）
# 2. 按照conf中定义的变量逐一处理：
#    （1）读取模拟数据和实测数据，利用时间进行匹配，完全匹配的数据，按照率定期和验证期筛选后分别计算模型性能指标；
#    （2）绘图：X轴是日期，左Y轴是模拟变量，右Y轴（方向向下）为降雨量（Label为Precipitation (mm)），
#             降雨绘图用柱状图，各变量根据plot_style设定的样式。图中用深灰色、虚线分隔率定期与验证期，并分别标注模型性能指标结果（NSE、PBIAS和RSR），保留3位小数，
#             验证期可能早于或晚于率定期，有些变量没有验证期则无需用虚线分隔
# 3. 绘图代码可参考plot_timeseries.py，结果图样式可参考Q-2013-01-01-2015-12-31.png
# 4. 所有函数中尽量不要有写死的配置，尽量封装成可复用的函数
#

# Please use the four functions in MathClass to calculate model performance
obs_array = []  # type: Union[numpy.ndarray, List[Union[float, int]]]
sim_array = []  # type: Union[numpy.ndarray, List[Union[float, int]]]
nse = MathClass.nashcoef(obs_array, sim_array)
r_square = MathClass.rsquare(obs_array, sim_array)
rmse = MathClass.rmse(obs_array, sim_array)
pbias = MathClass.pbias(obs_array, sim_array)
rsr = MathClass.rsr(obs_array, sim_array)

if __name__ == '__main__':
    sim_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\Results\OutletsResults'
    obs_dir = r'D:\data_m\manitowoc\observed'
    precip_file = obs_dir + os.sep + 'precip.csv'
    plot_stime = '2008/1/1'  # Start datetime of all plots
    plot_etime = '2024/12/31'  # End datetime of all plots
    ['_usgs04085427', '_363375', '_10020782', '_363313']

    # Filename format of simulated data: simu_<variable>_<day or mon>_<site_id>.csv, e.g., simu_flo_out_day_usgs04085427.csv
    # Filename format of observed data: <variable>_<day or mon>_<site_id>.csv, e.g., obs_flo_out_mon_usgs04085427.csv
    # Output figure name format will be: <variable>_<day or mon>_<site_id>.jpg, e.g., flo_out_mon_usgs04085427.csv
    {'site_id': {'variable': {'day': {'ylabel': 'xxx',
                                      'plot_style': 'dotline, bar, or point',
                                      'cali_stime': '2014/1/1',
                                      # Start datetime of calibration period, in format YYYY/MM/DD
                                      'cali_etime': '2024/12/31',
                                      # End datetime of calibration period, in format YYYY/MM/DD
                                      'vali_stime': '2008/1/1',
                                      # Start datetime of validation period, in format YYYY/MM/DD
                                      'vali_etime': '2013/12/31'
                                      },
                              'mon': {'ylabel': 'xxx',
                                      'plot_style': 'dotline, bar, or point',
                                      'cali_stime': '2014/1',
                                      # Start datetime of calibration period, in format YYYY/MM
                                      'cali_etime': '2024/12',
                                      # End datetime of calibration period, in format YYYY/MM
                                      'vali_stime': '2008/1',
                                      # Start datetime of validation period, in format YYYY/MM
                                      'vali_etime': '2013/12'
                                      }}
                 }
     }

    conf = {'usgs04085427': {'flo_out': {'day': {'ylabel': 'Q(m^3/s)',
                                                 'plot_style': 'dotline',
                                                 'cali_stime': '2014/1/1',
                                                 'cali_etime': '2024/12/31',
                                                 'vali_stime': '2008/1/1',
                                                 'vali_etime': '2013/12/31'},
                                         'mon': {'ylabel': 'Q(m^3/s)',
                                                 'plot_style': 'dotline',
                                                 'cali_stime': '2014/1',
                                                 'cali_etime': '2024/12',
                                                 'vali_stime': '2008/1',
                                                 'vali_etime': '2013/12'}},
                             'sed_out': {'day': {'ylabel': 'Sed(tons)',
                                                 'plot_style': 'point',
                                                 'cali_stime': '2011/1/1',
                                                 'cali_etime': '2024/12/31',
                                                 'vali_stime': '',
                                                 'vali_etime': ''},
                                         'mon': {'ylabel': 'Sed(tons)',
                                                 'plot_style': 'dotline',
                                                 'cali_stime': '2014/1',
                                                 'cali_etime': '2019/12',
                                                 'vali_stime': '2008/1',
                                                 'vali_etime': '2013/12'}},
                             'no3_out': {'day': {'ylabel': 'NO3 (Kg N)',
                                                 'plot_style': 'point',
                                                 'cali_stime': '2008/1/1',
                                                 'cali_etime': '2024/12/31',
                                                 'vali_stime': '',
                                                 'vali_etime': ''}},
                             'nh3_out': {'day': {'ylabel': 'NH3 (Kg N)',
                                                 'plot_style': 'point',
                                                 'cali_stime': '2008/1/1',
                                                 'cali_etime': '2024/12/31',
                                                 'vali_stime': '',
                                                 'vali_etime': ''}},
                             'orgn_out': {'day': {'ylabel': 'OrgN (Kg N)',
                                                  'plot_style': 'point',
                                                  'cali_stime': '2008/1/1',
                                                  'cali_etime': '2023/12/31',
                                                  'vali_stime': '',
                                                  'vali_etime': ''}},
                             'tn_out': {'day': {'ylabel': 'TN (Kg N)',
                                                'plot_style': 'point',
                                                'cali_stime': '2008/1/1',
                                                'cali_etime': '2024/12/31',
                                                'vali_stime': '',
                                                'vali_etime': ''}},
                             'solp_out': {'day': {'ylabel': 'SolP (Kg P)',
                                                  'plot_style': 'point',
                                                  'cali_stime': '2011/1/1',
                                                  'cali_etime': '2024/12/31',
                                                  'vali_stime': '',
                                                  'vali_etime': ''}},
                             'tp_out': {'day': {'ylabel': 'TP (Kg P)',
                                                'plot_style': 'point',
                                                'cali_stime': '2011/1/1',
                                                'cali_etime': '2024/12/31',
                                                'vali_stime': '',
                                                'vali_etime': ''},
                                        'mon': {'ylabel': 'TP (Kg P)',
                                                'plot_style': 'dotline',
                                                'cali_stime': '2014/1',
                                                'cali_etime': '2019/12',
                                                'vali_stime': '2008/1',
                                                'vali_etime': '2013/12'}},
                             },
            '363375': {'flo_out': {'mon': {'ylabel': 'Q (m^3/s)',
                                           'plot_style': 'dotline',
                                           'cali_stime': '2017/7',
                                           'cali_etime': '2019/5',
                                           'vali_stime': '',
                                           'vali_etime': ''}},
                       'sed_out': {'mon': {'ylabel': 'Sed (tons)',
                                           'plot_style': 'dotline',
                                           'cali_stime': '2017/7',
                                           'cali_etime': '2019/5',
                                           'vali_stime': '',
                                           'vali_etime': ''}},
                       'tp_out': {'mon': {'ylabel': 'TP (Kg P)',
                                          'plot_style': 'dotline',
                                          'cali_stime': '2017/7',
                                           'cali_etime': '2019/5',
                                           'vali_stime': '',
                                           'vali_etime': ''}},
                       },
            '10020782': {'flo_out': {'mon': {'ylabel': 'Q (m^3/s)',
                                             'plot_style': 'dotline',
                                             'cali_stime': '2017/7',
                                           'cali_etime': '2019/10',
                                           'vali_stime': '',
                                           'vali_etime': ''}}
                         },
            '363313': {'flo_out': {'mon': {'ylabel': 'Q (m^3/s)',
                                           'plot_style': 'dotline',
                                           'cali_stime': '2017/7',
                                           'cali_etime': '2019/10',
                                           'vali_stime': '',
                                           'vali_etime': ''}},
                       'sed_out': {'mon': {'ylabel': 'Sed (tons)',
                                           'plot_style': 'dotline',
                                           'cali_stime': '2017/7',
                                           'cali_etime': '2019/10',
                                           'vali_stime': '',
                                           'vali_etime': ''}},
                       'tp_out': {'mon': {'ylabel': 'TP (Kg P)',
                                          'plot_style': 'dotline',
                                          'cali_stime': '2017/7',
                                           'cali_etime': '2019/10',
                                           'vali_stime': '',
                                           'vali_etime': ''}},
                       }
            }

    # conf = {'Q': {'sim_file': 'simu_flo_out_day_usgs04085427.csv',  # file located in sim_dir
    #               'obs_file': 'flow_cms_usgs04085427.csv',  # file located in obs_dir
    #               'unit': 'm^3/s',  # unit, so the left Y-axes label will be Q(m^/s)
    #               'plot_style': 'dotline',
    #               'cali_stime': '2014/1/1',  # Start datetime of calibration period, in format YYYY/MM/DD
    #               'cali_etime': '2024/12/31',  # End datetime of calibration period, in format YYYY/MM/DD
    #               'vali_stime': '2008/1/1',  # Start datetime of validation period, in format YYYY/MM/DD
    #               'vali_etime': '2013/12/31'},  # End datetime of validation period, in format YYYY/MM/DD
    #         'Sed': {'sim_file': 'simu_sed.csv',
    #                 'obs_file': 'sed_usgs04085427.csv',
    #                 'unit': 'tons',
    #                 'plot_style': 'bar',
    #                 'cali_stime': '2011/1/1',
    #                 'cali_etime': '2024/12/31',
    #                 'vali_stime': '',  # one of vali_stime and vali_etime is '' or None means
    #                 'vali_etime': ''},  # no validation period is set for model performance and plotting
    #         'NO3': {'sim_file': 'simu_no3.csv',
    #                 'obs_file': 'no3_usgs04085427.csv',
    #                 'unit': 'kg N',
    #                 'plot_style': 'bar',
    #                 'cali_stime': '2008/1/1',
    #                 'cali_etime': '2024/12/31',
    #                 'vali_stime': '',
    #                 'vali_etime': ''},
    #         'NH3': {'sim_file': 'simu_nh3.csv',
    #                 'obs_file': 'nh3_usgs04085427.csv',
    #                 'unit': 'kg N',
    #                 'plot_style': 'bar',
    #                 'cali_stime': '2008/1/1',
    #                 'cali_etime': '2024/12/31',
    #                 'vali_stime': '',
    #                 'vali_etime': ''},
    #         'OrgN': {'sim_file': 'simu_orgn.csv',
    #                  'obs_file': 'orgn_usgs04085427.csv',
    #                  'unit': 'kg N',
    #                  'plot_style': 'bar',
    #                  'cali_stime': '2008/1/1',
    #                  'cali_etime': '2023/12/31',
    #                  'vali_stime': '',
    #                  'vali_etime': ''},
    #         'TN': {'sim_file': 'simu_tn.csv',
    #                'obs_file': 'tn_usgs04085427.csv',
    #                'unit': 'kg N',
    #                'plot_style': 'bar',
    #                'cali_stime': '2008/1/1',
    #                'cali_etime': '2024/12/31',
    #                'vali_stime': '',
    #                'vali_etime': ''},
    #         'SolP': {'sim_file': 'simu_solp.csv',
    #                  'obs_file': 'solp_usgs04085427.csv',
    #                  'unit': 'kg P',
    #                  'plot_style': 'bar',
    #                  'cali_stime': '2011/1/1',
    #                  'cali_etime': '2024/12/31',
    #                  'vali_stime': '',
    #                  'vali_etime': ''},
    #         'TP': {'sim_file': 'simu_tp.csv',
    #                'obs_file': 'tp_usgs04085427.csv',
    #                'unit': 'kg P',
    #                'plot_style': 'bar',
    #                'cali_stime': '2011/1/1',
    #                'cali_etime': '2024/12/31',
    #                'vali_stime': '',
    #                'vali_etime': ''}
    # }

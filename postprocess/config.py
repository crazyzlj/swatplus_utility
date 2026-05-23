
# Study area specific configurations for postprocess
# outlet channel with daily observations
CHANNEL_NUMBER = [68]
SUFFIX = ['_usgs04085427']

# all channels with monthly observations
CHANNEL_NUMBERS = [68, 170, 157, 74]
SUFFIXES = ['_usgs04085427', '_363375', '_10020782', '_363313']

GRID_IDS = [6938, 6938, 6938, 6939, 6940, 6940, 7067, 7067, 7068, 7069, 7069, 8746, 9577, 9577,
            9707, 9834, 10929, 12025, 12448, 15015]
WELL_IDS = ['PK237', 'PK335', 'VT642', 'PK331', 'PK240', 'VT691', 'PK313', 'PK345', 'PK312',
            'PK238', 'PK239', 'ZS174', 'VU807', 'VU806', 'VU861', 'VU732', 'SO614', 'UX025',
            'VA157', 'RG659']

PLOT_STIME = '2008/1/1'
PLOT_ETIME = '2024/12/31'

CONF = {'usgs04085427': {'flo_out': {'day': {'ylabel': 'Q(m^3/s)',
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
                   },
        'PK237': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'PK238': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'PK239': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              },
                  'gw_no3': {'day': {'ylabel': 'm',
                                     'plot_style': 'dotline',
                                     'cali_stime': '2008/1/1',
                                     'cali_etime': '2024/12/31',
                                     'vali_stime': '',
                                     'vali_etime': ''}
                             }
                  },
        'PK240': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'PK312': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'PK313': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'PK331': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'PK335': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'PK345': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'VT642': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'VT691': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'VU732': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'VU806': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              },
                  'gw_no3': {'day': {'ylabel': 'm',
                                     'plot_style': 'dotline',
                                     'cali_stime': '2008/1/1',
                                     'cali_etime': '2024/12/31',
                                     'vali_stime': '',
                                     'vali_etime': ''}
                             }
                  },
        'VU807': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'VU861': {'gw_head': {'day': {'ylabel': 'm',
                                      'plot_style': 'dotline',
                                      'cali_stime': '2014/1/1',
                                      'cali_etime': '2024/12/31',
                                      'vali_stime': '2008/1/1',
                                      'vali_etime': '2013/12/31'}
                              }
                  },
        'RG659': {'gw_no3': {'day': {'ylabel': 'm',
                                     'plot_style': 'dotline',
                                     'cali_stime': '2014/1/1',
                                     'cali_etime': '2024/12/31',
                                     'vali_stime': '2008/1/1',
                                     'vali_etime': '2013/12/31'}
                             }
                  },
        'SO614': {'gw_no3': {'day': {'ylabel': 'm',
                                     'plot_style': 'dotline',
                                     'cali_stime': '2014/1/1',
                                     'cali_etime': '2024/12/31',
                                     'vali_stime': '2008/1/1',
                                     'vali_etime': '2013/12/31'}
                             }
                  },
        'UX025': {'gw_no3': {'day': {'ylabel': 'm',
                                     'plot_style': 'dotline',
                                     'cali_stime': '2008/1/1',
                                     'cali_etime': '2024/12/31',
                                     'vali_stime': '',
                                     'vali_etime': ''}
                             }
                  },
        'VA157': {'gw_no3': {'day': {'ylabel': 'm',
                                     'plot_style': 'dotline',
                                     'cali_stime': '2008/1/1',
                                     'cali_etime': '2024/12/31',
                                     'vali_stime': '',
                                     'vali_etime': ''}
                             }
                  },
        'ZS174': {'gw_no3': {'day': {'ylabel': 'm',
                                     'plot_style': 'dotline',
                                     'cali_stime': '2008/1/1',
                                     'cali_etime': '2024/12/31',
                                     'vali_stime': '',
                                     'vali_etime': ''}
                             }
                  }
    }
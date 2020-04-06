#!/usr/bin/python3

import subprocess
from bases.FrameworkServices.SimpleService import SimpleService

ORDER = [
  'metadata',
  'locks',
  'ost'
]

CHARTS = {
  'metadata': {
    'options': [None, 'Lustre Metadata', 'samples/s', 'Metadata Ops', 'mdt_metadata', 'line'],
    'lines': [
  	  ['open', 'open', 'incremental'],
      ['close', 'close', 'incremental'],
      ['mknod', 'mknod', 'incremental'],
      ['link', 'link', 'incremental'],
      ['unlink', 'unlink', 'incremental'],
      ['mkdir', 'mkdir', 'incremental'],
      ['rmdir', 'rmdir', 'incremental'],
      ['rename', 'rename', 'incremental'],
      ['getattr', 'getattr', 'incremental'],
      ['setattr', 'setattr', 'incremental'],
      ['getxattr', 'getxattr', 'incremental'],
      ['setxattr', 'setxattr', 'incremental'],
      ['statfs', 'statfs', 'incremental'],
      ['sync', 'sync', 'incremental'],
      ['samedir_rename', 'samedir_rename', 'incremental'],
      ['crossdir_rename', 'crossfir_rename', 'incremental'],
    ]
  },
  'locks': {
    'options': [None, 'MDT Locks', 'current', 'Lock Count', 'mdt_locks', 'line'],
    'lines': [
      ['locks', 'locks', 'absolute']
    ]
  },
  'ost': {
    'options': [None, 'OST Template', 'none', 'none', 'ost_none', 'line'],
    'lines': []
  }
}

class Service(SimpleService):
  def __init__(self, configuration=None, name=None):
    SimpleService.__init__(self, configuration=configuration, name=name)
    self.order = ORDER
    self.definitions = CHARTS		

  @staticmethod
  def check():
    return True

  def get_data(self):
    data = dict()
    data.update(_get_md_stats())
    data.update(_get_md_locks())
    data.update(self.get_ost_data())
    return data

  def get_ost_data(self):
    data = dict()

    obdfilter_targets = _get_obdfilter_targets()
    for target in obdfilter_targets:
      if target not in self.charts['ost']:
        self.add_ost_dimension(target)
        self.create_new_ost_chart(target)

      data.update(_get_obdfilter_stats(target))
    
    return data

  def add_ost_dimension(self, name):
    self.charts['ost'].add_dimension(['{0}'.format(name), name, 'incremental'])

  def create_new_ost_chart(self, name):
    self.add_new_charts(ost_chart_template, name)

  def add_new_charts(self, template, *params):
    order, charts = template(*params)    
	 
    for chart_name in order: 
      params = [chart_name] + charts[chart_name]['options']
      dimensions = charts[chart_name]['lines']

      new_chart = self.charts.add_chart(params)
      for dimension in dimensions:
        new_chart.add_dimension(dimension)

def _get_md_stats():
  fields = ['snapshot_time',
    'open',
    'close',
    'mknod',
    'link',
    'unlink',
    'mkdir',
    'rmdir',
    'rename',
    'getattr',
    'setattr',
    'getxattr',
    'setxattr',
    'statfs',
    'sync',
    'samedir_rename',
    'crossdir_rename'
  ]
  data = dict()
  md_stats = subprocess.check_output(["lctl", "get_param", "-n", "mdt.*.md_stats"], universal_newlines=True)
  for line in md_stats.splitlines():
   l = line.split()
   data[l[0]] = l[1]

  for f in fields:
    if f not in data:
      data[f] = 0
  return data

def _get_md_locks():
  data = dict()
  md_locks = subprocess.check_output(['lctl', 'get_param', '-n', 'ldlm.namespaces.mdt-*.lock_count'], universal_newlines=True)
  data['locks'] = md_locks

  return data

def _get_obdfilter_targets():
  obdfilter_targets = []
  targets = subprocess.check_output(['lctl', 'list_param', 'obdfilter.*'], universal_newlines=True)
  for t in targets.splitlines():
    obdfilter_targets.append(t.split(".")[1])

  return obdfilter_targets

def _get_obdfilter_stats(target):
  fields = [
    'write_bytes_{0}'.format(target),
    'write_calls_{0}'.format(target),
    'read_bytes_{0}'.format(target),
    'read_calls_{0}'.format(target),
    'cache_hit_{0}'.format(target),
    'cache_miss_{0}'.format(target),
    'cache_access_{0}'.format(target)
  ]

  data = dict()
  obdfilter_stats = subprocess.check_output(['lctl', 'get_param', '-n', 'obdfilter.{0}.stats'.format(target)], universal_newlines=True)
  for line in obdfilter_stats.splitlines():
    l = line.split()
    data['{0}_{1}'.format(l[0], target)] = l[1]
  
  for f in fields:
    if f not in data:
      data[f] = 0

  return data

def ost_chart_template(name):
  order = [
    '{0}'.format(name)
  ]
  family = 'OST {0}'.format(name)

  charts = {
    order[0]: {
      'options': [None, 'Lustre OST Stats', 'samples/s', 'OST Stats {0}'.format(name), 'ost.stats', 'line'],
      'lines': [ 
        ['write_bytes_{0}'.format(name), 'open', 'incremental'],
        ['write_calls_{0}'.format(name), 'close', 'incremental'],
        ['read_bytes_{0}'.format(name), 'mknod', 'incremental'],
        ['read_calls_{0}'.format(name), 'link', 'incremental'],
        ['cache_hit_{0}'.format(name), 'unlink', 'incremental'],
        ['cache_miss_{0}'.format(name), 'mkdir', 'incremental'],
        ['cache_access_{0}'.format(name), 'rmdir', 'incremental']
      ]
    }
  }

  return order, charts


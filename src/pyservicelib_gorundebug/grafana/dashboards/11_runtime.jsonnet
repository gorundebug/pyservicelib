// Dashboard: Python Runtime & Process
//
// Source: prometheus_client collectors (ProcessCollector, GCCollector, PlatformCollector):
//   process_cpu_seconds_total           counter
//   process_resident_memory_bytes       gauge
//   process_virtual_memory_bytes        gauge
//   process_open_fds                    gauge
//   process_max_fds                     gauge
//   process_start_time_seconds          gauge
//   python_gc_collections_total{generation}  counter
//   python_gc_objects_collected_total{generation}  counter
//   python_gc_objects_uncollectable_total{generation}  counter
//   python_info{implementation, major, minor, patchlevel, version}  gauge

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local jobFilter = 'job=~"$job"';

lib.dashboard(
  title='%s / Python Runtime & Process' % lib.svc,
  uid='%s-runtime' % lib.svc,
  tags=['runtime', 'python'],
  variables=[
    lib.dsVar,
    lib.labelVar('job', 'job', 'process_start_time_seconds'),
  ],
  panels=[
    // -------------------------------------------------------------------------
    // Row: Process
    // -------------------------------------------------------------------------
    lib.row('Process'),

    lib.ts(
      title='CPU Usage',
      targets=[
        lib.rate('process_cpu_seconds_total', jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='s/s',
    ),

    lib.ts(
      title='Resident Memory (RSS)',
      targets=[
        lib.promQ('process_resident_memory_bytes{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='bytes',
    ),

    lib.ts(
      title='Virtual Memory',
      targets=[
        lib.promQ('process_virtual_memory_bytes{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='bytes',
    ),

    lib.ts(
      title='Open File Descriptors',
      targets=[
        lib.promQ('process_open_fds{%s}' % jobFilter, '{{job}}'),
        lib.promQ('process_max_fds{%s}' % jobFilter,  'max {{job}}'),
      ],
      w=12, h=8,
      unit='short',
    ),

    lib.stat(
      title='Process Start Time',
      targets=[
        lib.promQ('process_start_time_seconds{%s} * 1000' % jobFilter, '{{job}}'),
      ],
      w=12, h=4,
      unit='dateTimeAsLocal',
      reduceCalc='lastNotNull',
    ),

    // -------------------------------------------------------------------------
    // Row: Python GC
    // -------------------------------------------------------------------------
    lib.row('Python Garbage Collector'),

    lib.ts(
      title='GC Collections per Second',
      targets=[
        lib.rate(
          'python_gc_collections_total',
          '%s, generation=~".*"' % jobFilter,
          'gen{{generation}} {{job}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Objects Collected per Second',
      targets=[
        lib.rate(
          'python_gc_objects_collected_total',
          '%s, generation=~".*"' % jobFilter,
          'gen{{generation}} {{job}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Uncollectable Objects per Second',
      targets=[
        lib.rate(
          'python_gc_objects_uncollectable_total',
          '%s, generation=~".*"' % jobFilter,
          'gen{{generation}} {{job}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    // -------------------------------------------------------------------------
    // Row: Python Info
    // -------------------------------------------------------------------------
    lib.row('Python Info'),

    lib.stat(
      title='Python Version',
      targets=[
        lib.promQ(
          'python_info{%s}' % jobFilter,
          '{{version}} {{implementation}}'
        ),
      ],
      w=12, h=4,
      unit='short',
      reduceCalc='lastNotNull',
    ),
  ]
)

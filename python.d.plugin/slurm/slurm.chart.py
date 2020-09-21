import subprocess
from bases.FrameworkServices.SimpleService import SimpleService
import re

ORDER = ['sdiag_server',
        'sdiag_jobs',
        'sdiag_main_sched_stats',
        'sdiag_backfilling_stats',
        'sdiag_rpc_call_statistics']

CHARTS = {
    'sdiag_server': {
        'options': [None,
                'SDIAG Server Statistics',
                'total',
                'SDIAG Server Stats',
                'sdiag_server_stats',
                'line'],
        'lines': [
            ['agent_count', 'Agent Count', 'absolute'],
            ['agent_queue_size', 'Agent Queue Size', 'absolute'],
            ['agent_thread_count', 'Agent Thread Count', 'absolute'],
            ['server_thread_count', 'Server Thread Count', 'absolute'],
            ['dbd_agent_queue_size', 'DBD Agent Queue Size', 'absolute']
        ]
    },

    'sdiag_jobs': {
        'options': [None,
                'SDIAG Job Statistics',
                'total',
                'SDIAG Job Stats',
                'sdiag_job_stats',
                'line'],
        'lines': [
            ['jobs_submitted', 'Jobs Submitted', 'absolute'],
            ['jobs_started', 'Jobs Started', 'absolute'],
            ['jobs_completed', 'Jobs Completed', 'absolute'],
            ['jobs_canceled', 'Jobs Canceled', 'absolute'],
            ['jobs_failed', 'Jobs Failed', 'absolute'],
            ['jobs_pending', 'Jobs Pending', 'absolute'],
            ['jobs_running', 'Jobs Running', 'absolute']
        ]
    },

    'sdiag_main_sched_stats': {
        'options': [None,
                'SDIAG Main Schedule Statistics',
                'total',
                'SDIAG Main Schedule Stats',
                'sdiag_main_sched_stats',
                'line'],
        'lines': [
            ['main_last_cycle', 'Last Cycle', 'absolute'],
            ['main_max_cycle', 'Max Cycle', 'absolute'],
            ['main_total_cycles', 'Total Cycles', 'absolute'],
            ['main_mean_cycle', 'Mean Cycle', 'absolute'],
            ['main_mean_depth_cycle', 'Mean Depth Cycle', 'absolute'],
            ['main_cycles_per_minute', 'Cycles Per Minute', 'absolute'],
            ['main_last_queue_length', 'Last Queue Length', 'absolute']
        ]
    },

    'sdiag_backfilling_stats': {
        'options': [None,
                'SDIAG Backfilling Statistics',
                'total',
                'SDIAG Backfilling Stats',
                'sdiag_backfilling_stats',
                'line'],
        'lines': [
            ['backfill_total_backfilled_jobs_since_last_slurm_start', 'Total backfilled jobs (since last slurm start)', 'absolute'],
            ['backfill_total_backfilled_jobs_since_last_stats_cycle_start', 'Total backfilled jobs (since last stats cycle start)', 'absolute'],
            ['backfill_total_backfilled_heterogeneous_job_components', 'Total backfilled heterogeneous job components', 'absolute'],
            ['backfill_total_cycles', 'Total cycles', 'absolute'],
            ['backfill_last_cycle', 'Last cycle', 'absolute'],
            ['backfill_max_cycle', 'Max cycle', 'absolute'],
            ['backfill_last_depth_cycle', 'Last depth cycle', 'absolute'],
            ['backfill_last_depth_cycle_try_sched', 'Last depth cycle (try sched)', 'absolute'],
            ['backfill_last_queue_length', 'Last queue length', 'absolute'],
            ['backfill_last_table_size', 'Last table size', 'absolute']
        ]
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
        data = self.get_sdiag()
        return data

    def get_sdiag(self):
        sdiag_data = dict()
        sdiag_raw = subprocess.check_output(['sdiag'], universal_newlines=True)
        prepend = ""
        for line in sdiag_raw.splitlines():
            line = line.lstrip()
            if line == "Backfilling stats":
                prepend = "backfill_"
            elif line == "Main schedule statistics (microseconds):":
                prepend = "main_"
            elif line == "Remote Procedure Call statistics by message type":
                prepend = "rpc_"
            elif line == "Remote Procedure Call statistics by user":
                prepend = "user_rpc_"
            match = re.match('^(?P<field>[^*]+):[\s]+(?P<value>[\d]+)$',line)
            if match:
                field = "{0}{1}".format(prepend, match.group("field").lower())
                field = re.sub("\(", "", field)
                field = re.sub("\)", "", field)
                field = re.sub(" ", "_", field)
                sdiag_data[field] = match.group("value")
                continue
            match = re.match('^(?P<field>[\S]+)\s*\(\s*(?P<value>[\d]+)\) count:(?P<count>[\d]+)\s*ave_time:(?P<ave_time>[\d]+)\s*total_time:(?P<total_time>[\d]+)$', line)
            if match:
                field = "{0}{1}".format(prepend, match.group("field").lower())
                sdiag_data[field] = {"count": match.group("count"),
                                        "ave_time": match.group("ave_time"),
                                        "total_time": match.group("total_time")}
        return sdiag_data
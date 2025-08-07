from slop.slurm.state import *

def get_display_attr(job):
    if job.is_array_parent:
       return {
           'job_id': ('weight', 5, None),
           'array_tasks': ('weight', 4, None),
           'submit_time': ('weight', 4, None),
           'account': ('weight', 3, 'clip'),
           'partition': ('weight', 4, None),
           'name': ('weight', 5, 'clip'),
       }
    elif job.is_array_child:
       return {
           'job_id': ('weight', 3, None),
           'start_time': ('weight', 4, None),
           'end_time': ('weight', 3, None),
           'nodes': ('weight', 3, None),
           'tres': ('weight', 5, None),
       }

    elif is_running(job) or job.has_running_children:
       return {
           'job_id': ('weight', 5, None),
           'start_time': ('weight', 4, None),
           'end_time': ('weight', 3, None),
           'account': ('weight', 3, 'clip'),
           'partition': ('weight', 4, None),
           'name': ('weight', 5, 'clip'),
           'nodes': ('weight', 5, None),
       }
    elif is_ended(job):
        return {
            'job_state': ('weight', 3, None),
            'job_id': ('weight', 4, None),
            'wall_time': ('weight', 4, None),
            'account': ('weight', 4, None),
            'partition': ('weight', 4, None),
            'name': ('weight', 5, 'ellipsis'),
            'exit_code': ('weight', 8, 'ellipsis'),
       }
    elif is_pending(job):
        return {
            'job_id': ('weight', 3, None),
            'submit_time': ('weight', 3, None),
            'wall_time': ('weight', 2, None),
            'account': ('weight', 3, None),
            'partition': ('weight', 3, 'ellipsis'),
            'name': ('weight', 5, 'ellipsis'),
            'reason': ('weight', 5, 'ellipsis'),
       }
    else:
        return {
            'job_state': ('given', 3, None),
            'job_id': ('weight', 3, None),
            'submit_time': ('weight', 3, None),
            'wall_time': ('weight', 3, None),
            'account': ('weight', 3, None),
            'partition': ('weight', 3, None),
            'name': ('weight', 5, 'ellipsis'),
            'reason': ('weight', 5, 'ellipsis'),
       }

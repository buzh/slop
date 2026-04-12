from slop.slurm.state import *

def get_display_attr(job, width=None, view_type=None):
    """Get display attributes for a job based on its state, available width, and view context.

    Args:
        job: The job object
        width: Available width in columns (None = use default/full)
        view_type: Which view is displaying this job ('users', 'accounts', 'partitions', 'states')

    Returns:
        dict: Column configuration {field: (sizing, weight, wrap_mode)}
    """
    # Determine width category
    if width is None:
        size = 'wide'
    elif width < 90:
        size = 'narrow'
    elif width < 130:
        size = 'medium'
    else:
        size = 'wide'

    # Determine if we should show user_name (when not in users view)
    show_user = view_type and view_type != 'users'

    if job.is_array_parent:
        if size == 'narrow':
            if show_user:
                return {
                    'job_id': ('weight', 5, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'array_tasks': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
            else:
                return {
                    'job_id': ('weight', 5, 'clip'),
                    'array_tasks': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
        elif size == 'medium':
            if show_user:
                return {
                    'job_id': ('weight', 5, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'array_tasks': ('weight', 4, 'clip'),
                    'submit_time': ('weight', 4, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
            else:
                return {
                    'job_id': ('weight', 5, 'clip'),
                    'array_tasks': ('weight', 4, 'clip'),
                    'submit_time': ('weight', 4, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
        else:  # wide
            if show_user:
                return {
                    'job_id': ('weight', 5, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'array_tasks': ('weight', 4, 'clip'),
                    'submit_time': ('weight', 4, 'clip'),
                    'account': ('weight', 3, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
            else:
                return {
                    'job_id': ('weight', 5, 'clip'),
                    'array_tasks': ('weight', 4, 'clip'),
                    'submit_time': ('weight', 4, 'clip'),
                    'account': ('weight', 3, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }

    elif job.is_array_child:
        if size == 'narrow':
            return {
                'job_id': ('weight', 3, 'clip'),
                'start_time': ('weight', 4, 'clip'),
                'nodes': ('weight', 3, 'clip'),
            }
        else:
            return {
                'job_id': ('weight', 3, 'clip'),
                'start_time': ('weight', 4, 'clip'),
                'end_time': ('weight', 3, 'clip'),
                'nodes': ('weight', 3, 'clip'),
                'tres': ('weight', 5, 'clip'),
            }

    elif is_running(job) or job.has_running_children:
        if size == 'narrow':
            if show_user:
                return {
                    'job_id': ('weight', 4, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'start_time': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
            else:
                return {
                    'job_id': ('weight', 4, 'clip'),
                    'start_time': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
        elif size == 'medium':
            if show_user:
                return {
                    'job_id': ('weight', 5, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'start_time': ('weight', 4, 'clip'),
                    'end_time': ('weight', 3, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
            else:
                return {
                    'job_id': ('weight', 5, 'clip'),
                    'start_time': ('weight', 4, 'clip'),
                    'end_time': ('weight', 3, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
        else:  # wide
            if show_user:
                return {
                    'job_id': ('weight', 5, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'start_time': ('weight', 4, 'clip'),
                    'end_time': ('weight', 3, 'clip'),
                    'account': ('weight', 3, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'nodes': ('weight', 5, 'clip'),
                }
            else:
                return {
                    'job_id': ('weight', 5, 'clip'),
                    'start_time': ('weight', 4, 'clip'),
                    'end_time': ('weight', 3, 'clip'),
                    'account': ('weight', 3, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'nodes': ('weight', 5, 'clip'),
                }

    elif is_ended(job):
        if size == 'narrow':
            if show_user:
                return {
                    'job_state': ('weight', 2, 'clip'),
                    'job_id': ('weight', 4, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'exit_code': ('weight', 6, 'clip'),
                }
            else:
                return {
                    'job_state': ('weight', 2, 'clip'),
                    'job_id': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'exit_code': ('weight', 6, 'clip'),
                }
        elif size == 'medium':
            if show_user:
                return {
                    'job_state': ('weight', 3, 'clip'),
                    'job_id': ('weight', 4, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 4, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'exit_code': ('weight', 6, 'clip'),
                }
            else:
                return {
                    'job_state': ('weight', 3, 'clip'),
                    'job_id': ('weight', 4, 'clip'),
                    'wall_time': ('weight', 4, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'exit_code': ('weight', 6, 'clip'),
                }
        else:  # wide
            if show_user:
                return {
                    'job_state': ('weight', 3, 'clip'),
                    'job_id': ('weight', 4, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 4, 'clip'),
                    'account': ('weight', 4, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'exit_code': ('weight', 8, 'clip'),
                }
            else:
                return {
                    'job_state': ('weight', 3, 'clip'),
                    'job_id': ('weight', 4, 'clip'),
                    'wall_time': ('weight', 4, 'clip'),
                    'account': ('weight', 4, 'clip'),
                    'partition': ('weight', 4, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'exit_code': ('weight', 8, 'clip'),
                }

    elif is_pending(job):
        if size == 'narrow':
            if show_user:
                return {
                    'job_id': ('weight', 3, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 2, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'reason': ('weight', 4, 'ellipsis'),
                }
            else:
                return {
                    'job_id': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 2, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'reason': ('weight', 4, 'ellipsis'),
                }
        elif size == 'medium':
            if show_user:
                return {
                    'job_id': ('weight', 3, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'submit_time': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 2, 'clip'),
                    'partition': ('weight', 3, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'reason': ('weight', 4, 'ellipsis'),
                }
            else:
                return {
                    'job_id': ('weight', 3, 'clip'),
                    'submit_time': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 2, 'clip'),
                    'partition': ('weight', 3, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'reason': ('weight', 4, 'ellipsis'),
                }
        else:  # wide
            if show_user:
                return {
                    'job_id': ('weight', 3, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'submit_time': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 2, 'clip'),
                    'account': ('weight', 3, 'clip'),
                    'partition': ('weight', 3, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'reason': ('weight', 5, 'ellipsis'),
                }
            else:
                return {
                    'job_id': ('weight', 3, 'clip'),
                    'submit_time': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 2, 'clip'),
                    'account': ('weight', 3, 'clip'),
                    'partition': ('weight', 3, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'reason': ('weight', 5, 'ellipsis'),
                }

    else:
        if size == 'narrow':
            if show_user:
                return {
                    'job_state': ('given', 3, 'clip'),
                    'job_id': ('weight', 3, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
            else:
                return {
                    'job_state': ('given', 3, 'clip'),
                    'job_id': ('weight', 3, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
        elif size == 'medium':
            if show_user:
                return {
                    'job_state': ('given', 3, 'clip'),
                    'job_id': ('weight', 3, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 3, 'clip'),
                    'partition': ('weight', 3, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
            else:
                return {
                    'job_state': ('given', 3, 'clip'),
                    'job_id': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 3, 'clip'),
                    'partition': ('weight', 3, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                }
        else:  # wide
            if show_user:
                return {
                    'job_state': ('given', 3, 'clip'),
                    'job_id': ('weight', 3, 'clip'),
                    'user_name': ('weight', 3, 'clip'),
                    'submit_time': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 3, 'clip'),
                    'account': ('weight', 3, 'clip'),
                    'partition': ('weight', 3, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'reason': ('weight', 5, 'ellipsis'),
                }
            else:
                return {
                    'job_state': ('given', 3, 'clip'),
                    'job_id': ('weight', 3, 'clip'),
                    'submit_time': ('weight', 3, 'clip'),
                    'wall_time': ('weight', 3, 'clip'),
                    'account': ('weight', 3, 'clip'),
                    'partition': ('weight', 3, 'clip'),
                    'name': ('weight', 5, 'ellipsis'),
                    'reason': ('weight', 5, 'ellipsis'),
                }

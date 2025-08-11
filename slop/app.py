import urwid as u
import asyncio
from slop.models import Job, Jobs
from slop.slurm.fetcher import SlurmJobFetcher
from slop.ui.widgets import *
from slop.ui.views import *
from slop import __version__

class SC(u.WidgetWrap):
    def __init__(self):

        palette = {
            ("bg",               "white",       "black"),
            ("bar",             "black",     "white"),
            ("jobid",          "white",       "black"),
            ("faded",          "light gray",       "black"),
            ("jobid_selected", "black",       "yellow"),
            ("footer",           "white, bold", "dark red"),
            ("header",           "white, bold", "dark blue"),
            ("jobheader",           "white, bold", "dark cyan"),
            ("heading",        "white",        "black"),
            ("buttons",        "yellow",       "black"),
            ("buttons_selected",        "white",       "dark red")
        }

        self.asyncloop = u.AsyncioEventLoop()
        self.loop = u.MainLoop(self, palette, event_loop=self.asyncloop, unhandled_input=unhandled_input)
        self.jobfetcher = SlurmJobFetcher(loop=asyncio.get_event_loop())
        self.jobs = Jobs(self.jobfetcher.fetch_sync())

        self.refreshing = False

        self.header = Header()
        self.confirmexit = ConfirmExit(self)

        self.overlay_showing = 0
        self.current_view = 1 # F1 screen
        self.selected_job = None
        self.selected_user = None

        col_rows = u.raw_display.Screen().get_cols_rows()
        self.height = col_rows[0] - 4

        self.screen_users = ScreenViewUsers(self, self.jobs)
        self.body = u.AttrMap(self.screen_users, 'bg')
        self.frame = u.Frame(header=self.header, body=self.body)
        super().__init__(self.frame)
        self.show_screen_users()
        self.show_splash_screen()

    async def auto_refresh(self, *_args):
        if self.refreshing:
            return
        self.refreshing = True

        await self.jobfetcher.update_once()
        slurm_job_data = await self.jobfetcher.fetch()
        self.jobs.update_slurmdata(slurm_job_data)

        if self.current_view == 1:
            self.screen_users.update()

        self.refreshing = False
        self.loop.set_alarm_in(3, lambda loop, user_data: asyncio.create_task(self.auto_refresh()))


    def show_app_info(self):
        info_text = f"""
        === Slurm Top (a.k.a slop) {__version__} ===
        
        This application is licensed under the GNU General Public License v3.0.
        Copyright (C) 2025 Andreas Skau (andreas@scheen.no)

        https://github.com/buzh/slop
        """

        overlay_widget = GenericOverlayText(self, info_text)
        self.open_overlay(overlay_widget, height=overlay_widget.overlay_height)


    def show_splash_screen(self):
        splash_text = "Welcome to slop\nPlease wait while fetching job data"
        overlay_widget = GenericOverlayText(self, splash_text)
        self.open_overlay(overlay_widget, height=overlay_widget.overlay_height)

        def on_job_update(*_args):
            self.close_overlay()
            u.disconnect_signal(self.jobs, 'jobs_updated', on_job_update)
        u.connect_signal(self.jobs, 'jobs_updated', on_job_update)
        

    def show_screen_users(self):
        body = u.AttrMap(self.screen_users, 'bg')
        self.frame.body = body
        self.screen_users.update()

    def keypress(self, size, key):
        if key == 'q' or key == 'f10':
            self.open_overlay(self.confirmexit, height=3)
            return None
        if key == 'f1':
            self.show_app_info()
            return None
        if key == 'esc':
            if self.overlay_showing:
                self.close_overlay()
                return None

        else:
            return super().keypress(size, key)

    def open_overlay(self, o=None, height=None, width=None):
        self.previous_body = self.frame.body
        if height is None:
            if hasattr(o, "height"):
                height = o.height
            else:
                height = int(self.height * 0.8)

        overlay = u.Overlay(
            u.Filler(o),
            self.frame.body,
            align='center',
            width=('relative', 70),
            valign='middle',
            height=height
            )

        overlay = u.AttrMap(overlay, 'bg')
        self.frame.body = overlay
        self.overlay_showing = 1

    def close_overlay(self):
        self.frame.body = self.previous_body
        self.overlay_showing = 0

    def startloop(self):
        self.loop.set_alarm_in(1, lambda loop, user_data: asyncio.create_task(self.auto_refresh()))
        self.loop.run()

from slop.main import unhandled_input

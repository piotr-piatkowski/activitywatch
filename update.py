#!/usr/bin/env python3
import re
import datetime
import logging
import time
from typing import Pattern, cast, List, Set
from copy import deepcopy
from pprint import pprint

import sys

from aw_client import ActivityWatchClient
from aw_core.models import Event

def setup_logging():

    root_logger = logging.getLogger()
    loglevel = logging.INFO
    root_logger.setLevel(loglevel)

    stdout_log_handler = logging.StreamHandler(sys.stdout)
    log_format = '%(asctime)s %(name)s %(levelname)s %(message)s'
    stdout_log_handler.setFormatter(
        logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")
    )
    stdout_log_handler.setLevel(loglevel)
    root_logger.addHandler(stdout_log_handler)

logger = logging.getLogger("updater")

src_client = ActivityWatchClient("src-client")
dst_client = ActivityWatchClient("dst-client", testing=True)


class BucketData:
    bucket_id: str
    events: list[Event]
    data_pos: int = 0

    def __init__(self, bucket_id: str):
        self.bucket_id = bucket_id
        self.events = {}

    def load(self):
        logger.info(f"Loading bucket {self.bucket_id}")
        events = src_client.get_events(self.bucket_id)
        logger.info(f"Loaded {len(events)} events")
        self.events = sorted(events, key=lambda e: e.timestamp)

    def reset_pos(self):
        self.data_pos = 0

    def get_first_ts(self) -> datetime.datetime | None:
        if not self.events:
            return None
        return self.events[0].timestamp

    def get_total_duration(self) -> datetime.timedelta:
        if not self.events:
            return datetime.timedelta(0)
        return (
            self.events[-1].timestamp
            + self.events[-1].duration
            - self.events[0].timestamp
        )

    def get_by_timestamp(
        self, timestamp: datetime.datetime, use_duration: bool = True
    ) -> Event | None:
        if not self.events:
            return None

        if (
            self.data_pos >= len(self.events)
            or self.events[self.data_pos].timestamp > timestamp
        ):
            self.data_pos = 0

        ev = self.events[self.data_pos]
        while self.data_pos < len(self.events) - 1:
            next_ev = self.events[self.data_pos + 1]
            if next_ev.timestamp >= timestamp:
                break
            ev = next_ev
            self.data_pos += 1

        if use_duration and timestamp > ev.timestamp + ev.duration:
            return None
        return ev


class AllData:
    data: dict[str, BucketData]
    src_client: ActivityWatchClient
    dst_client: ActivityWatchClient

    def __init__(
        self, src_client: ActivityWatchClient, dst_client: ActivityWatchClient
    ):
        self.data = {}
        self.src_client = src_client
        self.dst_client = dst_client

        self.dst_window_bucket = (
            "aw-watcher-window_" + self.dst_client.client_hostname
        )
        self.dst_afk_bucket = (
            "aw-watcher-afk_" + self.dst_client.client_hostname
        )

    def prepare_target(self):
        buckets = self.dst_client.get_buckets()
        for bucket_id, bucket in buckets.items():
            self.dst_client.delete_bucket(bucket_id)
            self.dst_client.create_bucket(
                bucket_id,
                bucket["type"],
            )

    def load(self):
        buckets = self.src_client.get_buckets()
        for bucket_id in buckets.keys():
            if not bucket_id.startswith("aw-watcher-"):
                continue
            bname, host = bucket_id.split("_")
            if host != self.src_client.client_hostname:
                continue
            bname = bname.replace("aw-watcher-", "")
            if bname == 'web-chrome':
                bname = 'web'
            self.data[bname] = BucketData(bucket_id)
            self.data[bname].load()

        assert all(bname in self.data for bname in ('window', 'afk', 'web'))

    def process(self):
        self.load()
        self.prepare_target()

        cur_event = None

        def update_event(app: str, title: str, ts: datetime.datetime):
            nonlocal cur_event

            if cur_event is not None:
                if (
                    app is not None
                    and cur_event.data.get("app") == app
                    and cur_event.data.get("title") == title
                ):
                    cur_event.duration = ts - cur_event.timestamp
                    return
                else:
                    if cur_event.duration.total_seconds() > 0:
                        afk_event = deepcopy(cur_event)
                        afk_event.data["status"] = "not-afk"
                        self.dst_client.insert_event(
                            self.dst_window_bucket, cur_event
                        )
                        self.dst_client.insert_event(
                            self.dst_afk_bucket, afk_event
                        )
                    cur_event = None

            if cur_event is None and app is not None:
                cur_event = Event(
                    timestamp=ts,
                    duration=0,
                    data={
                        "app": app,
                        "title": title,
                    },
                )

        first_ts = min(
            bucket.get_first_ts() for bucket in self.data.values()
        )
        total_duration = max(
            bucket.get_total_duration() for bucket in self.data.values()
            if bucket.get_total_duration() is not None
        )

        last_update = time.time()

        for offset in range(int(total_duration.total_seconds())):
            if time.time() - last_update > 5:
                last_update = time.time()
                progress = int(offset / total_duration.total_seconds() * 100)
                logger.info(
                    f"Processing data, {progress}% done"
                )
            timestamp = first_ts + datetime.timedelta(seconds=offset)
            ev_window = self.data['window'].get_by_timestamp(timestamp)
            ev_web = self.data["web"].get_by_timestamp(
                timestamp, use_duration=False
            )
            ev_afk = self.data['afk'].get_by_timestamp(timestamp)

            is_afk = ev_afk is not None and ev_afk.data.get("status") == "afk"
            if ev_web and ev_web.data.get("audible", False):
                is_afk = False

            if is_afk or ev_window is None:
                update_event(None, None, timestamp)
                continue

            assert ev_window is not None
            app = ev_window.data.get("app")
            title = ev_window.data.get("title")

            if app == "Google-chrome" and ev_web:
                url = ev_web.data.get("url")
                title = ev_web.data.get("title")
                incognito = ev_web.data.get("incognito")
                if incognito:
                    title = "PRIVATE"
                else:
                    m = re.search(r"^https?://([^/]+)", url)
                    if m:
                        app = m.group(1)

            if app == "kanbanflow.com":
                title = re.sub(r'^\d+:\d+\s+', '', title)

            if app == "Code":
                title = re.sub(r'● ', '', title)

            if app in ('localhost:5600', 'localhost:5666'):
                app = "ActivityWatch"

            if app in ("www.messenger.com", "www.facebook.com"):
                title = re.sub(r"\(\d+\)\s+", "", title)

            if app == "app.slack.com":
                title = re.sub(r"\*\s+", "", title)

            update_event(app, title, timestamp)

setup_logging()
ddata = AllData(src_client, dst_client)
ddata.process()

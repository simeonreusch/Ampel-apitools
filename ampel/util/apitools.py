#!/usr/bin/env python3
# Author: Simeon Reusch (simeon.reusch@desy.de)
# License: BSD-3-Clause

import io, os, logging, time, json
import requests
import backoff
from appdirs import AppDirs
import numpy as np
import pandas as pd
from astropy.time import Time
from tqdm import tqdm
from ampel.ztf.t0.load.ZTFArchiveAlertLoader import ZTFArchiveAlertLoader

auth_token = os.environ["AMPEL_API_ARCHIVE_TOKEN_PASSWORD"]
endpoint_query = "https://ampel.zeuthen.desy.de/api/ztf/archive/v3/streams/from_query"
endpoint_stream = "https://ampel.zeuthen.desy.de/api/ztf/archive/v3"
filternames = {1: "g", 2: "r", 3: "i"}

logger = logging.getLogger(__name__)


class Stream(object):
    """Initiate and run stream query"""

    def __init__(self):
        super(Stream, self).__init__()
        self.create_dir_for_token()

    def create_dir_for_token(self):
        """
        Create cache dir for token
        """

        cache_dir = AppDirs("ampel-apitools").user_cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        self.cache_dir = cache_dir

        return cache_dir

    def create_stream_from_objectIds(
        self,
        token: str = auth_token,
        objectIds: list = None,
        candidate_dict: dict = {},
    ) -> None:
        """
        Initiate a epoch based stream query
        """
        logger.debug(f"Creating a stream from objecIds: {objectIds}")

        query = {
            "objectId": objectIds,
            "candidate": candidate_dict,
        }
        logger.debug(f"Query: {query}")

        self.generic_stream(query=query)

    def create_stream_from_epoch(
        self,
        token: str = auth_token,
        date_start: str = "2014-10-10",
        date_end: str = "2022-06-28",
        candidate_dict: dict = {},
    ) -> str:
        """
        Initiate a epoch based stream query
        """
        t_min_jd = Time(date_start).jd
        t_max_jd = Time(date_end).jd

        logger.debug(
            f"Generating query from {Time(t_min_jd, format='jd').iso} to {Time(t_max_jd, format='jd').iso}"
        )

        query = {
            "jd": {
                "$gt": t_min_jd,
                "$lt": t_max_jd,
            },
            "candidate": candidate_dict,
        }

        logger.debug(f"Query: {query}")

        self.generic_stream(query=query)

    def generic_stream(self, query: dict):

        header = {"Authorization": "bearer " + auth_token}

        response = requests.post(url=endpoint_query, json=query, headers=header)

        if not response.ok:
            logger.warn(f"Accessing stream not successful. Response: {response.json()}")
            raise ValueError(f"{response.json()['detail'][0]['msg']}")

        else:
            resume_token = response.json()["resume_token"]

        logger.info("Stream initiated.")
        logger.info(f"Your token: {resume_token}")

        self.resume_token = resume_token

        with open(os.path.join(self.cache_dir, "resume_token.json"), "w") as outfile:
            json.dump({"resume_token": resume_token}, outfile)

        logger.debug(f"Written resume token to {self.cache_dir}")

        return resume_token

    @backoff.on_exception(
        backoff.expo,
        requests.HTTPError,
        giveup=lambda e: e.response.status_code not in {423},
        max_time=3600,
    )
    def access_stream(self, resume_token: str = None) -> list:
        """
        Access the stream for a given resume_token
        """
        if not resume_token:
            resume_token = self.resume_token

        alert_loader = ZTFArchiveAlertLoader(
            archive=endpoint_stream,
            stream=resume_token,
        )
        alertlist = []
        alerts = alert_loader.get_alerts()

        for alert in alerts:
            alertlist.append(alert)

        self.alert_list = alertlist

        return alertlist

    def merge_alerts(self, alert_list: list = None) -> list:
        """
        Generate one unified alert for each objectId
        """
        if not alert_list:
            alert_list = self.alert_list

        merged_list = []
        keys = list(set([x["objectId"] for x in alert_list]))

        for objectid in keys:
            alerts = [x for x in alert_list if x["objectId"] == objectid]
            if len(alerts) == 1:
                merged_list.append(alerts[0])
            else:
                jds = [x["candidate"]["jd"] for x in alerts]
                order = [jds.index(x) for x in sorted(jds)[::-1]]
                latest = alerts[jds.index(max(jds))]
                latest["candidate"]["jdstarthist"] = min(
                    [x["candidate"]["jdstarthist"] for x in alerts]
                )

                for index in order[1:]:

                    x = alerts[index]

                    # Merge previous detections

                    for prv in x["prv_candidates"] + [x["candidate"]]:
                        if prv not in latest["prv_candidates"]:
                            latest["prv_candidates"] = [prv] + latest["prv_candidates"]

                merged_list.append(latest)
        self.merged_list = merged_list
        return merged_list

    def get_info_from_alerts(
        self, merged_list: list = None, key_list: list = ["ra", "dec", "distnr"]
    ) -> list:
        """
        Get data from the alerts.
        """
        if not merged_list:
            merged_list = self.merged_list

        data = {}

        for transient in merged_list:
            _returndict = {}
            objectId = transient["objectId"]
            alert_content = transient["candidate"]
            prv = transient["prv_candidates"]
            prv.append(alert_content)
            detections = []
            for a in prv:
                if "magpsf" in a.keys():
                    detections.append(a)

            for key in key_list:
                list_for_mean = []
                for d in detections:
                    if key in d.keys():
                        list_for_mean.append(d[key])
                if list_for_mean:
                    _returndict.update({key: np.mean(list_for_mean)})
                else:
                    _returndict.update({key: None})

            for f in range(1, 4):

                magpsf = []
                jd = []
                for d in detections:
                    if d["fid"] == f:
                        magpsf.append(d["magpsf"])
                        jd.append(d["jd"])
                if len(magpsf) > 0:
                    i = np.argmin(magpsf)
                    _returndict.update(
                        {f"peak_mjd_{filternames[f]}": jd[i] - 2400000.5}
                    )
                else:
                    _returndict.update({f"peak_mjd_{filternames[f]}": None})

            data.update({objectId: _returndict})

        data = pd.DataFrame.from_dict(data, orient="index")

        data.sort_index(inplace=True)

        self.data = data
        return data

import argparse
import logging
import time
import requests
import datetime
from pathlib import Path
import schedule
import signal
import sys


class Crawler:

    log = None
    api_url = "https://api.deutschebahn.com/flinkster-api-ng/v1/"

    def __init__(self, args: object) -> object:
        self.log = logging.getLogger('crawler')
        self.log.setLevel(logging.INFO)

        # console handler
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        default_formatter = logging.Formatter('[%(asctime)s] %(message)s')
        handler.setFormatter(default_formatter)
        self.log.addHandler(handler)

        if args.verbose:
            self.log.setLevel(logging.DEBUG)
            # increase the verbosity of the formatter
            debug_formatter = logging.Formatter('[%(asctime)s%(msecs)03d] %(levelname)s [%(name)s:%(lineno)s] %(message)s')
            handler.setFormatter(debug_formatter)
        elif args.quiet:
            self.log.setLevel(logging.WARN)

        if args.log_path:
            file_formatter = logging.Formatter('[%(asctime)s%(msecs)03d] %(levelname)s [%(name)s:%(lineno)s] %(message)s')
            fh = logging.FileHandler(args.log_path)
            fh.setFormatter(file_formatter)
            fh.setLevel(logging.DEBUG)
            self.log.addHandler(fh)

        self.log.info("#####################################################")
        self.log.info("# Call a Bike Crawler                               #")
        self.log.info("# Version 1                                         #")
        self.log.info("#####################################################")

        self.headers = {'Authorization': 'Bearer ' + args.token}
        self.lat = '49.8739'
        self.lon = '8.6512'
        self.radius = 10000
        self.limit = 50

        self.start = datetime.datetime.now()
        self.dateformat = "%Y-%m-%d-%H%M%S"
        self.base_path = Path(args.data_path, self.start.strftime(self.dateformat))
        self.log.debug("Data path: %s", self.base_path.absolute())
        self.base_path.mkdir(parents=True, exist_ok=True)

        self.log.debug("Setting up schedule: one request each minute")
        schedule.every(1).minute.do(self.crawl_now)

        self.log.debug("Starting execution")
        while True:
            schedule.run_pending()
            time.sleep(1)

    def crawl_now(self):
        ''' Crawling job which is run every one minute which gets one complete set of all items. '''
        roundstart = datetime.datetime.now()
        self.log.info("Starting one crawl")

        for offset in range(0, 10):
            payload = {
                'lat': self.lat,
                'lon': self.lon,
                'radius': self.radius,
                'offset': offset*50,
                'limit': self.limit,
                'providernetwork': 2,
                'expand': 'rentalobject'

            }
            self.log.debug("Requesting Info Offset: %i" % (offset))
            r = requests.get(self.api_url + "bookingproposals", params=payload, headers=self.headers)
            filepath = self.base_path / Path(roundstart.strftime(self.dateformat) + "-" + str(offset) + ".json")
            self.log.debug("Writing File: %s", filepath.absolute())
            with filepath.open("w", encoding="utf-8") as f:
                f.write(r.text)
            f.close()

        self.log.debug("One crawl done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose",
                        action="store_true", dest="verbose",
                        help="increase output verbosity")
    parser.add_argument('-q', '--quiet',
                        action='store_true', dest="quiet",
                        help="Do not log unless it is critical")
    parser.add_argument("--log-path", type=str,
                        action="store", dest="log_path",
                        help="write log to file")
    parser.add_argument("-d", "--data-path", type=str,
                        action="store", dest="data_path",
                        default=".",
                        help="folder to write responses DEFAULT: current directory")
    parser.add_argument("-t", "--token", type=str,
                        action="store", dest="token",
                        required=True,
                        help="API Authorization Token")

    args = parser.parse_args()

    def signal_handler(sig, frame):
        print('Exiting')
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    Crawler(args)

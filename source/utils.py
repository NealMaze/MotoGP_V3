# -*- coding: utf-8 -*-
import sys
import csv
import logging
import requests
import os
from retry import retry


def is_alive(proxy):
    """Check if a proxy is alive or not
    @proxy - dict
    @return: True if alive, False otherwise
    """

    try:
        requests.get('http://www.google.com',
                     proxies=proxy, timeout=5)
        return True
    except:
        return False


def read_proxies(filename):
    """Read proxies from txt file
    @return: list of dicts
    """
    proxies = []
    try:
        with open(filename, 'r') as inpt:
            for line in inpt:
                if not line.strip():
                    continue
                proxies.append({"http": ''.join(["http://",
                                                 line.strip()]),
                                "https": ''.join(["http://",
                                                 line.strip()])})
    except IOError:
        logging.exception('%s is missing!' % filename)
        sys.exit(1)

    return proxies


@retry(ValueError, tries=2, delay=3, backoff=2)
def download_file(scraper_instance, target_url, output_path, **kwargs):
    """
    Download a file from given URL and saved it into the path
    :param scraper_instance: scraper object with HTTPrequests instance
    :param target_url: string
    :param output_path: string
    :param kwargs: dict with other optional parameters
    :return: str
    """
    request_type = kwargs.pop('request_type', 'GET')
    raw_file = scraper_instance.make_request(
        request_type,
        target_url,
        return_response=True,
        stream=True,
        **kwargs
    )

    with open(output_path, 'wb') as file_:
        for chunk in raw_file:

            if b'<html' in chunk:
                # delete empty file and retry
                os.remove(output_path)

                raise ValueError('download_file[html in file]:{}:{}'.format(target_url, output_path))

            file_.write(chunk)


def write_to_csv(queue, filename, headers, filemode='w'):
    """Continously write to CSV from queue
    @queue - queue with results, dictionaries
    @filename - string
    @headers - list of strings"""

    header_aliases = {h: h for h in headers}
    header_needed = False

    if not os.path.exists(filename) or 'w' in filemode:
        header_needed = True

    with open(filename, filemode) as csvfile:
        writer = csv.writer(csvfile, quotechar='\"',
                            quoting=csv.QUOTE_ALL, delimiter=',')
        if header_needed:
            writer.writerow([header_aliases[key] for key in headers])

        while not queue.empty():
            item = queue.get()

            if not item:
                continue

            try:
                writer.writerow([item.get(key, None) for key in headers])
            except UnicodeDecodeError:
                logging.exception(
                        'write_to_csv[ENCODING ERROR] %s' % str(item))
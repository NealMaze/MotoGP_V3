import os
import json
import logging
import random
import requests
import utils
import ssl
import socket
from requests.exceptions import ConnectionError
from requests.exceptions import Timeout, SSLError, ProxyError
from retry import retry
import time


class Retry(Exception):
    pass


class ChangeProxy(Exception):
    def __init__(self, req_object, message, proxy_type):
        """
        :param req_object: ScraperRequest object
        :param message: string
        :param proxy_type: string
        """
        super().__init__(message)

        if 'remove_proxy' in message:
            remove_proxy = True
        else:
            remove_proxy = False

        if proxy_type == 'static':
            # get a new random IP and use it only for static IPs
            # api proxy will get new IP on simple retry
            if req_object.thread_lock:
                with req_object.thread_lock:
                    req_object.set_proxy(remove_proxy)
            else:
                req_object.set_proxy(remove_proxy)

        raise Retry(message)


class ScraperRequest(object):
    """
        Make HTTP requests using requests library
        """

    def __init__(self, **kwargs):
        """
        Define requests parameters
        :param kwargs: dict of arguments
        """
        self.use_proxy = kwargs.pop('use_proxy')
        self.proxy_type = kwargs.pop('proxy_type', None)  # static or api
        self.thread_lock = kwargs.pop('thread_lock', None)  # lock for multithreading when changing proxies
        self.err_messages = kwargs.pop('err_messages', [])
        # get a list of user agents
        self.user_agents = []

        proxies_fn = kwargs.pop('proxy_fn', 'proxies.txt')
        self.kwargs = kwargs  # keep the remaining arguments

        with open(kwargs.pop('user_agents_fn', 'user_agents.txt'), 'r') as inpt:
            
            for line in inpt:
                if line.strip():
                    self.user_agents.append(line.strip())

        self.headers = {'User-agent': random.choice(self.user_agents)}
        self.session = requests.Session()

        self.current_proxy = None
        if self.use_proxy:

            # get a list of proxies
            self.proxies = utils.read_proxies(proxies_fn)

            # count how many requests are made for each IP
            self.requests_count = 0

            self.set_proxy(self.proxy_type)

        else:
            self.proxies = []

    def set_proxy(self, proxy_type, remove_proxy=False):
        """Get a random proxy from list"""
        if remove_proxy and proxy_type == 'static':
            try:
                self.proxies.remove(self.current_proxy)
            except:
                pass

        if self.proxies:

            if proxy_type == 'api':
                self.current_proxy = self.proxies[0]
                self.session = requests.Session()
                self.headers['User-agent'] = random.choice(self.user_agents)
                self.session.proxies = self.current_proxy

            else:

                while self.proxies:

                    random.shuffle(self.proxies)
                    proxy = random.choice(self.proxies)

                    if not utils.is_alive(proxy):
                        try:
                            self.proxies.remove(proxy)
                        except:
                            pass

                        continue

                    # make sure we get a different IP
                    if proxy == self.current_proxy and len(self.proxies) > 1:
                        continue

                    self.current_proxy = proxy
                    self.session = requests.Session()
                    self.headers['User-agent'] = random.choice(self.user_agents)
                    self.requests_count = 0
                    break

        else:
            logging.exception('EMPTY PROXY LIST')
            os._exit(1)

        if self.current_proxy is None:
            logging.exception("set_proxy[couldn't find a working proxy]")
            os._exit(1)

    @retry(
        (Retry, SSLError, OSError, requests.exceptions.ReadTimeout, ssl.SSLEOFError),
        tries=5, delay=5, backoff=2
    )
    def make_request(self, method, url, **kwargs):
        """
        Make a POST or GET http request
        :param method: string, POST or GET
        :param url: string
        :param kwargs: dict
        :return: string or json
        """
        # use the initial kwargs updated with current kwargs if exists
        kwargs = {**self.kwargs, **kwargs}

        # add default headers
        if 'headers' not in kwargs:
            kwargs['headers'] = self.headers

        # use a different proxy than the default one
        proxy = kwargs.pop('proxies', self.current_proxy)

        session = kwargs.pop('session', self.session)
        if session is None:
            session = requests

        return_json = kwargs.pop('return_json', False)  # load the response as JSON or not
        return_response = kwargs.pop('return_response', False)  # return the response object instead of response string
        min_wait = kwargs.pop('min_wait', None)  # minimum delay between two requests
        max_wait = kwargs.pop('max_wait', None)  # maximum delay between two requests
        connect_timeout = kwargs.pop('connect_timeout', 30)
        read_timeout = kwargs.pop('read_timeout', 45)
        min_requests = kwargs.pop('min_requests', 100)  # minimum number of requests per IP
        max_requests = kwargs.pop('max_requests', 250)  # maximum number of requests per IP

        if max_wait:
            time.sleep(random.randrange(min_wait, max_wait))

        if self.use_proxy:

            with self.thread_lock:

                if self.proxy_type == 'static':

                    if self.requests_count > random.randrange(min_requests, max_requests):
                        self.set_proxy(self.proxy_type)
                    else:
                        self.requests_count += 1

        try:
            if method == 'GET':
                request_method = session.get

            elif method == 'POST':
                request_method = session.post

            elif method == 'PUT':
                request_method = session.put

            else:
                logging.exception('make_requests[wrong_method]:{}'.format(url))
                return

            req = request_method(
                url,
                timeout=(connect_timeout, read_timeout),
                verify=False,
                proxies=proxy,
                **kwargs
            )

            if req.encoding and req.encoding.lower() not in ['utf8', 'utf-8']:
                page = req.content.decode(req.encoding)
            else:
                page = req.text

            if any(error in page for error in self.err_messages) and self.use_proxy:
                raise ChangeProxy(self, '{}:{}:{}'.format(method, url, kwargs), self.proxy_type)

            if req.status_code == 407:
                logging.exception('make_request[Proxy Authentication]')
                os._exit(1)

            if return_json:
                try:
                    return req.json()
                except json.decoder.JSONDecodeError:

                    raise ValueError("make_request[decode json]:{}".format(page))

            if return_response:
                return req

            if kwargs.get('stream', False):
                return req.raw

            if not page:
                raise ChangeProxy(self, '{}:{}:{}'.format(method, url, kwargs), self.proxy_type)

            return page

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403 and self.use_proxy:
                # access forbidden. proxy blocked
                raise ChangeProxy(self, '{}:{}:{}'.format(method, url, kwargs), self.proxy_type)

            elif e.response.status_code == 404:
                logging.exception(' '.join(['make_request[HTTPError]', url, str(e)]))
                return None

            elif e.response.status_code == 429 and self.use_proxy:
                # too many requests. proxy blocked
                raise ChangeProxy(self, '{}:{}:{}'.format(method, url, kwargs), self.proxy_type)

        except (socket.timeout, Timeout, ProxyError, ConnectionError) as e:
            if self.use_proxy:
                raise ChangeProxy(self, '{}:{}:{}:{}'.format(method, url, kwargs, e), self.proxy_type)

            else:
                raise    

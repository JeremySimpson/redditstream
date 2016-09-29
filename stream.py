import logging
import time

import ujson
import pylru
import requests
import requests.auth
import requests.exceptions

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

OAUTH_ACCESS_TOKEN_URL = 'https://www.reddit.com/api/v1/access_token'
STREAM_LIMIT_PER_FETCH = 100
BACKOFF_START = 2.0
BACKOFF_INCREASE_FACTOR = 2
BACKOFF_MAX = 8.0
LRU_CACHE_SIZE = 200


class StreamException(Exception):
    pass


class RedditStream(object):
    def __init__(self, username, password, client_id, client_secret, user_agent, log_verbose=False):
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.log_verbose = log_verbose
        self.access_token = None
        self.expires = -1
        self.latest_before = None

    def _update_access_token(self):
        # Fetch access token
        client_auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        response = requests.post(OAUTH_ACCESS_TOKEN_URL, auth=client_auth, data={
            'grant_type': 'password',
            'username': self.username,
            'password': self.password
        }, headers={
            'User-Agent': self.user_agent
        })

        # Check response
        if not response.ok:
            raise StreamException(
                'Could not retrieve access token: Status {status}'.format(status=response.status_code)
            )

        response_json = response.json()

        # Check for error
        if 'error' in response_json:
            raise StreamException(
                'Could not retrieve access token: Json error: {error}'.format(error=response_json['error'])
            )

        logger.info('Setting access token: ' + ujson.dumps(response_json))
        self.access_token = response_json['access_token']
        self.expires = time.time() + int(response_json['expires_in']) * 0.9

    def _check_token_expiry(self):
        if self.expires == -1 or time.time() > self.expires:
            self._update_access_token()

    def _get_raw_listing(self, url, limit=100, before=None, after=None, session=None):
        # Check if we need to refresh/get the access token
        self._check_token_expiry()

        # Build params
        params = {'limit': limit, 'raw_json': 1, 'show': 'all'}
        if after:
            params['after'] = after
        if before:
            params['before'] = before

        # Perform get request
        # Use the provided session object, if given
        obj = session if session is not None else requests
        response = obj.get(url, params=params, headers={
            'Authorization': 'bearer ' + self.access_token,
            'User-Agent': self.user_agent
        })

        if not response.ok:
            raise StreamException(
                'Could not retrieve page listing: Status {status}'.format(status=response.status_code)
            )

        response_json = response.json()
        response_headers = {
            'Used': response.headers['X-Ratelimit-Used'],
            'Remaining': response.headers['X-Ratelimit-Remaining'],
            'Reset': response.headers['X-Ratelimit-Reset']
        }
        if self.log_verbose:
            logger.info('Response Headers: ' + ujson.dumps(response_headers))
        return response_json, response_headers

    def reset_stream(self):
        self.access_token = None
        self.expires = -1
        self.latest_before = None

    def stream_listing(self, url, concurrency_level=1, extra_delay=0.0):
        """
        Returns a generator representing a listing of the objects at the given url.
        Attempts to return only new objects, from oldest to newest, but there may be duplicates sometimes.
        Follows the api guidelines by monitoring the RateLimit Headers.
        Caches the `before` value, so if the stream needs to be resumed, it is possible.
        This class's functions cannot be run concurrently. One instance per stream please.
        :param url: The url. Must be a listing url.
        :param concurrency_level: The concurrency level. Set to the number of concurrent streams using the same account.
        :param extra_delay: The additional delay to have for each request (in seconds).
        :return: A generator.
        """
        session = requests.Session()
        cache = pylru.lrucache(LRU_CACHE_SIZE)
        backoff = BACKOFF_START
        while True:
            try:
                time_start = time.time()

                # Get data
                try:
                    response_json, response_headers = self._get_raw_listing(
                        url=url, limit=STREAM_LIMIT_PER_FETCH, before=self.latest_before, after=None, session=session
                    )
                except requests.exceptions.RequestException as ex:
                    raise StreamException(ex)

                data = response_json['data']
                children = data['children']
                if self.log_verbose:
                    logger.info('Found entries: ' + str(len(children)))
                if len(children) == STREAM_LIMIT_PER_FETCH:
                    logger.warn(
                        'Fetched max ({c}) elements. Possible stream lag.'.format(
                            c=STREAM_LIMIT_PER_FETCH)
                    )
                # Send off each object to be processed
                # We sort it because we want to process oldest to newest
                sorted_children = sorted(children, key=lambda c: int(c['data']['id'], 36))
                for count, child in enumerate(sorted_children):
                    d = child['data']
                    fullname = d['name']
                    if fullname not in cache:
                        yield d
                    cache[fullname] = 1
                    # Increment the `before` value if we processed the object without problem
                    # Only increment if it isn't the last iteration
                    # This is so that we always try to have at least one object the next time we request something
                    if count != len(children) - 1:
                        self.latest_before = fullname
                # If there were no children in the response, then we have either made a logic mistake, the page is
                # no longer valid (no longer cached), or there really are (currently) no more objects.
                # reset latest_before back to None
                if len(children) == 0:
                    logger.warn(
                        'Fetched zero entries. Possible invalid request. Setting latest_before=None'
                    )
                    self.latest_before = None
                # Calculate sleep time
                # Then sleep for that amount
                rl_reset_time = float(response_headers['Reset'])
                rl_remaining = float(response_headers['Remaining'])
                rl_sleep = rl_reset_time if rl_remaining == 0 else ((rl_reset_time / rl_remaining) * concurrency_level)
                process_time = time.time() - time_start
                t_sleep = max(rl_sleep - process_time, 0) + extra_delay
                if self.log_verbose:
                    logger.info(
                        'Reset_t: {}\tRemaining: {}\tCalc Sleep_t: {}\tProcess_t: {}\tFinal Sleep_t: {}'.format(
                            rl_reset_time, rl_remaining, rl_sleep, process_time, t_sleep)
                    )
                time.sleep(t_sleep)
            except StreamException as ex:
                logger.exception(ex)
                logger.warn(
                    "Caught StreamException - Waiting {backoff} seconds before continuing".format(backoff=backoff)
                )
                time.sleep(backoff)
                backoff = min(backoff * BACKOFF_INCREASE_FACTOR, BACKOFF_MAX)
            else:
                backoff = BACKOFF_START  # Reset backoff back to the starting value

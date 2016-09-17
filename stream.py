import logging
import time

import requests
import requests.auth
import requests.exceptions

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

OAUTH_ACCESS_TOKEN_URL = 'https://www.reddit.com/api/v1/access_token'
STREAM_LIMIT_PER_FETCH = 100


class StreamException(Exception):
    pass


class RedditStream(object):
    def __init__(self, username, password, client_id, client_secret, user_agent, log_verbose=False):
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.access_token = None
        self.expires = -1
        self.log_verbose = log_verbose

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

        logger.info('Setting access token: ' + str(response_json))
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
            logger.info('Response Headers: ' + str(response_headers))
        return response_json, response_headers

    def stream_listing(self, url):
        """
        Returns a generator representing a listing of the objects at the given url.
        Attempts to return only new objects, but this is not guaranteed.
        Follows the api guidelines by monitoring the RateLimit Headers.
        :param url: The url. Must be a listing url.
        :return: A generator.
        """
        before = None
        before_int = 0
        session = requests.Session()
        while True:
            try:
                request_time_start = time.time()
                response_json, response_headers = self._get_raw_listing(url, limit=STREAM_LIMIT_PER_FETCH,
                                                                        before=before, after=None, session=session)
                request_time_end = time.time()
                data = response_json['data']
                children = data['children']
                if len(children) == STREAM_LIMIT_PER_FETCH:
                    logger.warn(
                        'Fetched {c} elements. Possible object loss since previous request.'.format(
                            c=STREAM_LIMIT_PER_FETCH)
                    )
                process_time_start = time.time()
                for child in children:
                    d = child['data']
                    thing_id_int = int(d['id'], 36)
                    if thing_id_int > before_int:
                        before = child['kind'] + '_' + d['id']
                        before_int = thing_id_int
                    yield d
                process_time_end = time.time()
                rl_reset_time = float(response_headers['Reset'])
                rl_remaining = float(response_headers['Remaining'])
                rl_sleep = rl_reset_time if rl_remaining == 0 else (rl_reset_time / rl_remaining)
                process_time = process_time_end - process_time_start
                request_time = request_time_end - request_time_start
                t_sleep = max(rl_sleep - process_time - request_time, 0)
                if self.log_verbose:
                    logger.info(
                        'Reset_t: {}\tRemaining: {}\tCalc Sleep_t: {}\tProcess_t: {}\tRequest_t: {}\tFinal Sleep_t: {}'.format(
                            rl_reset_time, rl_remaining, rl_sleep, process_time, request_time, t_sleep)
                    )
                time.sleep(t_sleep)
            except (StreamException, requests.exceptions.RequestException) as ex:
                logger.error(ex)
                logger.warn("Caught Exception - Waiting 2 seconds before continuing")
                time.sleep(2)

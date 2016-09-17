import logging

from stream import RedditStream

URL_REDDIT_ALL_SUBMISSIONS = 'https://oauth.reddit.com/r/all/new'


def main():
    USERNAME = 'usename_here'
    PASSWORD = 'password_here'
    CLIENT_ID = 'client_id_here'
    CLIENT_SECRET = 'client_secret_here'
    USER_AGENT = 'your_user_agent"

    logging.basicConfig()

    rs = RedditStream(USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET, USER_AGENT)
    for e in rs.stream_listing(URL_REDDIT_ALL_SUBMISSIONS):
        print e


if __name__ == '__main__':
    main()

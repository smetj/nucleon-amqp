import urllib
import urlparse

AMQP_PORT = 5672


def parse_amqp_url(amqp_url):
    '''
    >>> parse_amqp_url('amqp:///')
    ('guest', 'guest', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://a:b@c:1/d')
    ('a', 'b', 'd', 'c', 1)
    >>> parse_amqp_url('amqp://g%20uest:g%20uest@host/vho%20st')
    ('g uest', 'g uest', 'vho st', 'host', 5672)
    >>> parse_amqp_url('http://asd')
    Traceback (most recent call last):
      ...
    AssertionError: Only amqp:// protocol supported.
    >>> parse_amqp_url('amqp://host/%2f')
    ('guest', 'guest', '/', 'host', 5672)
    >>> parse_amqp_url('amqp://host/%2fabc')
    ('guest', 'guest', '/abc', 'host', 5672)
    >>> parse_amqp_url('amqp://host/')
    ('guest', 'guest', '/', 'host', 5672)
    >>> parse_amqp_url('amqp://host')
    ('guest', 'guest', '/', 'host', 5672)
    >>> parse_amqp_url('amqp://user:pass@host:10000/vhost')
    ('user', 'pass', 'vhost', 'host', 10000)
    >>> parse_amqp_url('amqp://user%61:%61pass@ho%61st:10000/v%2fhost')
    ('usera', 'apass', 'v/host', 'hoast', 10000)
    >>> parse_amqp_url('amqp://')
    ('guest', 'guest', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://:@/') # this is a violation, vhost should be=''
    ('', '', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://user@/')
    ('user', 'guest', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://user:@/')
    ('user', '', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://host')
    ('guest', 'guest', '/', 'host', 5672)
    >>> parse_amqp_url('amqp:///vhost')
    ('guest', 'guest', 'vhost', 'localhost', 5672)
    >>> parse_amqp_url('amqp://host/')
    ('guest', 'guest', '/', 'host', 5672)
    >>> parse_amqp_url('amqp://host/%2f%2f')
    ('guest', 'guest', '//', 'host', 5672)
    >>> parse_amqp_url('amqp://[::1]')
    ('guest', 'guest', '/', '::1', 5672)
    '''
    assert amqp_url.startswith('amqp://'), "Only amqp:// protocol supported."
    # urlsplit doesn't know how to parse query when scheme is amqp,
    # we need to pretend we're http'
    o = urlparse.urlsplit('http://' + amqp_url[len('amqp://'):])
    username = urllib.unquote(o.username) if o.username is not None else 'guest'
    password = urllib.unquote(o.password) if o.password is not None else 'guest'

    path = o.path[1:] if o.path.startswith('/') else o.path
    # We do not support empty vhost case. Empty vhost is treated as
    # '/'. This is mostly for backwards compatibility, and the fact
    # that empty vhost is not very useful.
    vhost = urllib.unquote(path) if path else '/'
    host = urllib.unquote(o.hostname) if o.hostname else 'localhost'
    port = o.port if o.port else AMQP_PORT
    return (username, password, vhost, host, port)


def make_amqp_url(username, password, host, port, vhost):
    """Construct an AMQP URL given connection parameters."""

    if not vhost.startswith('/'):
        vhost = '/' + vhost

    if port != AMQP_PORT:
        port = ':%s' % port
    else:
        port = ''

    return "amqp://{username}:{password}@{host}{port}{vhost}".format(
        username=username,
        password=password,
        host=host,
        port=port,
        vhost=vhost
    )

from .spec import BASIC_PROPS_SET, encode_basic_properties


def encode_message(frame, headers, body, frame_size):
    """Encode message headers and body as a sequence of frames."""
    for f in frame.encode():
        yield f
    props, headers = split_headers(headers, BASIC_PROPS_SET)
    if headers:
        props['headers'] = headers
    yield encode_basic_properties(len(body), props)
    for chunk in encode_body(body, frame_size):
        yield chunk


def split_headers(user_headers, properties_set):
    """Split bitfield properties from named headers."""
    props = {}
    headers = {}
    for key, value in user_headers.iteritems():
        if key in properties_set:
            props[key] = value
        else:
            headers[key] = value
    return props, headers


def encode_body(body, frame_size):
    """Generate a sequence of chunks for body where each chunk is less than frame_size"""
    limit = frame_size - 7 - 1   # spec is broken...
    while body:
        payload, body = body[:limit], body[limit:]
        yield (0x03, payload)


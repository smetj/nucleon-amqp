import struct
import table

from .exceptions import ConnectionError


class DecodeBuffer(object):
    """A string that can be decoded sequentially."""
    __slots__ = 's', 'offset'

    def __init__(self, s):
        self.s = s
        self.offset = 0

    def read(self, fmt):
        out = struct.unpack_from(fmt, self.s, self.offset)
        self.offset += struct.calcsize(fmt)
        return out

    def read_string(self, fmt='!I'):
        size, = self.read(fmt)
        return self.read_bytes(size)

    def read_bytes(self, bytes):
        out = self.s[self.offset:self.offset + bytes]
        self.offset += bytes
        return out

    def read_table(self):
        result, offset = table.decode(self.s, self.offset)
        self.offset = offset
        return result


class BufferedReader(object):
    """A buffer around a reader socket.

    This is principally to allow us to read an exact number of bytes at a time.

    """
    def __init__(self, sock):
        self.sock = sock
        self.buf = ''

    def read(self, size):
        """Read exactly size bytes from self.sock"""
        while True:
            if len(self.buf) >= size:
                out, self.buf = self.buf[:size], self.buf[size:]
                return out

            chunk = self.sock.recv(4096)
            if not chunk:
                # FIXME: get a better error message from somewhere?
                raise ConnectionError("Connection lost")
            self.buf += chunk

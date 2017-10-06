#!/usr/bin/env python

import os
import re
import sys
import json
import keyword

sys.path.append(os.path.join("rabbitmq-codegen"))
from amqp_codegen import AmqpSpec, do_main_dict


AMQP_ACCEPTED_BY_UPDATE_JSON = "amqp-accepted-by-update.json"

BANNED_CLASSES = []  # 'access', 'tx'
BANNED_FIELDS = {
    'ticket': 0,
    'nowait': 0,
    'capabilities': '',
    'insist': 0,
    'out_of_band': '',
    'known_hosts': '',
}

import codegen_helpers

from xml.etree.ElementTree import parse
spec_xml = parse(open('amqp0-9-1.extended.xml'))


def method_doc(cls, method):
    """Find method documentation in the XML spec file."""
    e = spec_xml.find(
        ".//class[@name='%s']/method[@name='%s']" % (cls, method)
    )
    txt = ''
    if e is not None:
        doc = e.find('./doc')
        if doc is not None:
            txt = doc.text.strip()
            txt = re.sub(r'\s+', ' ', txt).strip()
            txt = re.sub(r'^(.*?\.)\s+', r'\1\n\n', txt)
            txt += '\n\n'

        for field in e.findall('./field[doc]'):
            name = pyize(field.get('name'))
            doc = field.find('./doc').text.strip()
            txt += ':param %s: %s\n\n' % (name, re.sub(r'\s+', ' ', doc))
    return txt.rstrip()


def method_responses(cls, method):
    """Find corresponding response methods in the XML spec file."""
    e = spec_xml.find(
        ".//class[@name='%s']/method[@name='%s']" % (cls, method)
    )

    responses = []
    if e is not None:
        if e.get('synchronous') == '1':
            for resp in e.findall('.//response'):
                n = resp.get('name')
                if n:
                    responses.append('%s.%s' % (cls, n))
            if not responses:
                print >>sys.stderr, "Warning: no responses found for sync method %s.%s" % (cls, method)
    return responses


def word_wrap(str, width=79, indent=8):
    """Format a prose string to 79 characters, as befits a docstring."""
    lines = []
    l = ''
    max_w = width - indent
    for w in str.split(' '):
        if '\n' in w:
            newlines = w.split('\n')
            l += ' ' + newlines[0]
            for nl in newlines[1:]:
                lines.append(l)
                l = nl
            continue

        if len(l) + len(w) + 1 > max_w:
            lines.append(l)
            l = ''
        if l:
            l += ' ' + w
        else:
            l += w
    if l:
        lines.append(l)

    return '\n'.join((' ' * indent + l).rstrip() for l in lines)


def pyize(*args):
    """Convert to an identifier in lowercase_with_underscores format"""
    a = ' '.join(args).replace('-', '_').replace(' ', '_')
    if keyword.iskeyword(a):
        a += '_'
    return a


def Pyize(*args):
    """Convert to a Python CamelCase class name"""
    words = []
    for a in args:
        words.extend(a.split('-'))
    a = ''.join([a.title() for a in words]).replace('-', '').replace(' ', '')
    if a in ['SyntaxError', 'NotImplemented']:
        a = 'AMQP' + a
    return a


def PYIZE(*args):
    """Convert to an identifier in UPPERCASE_WITH_UNDERSCORES format"""
    return ' '.join(args).replace('-', '_').replace(' ', '_').upper()


def print_constants(spec):
#    for c in spec.allClasses():
#        for m in c.allMethods():
#            print "%s = 0x%08X  # %i,%i %i" % (
#                m.u,
#                m.method_id,
#                m.klass.index, m.index, m.method_id
#                )
#
#    print
    for c in spec.allClasses():
        if c.fields:
            print "%-24s= 0x%04X" % (
                c.u,
                c.index,)


def print_decode_methods_map(client_methods):
    print "METHODS = {"
    for m in client_methods:
        print "    %-32s%s," % (
            m.u + ':',
            m.decode,
            )
    print "}"
    print


def print_decode_properties_map(props_classes):
    print "PROPS = {"
    for c in props_classes:
        print "    %s: %s, \t# %d" % (
            c.u, c.decode, c.index)
    print "}"
    print


def print_frame_class(m):
    print "class %s(Frame):" % (m.frame,)
    print "    __slots__ = %r" % (tuple(str(f.n) for f in m.arguments),)
    print "    name = '%s'" % (m.klass.name + '.' + m.name)
    print "    METHOD_ID = 0x%08X  # %i,%i %i" % (
        m.method_id,
        m.klass.index, m.index, m.method_id
        )
    if m.hasContent:
        print "    has_content = True"
        print "    class_id = %s" % (m.klass.u,)
    else:
        print "    has_content = False"
    print

#    print "    def __init__(self, %s):" % (', '.join(_default_params(m)),)
#    print "        super(%s, self).__init__(%s)" % (m.frame, ', '.join(tuple(f.n for f in m.arguments)))
#    print
    print "    @staticmethod"
    print "    def decode(buffer):"

    fields = codegen_helpers.UnpackWrapper()
    for i, f in enumerate(m.arguments):
        fields.add(f.n, f.t)

    fields.do_print(' ' * 8, "%s")
    print "        return %s(%s)" % (m.frame, ', '.join([f.n for f in m.arguments]))
    print
    print_encode_method(m)
    print


def print_encode_method(m):
#    print m.encode, ', '.join(_method_params_list(m))
    print "    def encode(self):"
    for f in [f for f in m.arguments if not f.banned and f.t in ['table']]:
        print "        %s_raw = table.encode(self.%s)" % (f.n, f.n)

#    if m.hasContent:
#        print "        props, headers = split_headers(self.user_headers, %s_PROPS_SET)" % (
#            m.klass.name.upper(),)
#        print "        if headers:"
#        print "            props['headers'] = headers"

    fields = codegen_helpers.PackWrapper()
    fields.add('self.METHOD_ID', 'long')
    for f in m.arguments:
        if not f.banned and f.t == 'table':
            fields.add('%s' % f.n, f.t)
        else:
            fields.add('self.%s' % f.n, f.t)
    fields.close()

    print "        yield (0x01,"
    if fields.group_count() > 1:
        print "            ''.join(["
        fields.do_print(' ' * 16, '%s')
        print "            ])"
    else:
        fields.do_print(' ' * 12, '%s')
    print "        )"

#    if not m.hasContent:
#        print "        yield (0x01,"
#        if fields.group_count() > 1:
#            print "            ''.join(["
#            fields.do_print(' ' * 16, '%s')
#            print "            ])"
#        else:
#            fields.do_print(' ' * 12, '%s')
#        print "        )"
#    else:
#        print "        yield (0x01,"
#        print "            ''.join(["
#        fields.do_print(' ' * 16, '%s')
#        print "               ])"
#        print "        )"
#        print "        yield %s(len(body), props)" % (m.klass.encode,)
#        print
#        print "        for chunk in encode_body(body, frame_size):"
#        print "            yield chunk"
#        print


def print_decode_properties(c):
    print "def %s(buffer):" % (c.decode,)
    print "    props = {}"
    print "    flags, = buffer.read('!H')"
    print "    assert (flags & 0x01) == 0"
    for i, f in enumerate(c.fields):
        print "    if (flags & 0x%04x): # 1 << %i" % (1 << (15-i), 15-i)
        fields = codegen_helpers.UnpackWrapper()
        fields.add(f.n, f.t)
        fields.do_print(" "*8, "props['%s']")
    print "    hs = props.pop('headers', {})"
    print "    hs.update(props)"
    print "    return hs"
#    print "    return props"
    print


def get_default_value(f):
    if f.n in ['reply_code']:
        return 200
    elif f.domain in ['short', 'long', 'longlong'] and not f.defaultvalue:
        return 0
    return f.defaultvalue


def _default_params(m):
    for f in m.arguments:
        if f.n in BANNED_FIELDS:
            continue
        yield "%s=%r" % (f.n, get_default_value(f))
    if m.hasContent:
        yield "headers={}"
        yield "body=''"
#        yield "frame_size=None"


def _pass_params(m):
    for f in m.arguments:
        try:
            yield repr(BANNED_FIELDS[f.n])
        except KeyError:
            yield f.n
#    if m.hasContent:
#        yield "headers"
#        yield "payload"
#        yield "frame_size=None"


def _method_params_list(m):
    for f in m.arguments:
        if not f.banned:
            yield f.n
#    if m.hasContent:
#        yield 'user_headers'
#        yield 'body'
#        yield 'frame_size'


def print_writer_class(methods):
    print open('templates/framewriter.py.template').read()

    for m in methods:
        method_name = pyize(m.klass.name + '_' + m.name)
        pass_params = ', '.join(_pass_params(m))
        responses = method_responses(m.klass.name, m.name)
        print

        if responses:
            print "    @syncmethod(%s)" % (', '.join(repr(str(r)) for r in responses))
        print "    def %s(self, %s):" % (method_name, ', '.join(_default_params(m)),)

        docstring = method_doc(m.klass.name, m.name)
        if docstring:
            if not responses:
                docstring = ".. warning:: This is an asynchronous method.\n\n" + docstring
            print '        """%s\n        """' % docstring

        if m.hasContent:
            print "        self._send_message(%s(%s), headers, body)" % (
                m.frame,
                pass_params
            )
        else:
            print "        self._send(%s(%s))" % (
                m.frame,
                pass_params
            )

def print_encode_properties(c):
    print "%s_PROPS_SET = set(("% (c.name.upper(),)
    for f in c.fields:
        print '    "%s", %s # %s' % (f.n, ' '*(16-len(f.n)), f.t)
    print "    ))"
    print
    print "ENCODE_PROPS_%s = {" % (c.name.upper(),)
    for i, f in enumerate(c.fields):
        pn = pyize(f.name)
        print "    '%s': (" % (pn,)
        print "        %i," % (i,)
        print "        0x%04x, # (1 << %i)" % ( 1 << (15-i), 15-i,)

        fields = codegen_helpers.PackWrapper()
        if f.t not in ['table']:
            fields.add("val", f.t)
        else:
            fields.add("table.encode(val)", f.t, nr='%s')
        fields.close()
        if len(fields.fields) > 1:
            print ' '*8 + "lambda val: ''.join(("
            fields.do_print(' '*16, '%s')
            print ' '*8 + ')) ),'
        else:
            print '  '*4 + 'lambda val:',
            fields.do_print('', '%s', comma=False)
            print '        ),'

    print "}"
    print
    print "def %s(body_size, props):" % (c.encode,)
    print "    pieces = ['']*%i" % (len(c.fields),)
    print "    flags = 0"
    print "    enc = ENCODE_PROPS_%s" % (c.name.upper(),)
    print
    print "    for key in %s_PROPS_SET & set(props.iterkeys()):" % \
        (c.name.upper(),)
    print "        i, f, fun = enc[key]"
    print "        flags |= f"
    print "        pieces[i] = fun(props[key])"
    print ""
    print "    return (0x02, ''.join(("
    print "        struct.pack('!HHQH',"
    print "                    %s, 0, body_size, flags)," % (c.u,)
    print "        ''.join(pieces),"
    print "        ))"
    print "        )"


def GetAmqpSpec(spec_path, accepted_by_udate):
    spec = AmqpSpec(spec_path)

    for c in spec.allClasses():
        c.banned = bool(c.name in BANNED_CLASSES)
        c.u = PYIZE('CLASS', c.name)

    spec.classes = filter(lambda c:not c.banned, spec.classes)

    for c in spec.allClasses():
        for m in c.allMethods():
            m.u = PYIZE('METHOD', m.klass.name, m.name)
            m.method_id = m.klass.index << 16 | m.index
            m.decode = pyize('decode', m.klass.name, m.name)
            m.encode = pyize('encode', m.klass.name, m.name)
            m.frame = Pyize('frame', m.klass.name, m.name)

            try:
                m.accepted_by = [str(s) for s in accepted_by_udate[c.name][m.name]]
            except KeyError:
                print >> sys.stderr, " [!] Method %s.%s unknown! Assuming " \
                    "['server', 'client']" % (c.name, m.name)
                m.accepted_by = ['server', 'client']

            for f in m.arguments:
                f.t = spec.resolveDomain(f.domain)
                f.n = pyize(f.name)
                f.banned = bool(f.name in BANNED_FIELDS)

    for c in spec.allClasses():
        if c.fields:
            c.decode = pyize('decode', c.name, 'properties')
            c.encode = pyize('encode', c.name, 'properties')
            for f in c.fields:
                f.t = spec.resolveDomain(f.domain)
                f.n = pyize(f.name)
    return spec


def generate_spec(spec_path):
    accepted_by_udate = json.loads(file(AMQP_ACCEPTED_BY_UPDATE_JSON).read())
    return GetAmqpSpec(spec_path, accepted_by_udate)


SPEC_TEMPLATE = 'templates/spec.py.template'
SPEC_EXCEPTIONS_TEMPLATE = 'templates/spec_exceptions.py.template'


def main(spec_path):
    spec = generate_spec(spec_path)

    print open(SPEC_TEMPLATE).read()

    print "PREAMBLE = 'AMQP\\x00\\x%02x\\x%02x\\x%02x'" % (
        spec.major, spec.minor, spec.revision)
    print
    print_constants(spec)
    print

    props_classes = [c for c in spec.allClasses() if c.fields]

    print
    for m in spec.allMethods():
        print_frame_class(m)
        print

#    client_methods = [m for m in spec.allMethods() if 'client' in m.accepted_by]
#    print
#    for m in client_methods:
#        print_decode_method(m)
#        print

    print
    #print_decode_methods_map(client_methods)
    #print
    for c in props_classes:
        print_decode_properties(c)
        print
    print_decode_properties_map(props_classes)
    print

#    server_methods = [m for m in spec.allMethods() if 'client' not in m.accepted_by]
#    for m in server_methods:
#        print_encode_method(m)
#        print

    for c in props_classes:
        print_encode_properties(c)
        print

    print_writer_class([m for m in spec.allMethods() if 'server' in m.accepted_by])



def spec_exceptions(spec_path):
    spec = generate_spec(spec_path)
    print open(SPEC_EXCEPTIONS_TEMPLATE).read()

    err_constants = [(name, value, klass)
                     for name, value, klass in spec.constants
                     if klass in ('hard-error', 'soft-error')]

    for name, value, klass in err_constants:
            print "class %s(AMQP%s):" % (Pyize(name),
                                         Pyize(klass))
            print "    reply_code = %s" % (value,)
            print
            print
    print "ERRORS = {"
    for name, value, klass in err_constants:
        print "    %i: %s," % (value, Pyize(name))
    print "}"
    print


if __name__ == "__main__":
    do_main_dict({"spec": main, 'spec_exceptions': spec_exceptions})

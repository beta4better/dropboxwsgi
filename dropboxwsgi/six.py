import sys

def b(str_):
    if sys.version_info >= (3,):
        str_ = str_.encode('latin1')
    return str_

def u(str_):
    if sys.version_info < (3,):
        str_ = str_.decode('latin1')
    return str_

def r(str_, enc='latin1'):
    """
    Converts the unicode-aware type in the current language
    to the str type in current language using the specified
    encoding if necessary.
    """
    if sys.version_info < (3,):
        str_ = str_.encode(enc)
    return str_

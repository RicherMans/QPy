import re
from functools import wraps

_QSUBSYNOPSIS = 'qsub [-a date_time] [-A account_string] [-b secs] [-c checkpoint_options]\
[-C directive_prefix] [-cwd] [-clear] [-d path] [-D path] [-e path] [-f] [-F] [-h]\
[-I ] [-j join ] [-k keep ] [-l resource_list ]\
[-m mail_options] [-M user_list] [-n] [-N name] [-o path]\
[-p priority] [-P user] [-q destination] [-r c] [-sync yesno] [-S path_to_shell]\
[-t array_request] [-u user_list]\
[-v variable_list] [-V] [-W additional_attributes] [-x] [-X] [-z] [script]'


smallclustersetting = {
    '-cwd': True,
    '-P': 'cpu.p',
    '-sync': 'y',
    '-S': '/usr/bin/python'
}

bigclustersetting = {
    '-cwd': True,
    '-sync': 'y',
    '-S': '/usr/bin/python'
}

_TEMPLATE = {
    '-sync': 'y',
    '-S': '/usr/bin/python'
}


def _parseSettings(setting):
    return


def validateSettings(mdict):
    '''
    Validates the given dict, if the given arguments are supported by qsub
    '''
    validargs = re.findall('\[(-[a-zA-Z]+)\s?(\w+)?\]', _QSUBSYNOPSIS)
    validdict = dict((x, y) for (x, y) in validargs)
    for key, value in mdict.iteritems():
        if key not in validdict:
            raise ValueError(
                "The key parameter in the settings (%s) is not valid for qsub!" % (key))
        # Check if the valid arguments require some argument after it or not
        if validdict[key]:
            if not isinstance(value, str):
                raise ValueError(
                    "The value for the key (%s) is wrong!" % (key))
        else:
            if not isinstance(value, bool):
                raise ValueError(
                    "The value for the key (%s) needs to be a boolean!" % (key))
    # Check if the required arguments are given
    for key, value in _TEMPLATE.iteritems():
        if key not in mdict:
            raise ValueError(
                "Two arguments are required for QPy to work properly : (%s). I could not find these.", (" ".join(_TEMPLATE.keys)))


def setting(setting):
    '''
    Decorator to use any other different setting than the default one using
    @runluster()
    Usage:

    @setting(mysetting)
    @runcluster(3)
    def add(a,b):
        return a+b

    mysetting needs to be a dict containing the Qsub settings, e.g. {'-o':'out'}

    If the setting has no value e.g. -cwd, please use True as it's dict value
    '''
    appendedSetting = dict(setting.items() + _TEMPLATE.items())
    validateSettings(appendedSetting)

    def decorate(func):
        @wraps(func)
        def wrap(*args):
            kw = {'settings': appendedSetting}
            return func(*args, **kw)
        return wrap
    return decorate


def newsetting(mdict):
    '''
    returns a new settings for qsub.

    mdict is a dictionary containing keys which are the switches for qsub e.g. -S -P ...
    and the values are the corresponding parameters for the switch
    '''
    validateSettings(mdict)  # validate first
    return dict(_TEMPLATE.items() + mdict.items())

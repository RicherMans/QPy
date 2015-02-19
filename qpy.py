import marshal
import subprocess as sub
import os
from inspect import getsource, getargspec
import re
import types
from functools import wraps
from contextlib import contextmanager
import sys
import qsubsettings
from glob import glob

_QSUBCMD = 'qsub'


def _globalimports(func):
    for name, val in func.__globals__.iteritems():
        if isinstance(val, types.ModuleType) and not name.startswith('__'):
            yield val.__name__


def _globalaliasimports(func):
    for name, modtype in func.func_globals.items():
        if isinstance(modtype, types.ModuleType) and not name.startswith('__'):
            yield name

# Procedure needs to be executed in the main file, since the locals are only visible from
# here. We use the localmodules as the real name in the produced python scripts for execution
# e.g. the global imports will be set as: import GLOBAL as LOCAL
# localmodules = [key for key in locals().keys()
# if isinstance(locals()[key], type(sys)) and not key.startswith('__')]

# importedmodules = zip(list(_globalimports()), localmodules)


@contextmanager
def stdout_redirected(to=os.devnull):
    '''
    import os

    with stdout_redirected(to=filename):
        print("from Python")
        os.system("echo non-Python applications are also supported")
    '''
    fd = sys.stdout.fileno()

    # assert that Python and C stdio write using the same file descriptor
    ####assert libc.fileno(ctypes.c_void_p.in_dll(libc, "stdout")) == fd == 1

    def _redirect_stdout(to):
        sys.stdout.close()  # + implicit flush()
        os.dup2(to.fileno(), fd)  # fd writes to 'to' file
        sys.stdout = os.fdopen(fd, 'w')  # Python writes to fd

    with os.fdopen(os.dup(fd), 'w') as old_stdout:
        with open(to, 'w') as file:
            _redirect_stdout(to=file)
        try:
            yield  # allow code to be run with the redirected stdout
        finally:
            _redirect_stdout(to=old_stdout)  # restore stdout.
                                            # buffering and flags such as
                                            # CLOEXEC may be different


def _parseSettings(settings):

    def executableExists(program):
        def is_executeable(fpath):
            return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

        fpath, fname = os.path.split(program)
        if fpath:
            if is_executeable(program):
                return program
        else:
            for path in os.environ["PATH"].split(os.pathsep):
                path = path.strip('"')
                exe_file = os.path.join(path, program)
                if is_executeable(exe_file):
                    return exe_file

    qsubset = [_QSUBCMD]
    # Do a quick qsub call to identify if qsub is installed on the machine
    # TODO: Check if qsub is outside of PATH available
    if not executableExists(qsubset[0]):
        raise OSError(
            "qsub cannot be found on this machine, did you install it?")
    for setting in settings:
        # Explicitly testing for the boolean, if True we just append the key
        # with no value since in cases of e.g. -cwd we don't want to have any
        # value
        if settings[setting] == True:
            qsubset.append(setting)
        else:
            qsubset.append(setting)
            qsubset.append(settings[setting])
    return qsubset


def _getQsubCmd(settings):
    return _parseSettings(settings)


def runcluster(numjobs, settings=qsubsettings.smallclustersetting):
    '''
    The main function of this module. Decorator which helps running a function in parallel
    on the gridengine cluster
    numjobs : The amount of Jobs which will be run for this function
    settings : A dict which contains all qsub commands and parameters.
    Can be extended at will, whereas the keys of this dict are the arguments of qsub e.g.
    {
     -o:outputfile,
     -P:gpu.p,
     ....
    }

    Usage :

    @runcluster(5)
    def add(a,b):
        return a+b

    runs the function add(a,b) on the cluster with 5 spawning Jobs
    '''

    def decorator(func):
        @wraps(func)
        def wrap(*args, **kw):
            try:
                # Check if the @settings decorator did pass different settings
                settings = kw['settings']
            except:
                pass
            qsubcmd = _getQsubCmd(settings)
            return _run_jobs(qsubcmd, numjobs, func, zip(*args))
        return wrap
    return decorator


# class SingleFileModuleFinder(modulefinder.ModuleFinder):

#     def import_hook(self, name, caller, *arg, **kwarg):
#         if caller.__file__ == self.name:
# Only call the parent at the top level.
# return modulefinder.ModuleFinder.import_hook(self, name, caller, *arg,
# **kwarg)

#     def __call__(self, node):
#         self.name = str(node)
#         self.run_script(self.name)


def _getModuleImports(func):
    '''
    Gets from the given function it's modules imports
    returns a list of tuples, where the fist item represents
    the full import name and the second is it's local alias
    e.g.:
    import marshal as mar
    The list would have the values:
    [(marshal','mar')]
    '''
    globalimports = list(_globalimports(func))
    globalaliases = list(_globalaliasimports(func))
    return zip(globalimports, globalaliases)


def _pickleLoadScript(mdict, modules):
    '''
    mdict: Dictionary containing the following keys:
    loaddir: Path to the file which is going to be taken as input
    functiondef :  The full function definition
    functionname: The name of the given function, which will be called
    output: The name of the outputfile which will be generated
    '''
    lines = []
    for globalname, localname in modules:
        lines.append('import {} as {}'.format(globalname, localname))
    lines.append('import marshal')
    lines.append("data = marshal.load(open('%(loaddir)s','rb'))" % (mdict))
    lines.append("%(functiondef)s" % (mdict))
    lines.append("ret=[]")
    lines.append('for arg in data:')
    lines.append('    ret.append(%(functionname)s(*arg))' % (mdict))
    lines.append("marshal.dump(ret,open('%(output)s','wb'))" % (mdict))
    return os.linesep.join(lines)


def _suppressedPopen(args):
    '''
    Same as sub.Popen(args) call but supresses the output
    '''
    with stdout_redirected():
        return sub.Popen(args)


def _run_jobs(qsubcmd, n, func, data):
    datachunks = _splitintochunks(data, n)
    funcret = []
    runningJobs = []

    # Parameters which are related to the function which will be decorated
    rawsource = getsource(func)
    argspec = getargspec(func)
    # Since the source has the decoration header, @runcluster we remove it
    # Remove the lines not starting with @, which indicates a decorator
    filteredlines = re.findall("^(?!@).*", rawsource, re.MULTILINE)
    # source = rawsource[firstline:]
    source = os.linesep.join(filteredlines)
    tmpfiles = []  # Keeps track of all open tempfiles
    try:
        for i, chunk in enumerate(datachunks):
            # Create some tempfiles which will be used as python script and binary
            # dumps respectively, cannot use tempfile since marshal does not allow
            # to use a wrapper as input
            tmpscript = open('{}_run_{}'.format(func.__name__, i + 1), 'w')
            datadump = open('{}_data_{}'.format(func.__name__, i + 1), 'w+b')
            output = open('{}_out_{}'.format(func.__name__, i + 1), 'w+b')
            # output = '{}_out_{}'.format(func.__name__, i + 1)
            # Output needs to be closed separately, since we want to keep the
            # file on the system as long as the qsub command is runnung
            marshal.dump(chunk, datadump)
            mdict = {
                'functiondef': source,
                # The name of the datadump which will be generated using pickle
                'loaddir': datadump.name,
                'functionname': func.func_name,
                'args': argspec.args,
                'output': output.name
            }
            imports = _getModuleImports(func)
            tmpscript.write(_pickleLoadScript(mdict, imports))
            tmpscript.flush()
            # Reset the datadump pointer, otherwise EOFError
            datadump.close()
            cur_qsub = qsubcmd + [tmpscript.name]
            job = _suppressedPopen(cur_qsub)
            tmpfiles.append((tmpscript, datadump, output))
            runningJobs.append(job)
            # execfile(tmpscript.name, dict(), ret)
        for job, tmpfilestuple in zip(runningJobs, tmpfiles):
            # Since we use the -sync flag, we need to wait for the calling command
            # to finish
            retcode = job.wait()
            # If we have any retcode, we keep the log outputs of the gridengine
            # alive
            tmpscript, dump, output = tmpfilestuple
            tmpscript.close()

            if retcode:
                raise ValueError(
                    "An error Occured while running the gridengine, please refer to the logs produced in the calling directory")
            else:  # Otherwise delete the logs of gridengine
                for ftoremove in glob('%s*' % (tmpscript.name)):
                    absremovepath = os.path.join(os.getcwd(), ftoremove)
                    os.remove(absremovepath)
            output.seek(0)
            funcret.extend(marshal.load(output))
            output.close()
            dump.close()
            os.remove(output.name)
            os.remove(dump.name)
    except:
        for f in tmpfiles:
            tmpscript, dump, output = f
            output.close()
            tmpscript.close()
            dump.close()
            os.remove(output.name)
            os.remove(tmpscript.name)
            os.remove(dump.name)

    return funcret


def _splitintochunks(l, num):
    '''
    Splits the given list l into roughly equal num chunks as iterator.
    It calculates the optimal split for the given NUM in relation to the length of the list l
    Note that the returned iterator has not necessary NUM chunks
    '''
    spl, ext = divmod(len(l), num)
    if ext:
        spl += 1
    return (l[i:i + spl] for i in xrange(0, len(l), spl))

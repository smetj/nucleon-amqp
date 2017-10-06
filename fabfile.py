import posixpath
from contextlib import contextmanager
from os.path import dirname, join, abspath

from fabric.api import env, run, sudo, local, cd, get, task, put, prompt
from fabric.decorators import hosts, runs_once
from fabric.contrib.project import rsync_project
from fabric.contrib.files import exists
from fabric.context_managers import prefix, settings


@contextmanager
def virtualenv(path):
    """Context manager that performs commands with an active virtualenv, eg:

    path is the path to the virtualenv to apply

    >>> with virtualenv(env):
            run('python foo')

    """
    activate = posixpath.join(path, 'bin/activate')
    if not exists(activate):
        raise OSError("Cannot activate virtualenv %s" % path)
    with prefix('. %s' % activate):
        yield


def make_virtualenv(path, dependencies=[], eggs=[], system_site_packages=True):
    """Create/update a virtualenv in path.

    :param dependencies: a list of dependent packages to install, optionally
        with version numbers in normal pip/setuptools format.
    :param eggs: a list of eggs to pre-install to the virtualenv (which may
        satisfy some of the dependencies above).
    :param system_site_packages: Whether the virtualenv should include or
        exclude packages installed to the the system site-packages directories.

    """
    if not exists(path):
        version = tuple(run('virtualenv --version').split('.'))
        if version >= (1, 7):
            args = '--system-site-packages' if system_site_packages else ''
        else:
            args = '--no-site-packages' if not system_site_packages else ''
        run('virtualenv {args} {path}'.format(
            args=args,
            path=path
        ))
    with virtualenv(path):
        for e in eggs:
            with settings(warn_only=True):
                run('easy_install \'%s\'' % e)
        for d in dependencies:
            run('pip install \'%s\'' % d)


RSYNC_EXCLUSIONS = ['.hg', '*.swp', '*.pyc', '*.i']
DEFAULT_AMQP = 'amqp://guest:guest@localhost/'


@task
def test(amqp_url=None):
    """Run nucleon.amqp tests on a remote server.

    amqp_url is the URL of the AMQP server to test against.

    """
    if amqp_url is None:
        amqp_url = prompt(
            "No AMQP URL specified. "
            "Please enter the URL of the AMQP server to test against:",
            default=DEFAULT_AMQP,
            validate=r'^amqp://.+'
        ).strip()
    # Build source distribution
    version = local('python setup.py -V', capture=True)
    local('rm -rf dist/*')
    local('python setup.py sdist')

    run('mkdir -p nucleon-amqp')

    # Copy and unpack source distribution
    put('dist/nucleon.amqp-%s.tar.gz' % version, 'nucleon-amqp/')
    run('tar xzf nucleon-amqp/nucleon.amqp-%s.tar.gz -C nucleon-amqp/' % version)

    with cd('nucleon-amqp'):
        # Create/update virtualenv
        make_virtualenv(
            path='venv',
            dependencies=[
                'nose>=1.1.2',
                'coverage>=3.5.1',
                'pylint',
                'mock'
            ],
            eggs=[
                'http://release.vertulabs.co.uk/python/greenlet-0.4.0-py2.6-linux-x86_64.egg',
                'http://release.vertulabs.co.uk/python/gevent-1.0b4-py2.6-linux-x86_64.egg',
            ],
            system_site_packages=False
        )

        with virtualenv('~/nucleon-amqp/venv'):
            # Install distribution and dependencies into virtualenv
            with cd('nucleon.amqp-%s' % version):
                run('python setup.py develop')

                with settings(warn_only=True):
                    # Run the tests
                    run('AMQP_URL=%s python runtests.py' % amqp_url)
                    run('pylint --output-format=parseable --rcfile=../tests/pylint.rc nucleon >pylint.report')

                # Retrieve the results
                for resfile in ['tests.xunit.xml', 'coverage.xml', 'pylint.report']:
                    get(resfile, local_path=resfile)


@task
def publish_docs():
    # Check whether the tests pass.
    # If they do, then build the sphinx docs and copy
    # them back to the jenkins server

    with virtualenv('~/nucleon/NUCLEON_ENV'):
        # Install distribution and dependencies into virtualenv
        run('pip install "sphinx"')
        with cd('nucleon/tests'):
            with settings(warn_only=True):
                all_tests_pass =  run('python check_nucleon_tests.py')

    if all_tests_pass == 'True':
        # Copy docs to remote machine
        rsync_project(
        remote_dir='nucleon/docs',
        local_dir=abspath(dirname(__file__)) + '/docs/',
        delete=True,
        exclude=RSYNC_EXCLUSIONS)

        with virtualenv('~/nucleon/NUCLEON_ENV'):
            with cd('nucleon/docs'):
                run('make html')

        #Copy the docs back to the jenkins server
        get('nucleon/docs/_build', local_path='docs')


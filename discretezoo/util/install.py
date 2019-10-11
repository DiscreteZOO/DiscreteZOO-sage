r"""
Package installation

This module contains a function which installs the necessary files for
DiscreteZOO to work.
"""

import os
from distutils.spawn import find_executable
from shutil import copyfileobj
from subprocess import call
from urllib2 import urlopen
from zipfile import ZipFile
from StringIO import StringIO
import sage
import discretezoo

# Repository locations
GITREPO = "https://github.com/DiscreteZOO/DiscreteZOO-sage.git"
SPECZIP = "https://github.com/DiscreteZOO/Specifications/archive/master.zip"


def install():
    r"""
    Install the package.

    If the package has been loaded from a relative path, attempts to create a
    symbolic link in Sage's Python distribution's package storage to the
    directory containing the package. Then, the directory containing object
    specification is checked. If no files are found, then the specification
    files are fetched - if the package is in a git repository and git is
    available, then git is used, otherwise the latest specification files are
    downloaded.
    """
    curdir = os.path.join(os.getcwd(), discretezoo.__path__[0])

    if not any(x.startswith('/') for x in discretezoo.__path__):
        pkgsdir = os.path.dirname(sage.__path__[0])
        zoodir = os.path.join(pkgsdir, 'discretezoo')
        if os.path.exists(zoodir):
            print("Warning: the path %s exists "
                  "but DiscreteZOO has not loaded from it" % zoodir)
        elif not os.access(pkgsdir, os.W_OK):
            print("Warning: the path %s is not writable." % pkgsdir)
            print("To make DiscreteZOO always available from Sage, "
                  "run the following line as a sufficiently privileged user:"
                  "\n")
            print("ln -s %s %s\n" % (curdir, zoodir))
        else:
            print("Creating symbolic link at %s to %s" % (zoodir, curdir))
            os.symlink(curdir, zoodir)

    gitdir = os.path.realpath(curdir)
    specdir = os.path.join(gitdir, 'spec')
    git = os.path.exists(os.path.join(gitdir, '../.git'))
    if git:
        git = find_executable('git') is not None
        if not git:
            print("Warning: git is not available on your system.")
            print("It is recommended that you install it "
                  "in order to obtain updates and contribute.")
    else:
        print("Warning: DiscreteZOO is not located in a git repository.")
        print("For an easily updatable version, "
              "clone the official repository by running\n")
        print("git clone %s\n" % GITREPO)
    if git:
        if len(os.listdir(specdir)) == 0:
            print("Initializing the object specifications submodule...")
            if call(["git", "submodule", "init", "spec"], cwd=gitdir):
                raise ImportError("Error initializing "
                                  "the specifications submodule")
            if call(["git", "submodule", "update", "spec"], cwd=gitdir):
                raise ImportError("Error updating "
                                  "the specifications submodule")
            print("Object specifications submodule successfully initialized!")
    else:
        if len(os.listdir(specdir)) == 0:
            if not os.access(specdir, os.W_OK):
                raise ImportError("The path %s is empty and not writable.\n"
                                  "To use DiscreteZOO, "
                                  "download the archive at\n\n%s\n\n"
                                  "and extract it into the path above." %
                                  (specdir, SPECZIP))
            print("Downloading object specifications...")
            f = urlopen(SPECZIP)
            io = StringIO()
            io.write(f.read())
            f.close()
            zip = ZipFile(io)
            for member in zip.namelist():
                filename = os.path.basename(member)
                if not filename:
                    continue
                src = zip.open(member)
                tgt = file(os.path.join(specdir, filename), "wb")
                with src, tgt:
                    copyfileobj(src, tgt)
            zip.close()
            io.close()
            print("Object specifications succesfully downloaded!")

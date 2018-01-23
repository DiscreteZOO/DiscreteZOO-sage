# Installation

*DiscreteZOO* comes in two main parts: the Sage interface, and the database itself. This repository contains the Sage interface and some additional tools. The database is shipped separately.

## The Sage interface

To use the Sage interface, you will first have to obtain [Sage](http://www.sagemath.org/) version 6.10 or newer. Then either [download](https://github.com/DiscreteZOO/DiscreteZOO-sage/archive/master.zip) the contents of the repository and unzip it, or clone the repository to your local computer:
```bash
git clone --recursive https://github.com/DiscreteZOO/DiscreteZOO-sage.git
```

Your local copy will contain a folder named `discretezoo`, which contains the Python module that acts as the Sage interface. To use it, you should make sure that it is in the serach path of Sage's copy of Python. There are two basic ways of achieving this:
* Run Sage from the root folder of this repository.
* Copy or symlink the `discrete` folder into the search path, e.g.
```bash
cp -r /path/to/discretezoo/discretezoo /path/to/sage/local/lib/python/site-packages/ # to copy
ln -s /path/to/discretezoo/discretezoo /path/to/sage/local/lib/python/site-packages/ # to link
```
Depending on where your installation of Sage is located, you might need superuser privileges to do this. If you plan to contribute, symlinking is the preferred option.

Importing the `discretezoo` package in Sage will first try symlinking to the appropriate path, if this has not been done yet. After that, the folder `discretezoo/spec` is tested for existence of the specifications. If they are not found, the latest version will be downloaded.

## The database

Currently, a [database](http://discretezoo.xyz/discretezoo.db) is available which contains:
* all connected cubic vertex-transitive graphs with at most 1280 vertices (from the [census](http://www.matapp.unimib.it/~spiga/census.html) by P. Potočnik, P. Spiga and G. Verret),
* all connected cubic arc-transitive graphs with at most 2048 vertices (from the [extended Foster census](https://www.math.auckland.ac.nz/~conder/symmcubic2048list.txt) by M. Conder), and
* all vertex-transitive graphs with at most 31 vertices (from the [census](http://staffhome.ecm.uwa.edu.au/~00013890/remote/trans/index.html) by G. Royle).

To import it, complete the above steps, download the database, run Sage, and execute the following commands:
```sage
import discretezoo
discretezoo.DEFAULT_DB.importDB("/path/to/discretezoo.db")
```
This overwrites any previously existing file at `~/.discretezoo/discretezoo.db` with the specified file and uses it as a new database. After this is done, *DiscreteZOO* is ready for use.

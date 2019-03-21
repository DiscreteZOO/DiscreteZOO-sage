# *DiscreteZOO* Sage interface

*DiscreteZOO* is an extension for Sage providing access to a database of graphs with precomputed properties. It aims to become a database of various combinatorial objects with links between them.

## Installation

If you don't have it yet, obtain [Sage](http://www.sagemath.org/). Then [download](https://github.com/DiscreteZOO/DiscreteZOO-sage/archive/master.zip) or clone this repository into the working folder, and [make sure](INSTALL.md) that Sage will see the `discretezoo` folder once it is run.

Currently, a [database](http://baza.fmf.uni-lj.si/discretezoo.db) is available which contains:
* all connected cubic vertex-transitive graphs with at most 1280 vertices (from the [census](http://www.matapp.unimib.it/~spiga/census.html) by P. Potočnik, P. Spiga and G. Verret),
* all connected cubic arc-transitive graphs with at most 2048 vertices (from the [extended Foster census](https://www.math.auckland.ac.nz/~conder/symmcubic2048list.txt) by M. Conder), and
* all vertex-transitive graphs with at most 31 vertices (from the [census](http://staffhome.ecm.uwa.edu.au/~00013890/remote/trans/index.html) by G. Royle).

To import it, download it, and then run in Sage:
```sage
import discretezoo
discretezoo.DEFAULT_DB.importDB("/path/to/discretezoo.db")
```
Now you are ready to work with the graphs in the database. Several examples are available on the [wiki](https://github.com/DiscreteZOO/DiscreteZOO-sage/wiki/Database%20interface%20for%20Sage).

# GraphZOO

*GraphZOO* is an extension for Sage providing access to a database of graphs with precomputed properties. It aims to become a database of various combinatorial objects with links between them.

## Installation

If you don't have it yet, obtain [Sage](http://www.sagemath.org/). Then [download](https://github.com/GraphZOO/GraphZOO/archive/master.zip) or clone this repository into the working folder, and [make sure](INSTALL.md) that Sage will see the `graphzoo` folder once it is run.

A [database](http://baza.fmf.uni-lj.si/graphzoo.db) of all cubic vertex-transitive graphs with at most 1280 vertices is currently available. To import it, download it, and then run in Sage:
```sage
import graphzoo
graphzoo.DEFAULT_DB.importDB("/path/to/graphzoo.db")
```
Now you are ready to work with the graphs in the database. Several examples are available on the [wiki](https://github.com/GraphZOO/GraphZOO/wiki/Database%20interface%20for%20Sage).

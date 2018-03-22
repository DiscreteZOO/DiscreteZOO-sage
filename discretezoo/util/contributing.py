r"""
Contributing

This module contains functions for managing contributions to the database.
"""

import os
from datetime import datetime
import discretezoo
from ..db.query import Column
from ..entities.change import Change
from ..entities.zootypes import tables

def write_changes(location, user, db = None):
    if db is None:
        db = discretezoo.DEFAULT_DB
    folder = os.path.join("contributions",
                          "%s-%s" % (datetime.strftime(datetime.now(),
                                                       "%Y-%m-%d"), user))
    cur = db.query([Column("zooid"), Column("table"), Column("column")],
                   Change._spec["name"], cond = {"commit": ""},
                   distinct = True)
    for zooid, table, column in cur:
        cl = tables[table]
        obj = cl(zooid)
        if column == "":
            column = None
        obj.write_json(location, folder = folder,
                       field = (cl, column), link = False)

#!/bin/bash

pg_dump -a discretezoo -x --column-inserts > discretezoo.sql
cat > discretezoo-insert.sql <<EOF
PRAGMA synchronous = OFF;
PRAGMA journal_mode = MEMORY;
BEGIN TRANSACTION;
EOF
grep "^INSERT" discretezoo.sql | sed -r -e 's/true([,)])/1\1/g' -e 's/false([,)])/0\1/g' >> discretezoo-insert.sql
echo "END TRANSACTION;" >> discretezoo-insert.sql
cp discretezoo-empty.db discretezoo.db
sqlite3 discretezoo.db < discretezoo-insert.sql

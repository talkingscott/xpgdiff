# xpgdiff

Diffs two PostgreSQL database schemas, producing a migration DDL script.

The two databases are specified by libpq connection strings.  The users used to connect must have sufficient permissions to query various pg_catalog tables and execute some system functions.

Requires Python 3.

```
git clone git@github.com:talkingscott/xpgdiff.git
cd xpgdiff
pip install -r requirements.txt
chmod +x xpgdiff.py
./xpgdiff.py "host=prod1 dbname=product user=boss password=super" "host=dev dbname=product user=boss password=super" >migrate.sql
```

## FAQ

Why can't I install using pip?

The author prefers the simplicity of a single file script that you can just download.

Why is this a single file rather than a nice package?

The author prefers the simplicity of a single file script that you can just download.

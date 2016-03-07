===============================
tymongoimport
===============================

Almost a drop-in  replacement for mongoimport using the PyMongo and supporting only jsonl files

Usage
------

     tymongoimport
     (Almost) a drop-in replacement for Mongoimport

    Usage:
    tymongoimport [--batchSize=<bs>] [--upsert | --find-replace] [--host=<host:port>] [--database=<db>] [--collection=<col>]
    tymongoimport [--batchSize=<bs>] [--upsert | --find-replace] [--host=<host:port>] [--database=<db>] [--collection=<col>]  (--user=<user> --password=<passwd>)
    tymongoimport [--batchSize=<bs>] [--upsert | --find-replace] [--host=<host:port>]  [--database=<db>] [--collection=<col>] (--user=<user> --password=<passwd> --authenticationDatabase=<authdb>)
    tymongoimport --help
    tymongoimport --version

    Options:
    --help     Show this screen.
    --version     Show version.
    --upsert      Use ReplaceOne with upsert=True for inserting
    --db
    --batchSize=<bs>  the batch size [default: 1000]
    --host=<host:port>  mongo instance  [default: localhost]
    --user=<user>  the user with write access to the database
    --password=<passwd>  the password for authentication
    --authenticationDatabase=<authdb>  the database to perform the authentication to
    --database=<db>  the target database  [default: test]
    --collection=<col>  the target collection  [default: test]


Features
--------
Uses the PyMongo's bulk API to insert many documents into a mongo database. The parameters and options are a subset of
`mongoimport` binary.

Requirements
------------

- Python >= 2.6 or >= 3.3
- docopt
- pymongo >= 3.2

License
-------

MIT licensed. See the bundled `LICENSE <https://github.com/trustyou/tymongoimport/blob/master/LICENSE>`_ file for more details.

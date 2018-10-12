ePandda Ingest service
=======================

This is a service that accesses the PaleoBioDB and iDigBio APIs and ingests
the results into the ePandda ElasticSearch document store. It is configurable and
can be run as frequently or infrequently as desired. There are several important
components that this document describes.

Main Script Configuration and parameters
------------------------------------------

There is a large list of applicable settings for the ePandda ingester, and they
are available in the config-sample.json file. The settings for each are:

* version: Current version of this software
* elastic_host: The URL to the ElasticSearch service
* elastic_mapping: DEPRECATED, has no function
* idigbio_endpoint: The Elastic endpoint for the iDigBio collection
* pbdb_endpoint: The Elastic endpoint for the PBDB collection
* auth_secret: Authentication key for Elastic
* mongodb_host: Host for the MongoDB instance that hosts logs and other supporting elements
* mongodb_user: Monogo username
* mongodb_password: Password for above user
* logash_path: Path to a logstash executable, used to ingest records into Elastic
* idigbio_db: DEPRECATED, unused Mongo db
* pbdb_db: DEPRECATED, unused Monogo db
* endpoints_db: DEPRECATED, unused Monogo db
* idigbio_coll: DEPRECATED, unused
* pbdb_coll: DEPRECATED, unused
* pbdb_ingest_interval: Number of days ago from which to start PBDB ingest
* idigbio_db: Number of days ago from which to start iDigBio ingest
* log_db: Mongo db where logs are stored
* ingest_collection: collection in mongo where information about ingests are stored
* sentinel_ratio: decimal number for percentage of records to use as sentinel tests, should be very small
* test_indexes: DEPRECATED, part of old MongoDB testing scheme
* email_recipients: List of recipients to recieve email updates on ingest status
* smtp: Dictionary of standard smtp settings for sending of mail

In order to run the ePandda ingester there is a set of parameters that are passed
to the script call (either manually or through a cron task)

The core call is to ``python ingest.py``, a help text describing the options can be
obtained by passing ``python ingest.py -h`` The general options are:
* -s/--source: What sources are to be ingested from, currently this accepts idigbio and pbdb
* -d/--dryrun: Set to test current configuration and record download, but does not import records
* -t/--test: Set to import only a small subset of accessed records, use to verify that system is working
* -l/--logLevel: Set to control output to the console, takes standard python log levels
* -F/--fullRefresh: Wipe database and pull in fresh set of data from sources set with -s. CAUTION WILL DELETE DB
* -D/--removeDeleted: If set will scan database for deleted records and remove any found. Will significantly length amount of time this takes to run

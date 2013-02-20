Leveldbkit
==========

Leveldbkit is an object mapper that for leveldb that also has secondary indexes!
This means it adds bloat and complexity to leveldb so use at your own risk!

The API is almost compatible with [riakkit](https://github.com/shuhaowu/riakkit)
(next generation riakkit only, post version 0.6) so you should be able to
port your applications really easily if you decided to use riak instead of
leveldb!

This means that non of the old bloat is here (should they be? let me know).
Basic features are here and porting your application to riakkit will be really
simple once riakkit-ng is complete!

A lot of the code here is stolen from riakkit-ng. This means that the code will
be shared between the riakkit and leveldbkit (namely properties and EmDocument)

This probably will degrade your performance.

Motivation
==========

I want to work on an app with riak. Riak-python-client is undergoing major
overhaul. So leveldb fits my needs and I can use it for now and port my app
into riak later.

I'm surprised how little code I had to write for this. It took me < 1 hour to
port simple features such as save, delete, get, and reload! Full port took about
2 hours (actually i haven't completed the riakkit-ng rewrite at the time)

    Now, owl.
    ,___,  ,___,
    (O,O)  (O,O)
    /)_)    (_(\
     ""      ""

Features
========

 - Object gets stored into leveldb file via json automagically.
 - Validation, conversion for object properties
 - Remote document references.
 - Automatic key generation (via uuid1)
 - Secondary indices (the implementation is currently fairly dumb: a leveldb db
   for a big hashtable that goes field_value => [doc_key1, doc_key2 ....], and
   a db is needed for every field for every Document subclass. See issue #2 for
   potential improvements)
 - Interface like Couchdbkit, Riakkit, and Django modelling system,
   GAE's modelling system.
 - As py3k friendly as possible, but made for py2.7 :)

Get started
===========

Installation
------------

Install via `$ python setup.py install`. This should take care of dependencies.
Install the optional dependencies if you'd like to take advantage of those 
features.

Dependencies if you're just linking in:

 1. [leveldb](http://code.google.com/p/leveldb/)
 2. [pyleveldb](http://code.google.com/p/py-leveldb/)

Optional libraries that this thing tries to use

 1. [bcrypt](http://www.mindrot.org/projects/py-bcrypt/) for `PasswordProperty`.
    Falls back to an insecure method involving SHA and `urandom` salt generation
 2. [ujson](https://github.com/esnme/ultrajson) for ultra fast json and json
    generation with no whitespaces. Falls back to `simplejson`, then just plain
    `json`.

Tutorial
--------

Will write one when I write an app for it and make this for reals. For now just
browse through the source code and take a look at the riakkit tutorial (although
the new version is not out you could see what kinda is the case).

Reference
---------

# -*- coding: utf-8 -*-
# This file is part of Riakkit or Leveldbkit
#
# Riakkit or Leveldbkit is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Riakkit or Leveldbkit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Riakkit or Leveldbkit. If not, see <http://www.gnu.org/licenses/>.

# Originally from riakkit, now I stole it from myself and put it into leveldbkit

from __future__ import absolute_import
try:
  import ujson as json
except ImportError:
  try:
    import simplejson as json
  except ImportError:
    import json

from uuid import uuid1

from .properties.standard import BaseProperty
from .helpers import walk_parents
from .exceptions import ValidationError, NotFoundError

from leveldb import WriteBatch

# Let's do something gross.

class EmDocumentMetaclass(type):
  def __new__(cls, clsname, parents, attrs):
    if clsname in ("Document", "EmDocument"):
      return type.__new__(cls, clsname, parents, attrs)

    meta = {}
    for name in attrs.keys():
      if isinstance(attrs[name], BaseProperty):
        meta[name] = attrs.pop(name)

    all_parents = reversed(walk_parents(parents))

    for p_cls in all_parents:
      meta.update(p_cls._meta)

    attrs["_meta"] = meta
    return type.__new__(cls, clsname, parents, attrs)

  def __getattr__(self, name):
    if hasattr(self, "_meta") and name in self._meta:
      return self._meta[name]
    raise AttributeError("'{0}' does not exist for class '{1}'.".format(name, self.__name__))


class EmDocument(object):
  """Embedded document as a JSON object"""
  __metaclass__ = EmDocumentMetaclass

  def __init__(self, data={}):
    self.clear()
    self.merge(data)

  def _validation_error(self, name, value):
    raise ValidationError("'{0}' doesn't pass validation for property '{1}'".format(value, name))

  def _attribute_not_found(self, name):
    raise AttributeError("Attribute '{0}' not found with '{1}'.".format(name, self.__class__.__name__))

  def serialize(self, dictionary=True):
    d = {}
    for name, value in self._data.iteritems():
      if name in self._meta and isinstance(self._meta[name], BaseProperty):
        if not self._meta[name].validate(value):
          self._validation_error(name, value)
        value = self._meta[name].to_db(value)

      d[name] = value

    return d if dictionary else json.dumps(d)

  def deserialize(self, data):
    converted_data = {}
    props_to_load = set()

    for name, value in data.iteritems():
      if name in self._meta:
        if self._meta[name].load_on_demand:
          props_to_load.add(name)
        else:
          value = self._meta[name].from_db(value)

      converted_data[name] = value

    self.merge(converted_data, True)
    self._props_to_load = props_to_load
    return self

  @classmethod
  def load(cls, data):
    doc = cls()
    return doc.deserialize(data)

  def is_valid(self):
    for name in self._meta:
      if not self._validate_attribute(name):
        return False
    return True

  def invalids(self):
    invalid = []
    for name in self._meta:
      if not self._validate_attribute(name):
        invalid.append(name)

    return invalid

  def _validate_attribute(self, name):
    if name not in self._data:
      self._attribute_not_found(name)

    if name in self._meta:
      return self._meta[name].validate(self._data[name])

    return True

  def merge(self, data, merge_none=False):
    if isinstance(data, EmDocument):
      data = data._data
    elif isinstance(data, basestring):
      data = json.loads(data)

    for name, value in data.iteritems():
      if not merge_none and name in self._meta and value is None:
        continue
      self.__setattr__(name, value)

    return self

  def clear(self, to_default=True):
    self._data = {}
    self._props_to_load = set()

    if to_default:
      for name, prop in self._meta.iteritems():
        self._data[name] = prop.default()
    else:
      for name, prop in self._meta.iteritems():
        self._data[name] = None

    return self

  def __setattr__(self, name, value):
    if name[0] == "_":
      self.__dict__[name] = value
      return

    if name in self._meta:
      if hasattr(self._meta[name], "on_set"):
        value = self._meta[name].on_set(value)

    self._data[name] = value

  def __getattr__(self, name):
    if name in self._data:
      if name in self._props_to_load:
        self._data[name] = self._meta[name].from_db(self._data[name])
        self._prop_to_load.discard(name)
      return self._data[name]
    self._attribute_not_found(name)

  def __delattr__(self, name):
    if name in self._data:
      if name in self._meta:
        self._data[name] = None
      else:
        print name
        del self._data[name]
    else:
      self._attribute_not_found(name)

  __setitem__ = __setattr__
  __getitem__ = __getattr__
  __delitem__ = __delattr__


class DocumentMetaclass(EmDocumentMetaclass):
  def __new__(cls, clsname, parents, attrs):
    attrs["_index_dbs_write_batches"] = {}

    if "index_dbs" in attrs:
      for field, dbs in attrs["index_dbs"].iteritems():
        attrs["_index_dbs_write_batches"][field] = WriteBatch()

    return EmDocumentMetaclass.__new__(cls, clsname, parents, attrs)

class Document(EmDocument):
  """The base Document class for custom classes to extend from.
  There are a couple of class variables that's required for this to work:
    - `db`: a `leveldb.LevelDB` instance that points to the database.
    - `index_dbs`: a dictionary: 2i field => `leveldb.LevelDB` instance.

  """
  __metaclass__ = DocumentMetaclass

  _write_batch = WriteBatch()

  index_dbs = {}
  _index_dbs_write_batches = {}

  @classmethod
  def _flush_indexes(cls, sync=True):
    for field, db in cls.index_dbs.iteritems():
      db.Write(cls._index_dbs_write_batches[field], sync=sync)
      cls._index_dbs_write_batches[field] = WriteBatch()

  @classmethod
  def flush(cls, sync=True, db=None):
    db = db or cls.db
    db.Write(cls._write_batch, sync=sync)
    cls._write_batch = WriteBatch()
    cls._flush_indexes(sync)

  def __hash__(self):
    return hash((self.key, self.bucket_name, self.vclock))

  def __eq__(self, other):
    if isinstance(other, self.__class__):
      return hash(self) == hash(other)
    else:
      return False

  def __ne__(self, other):
    return not self.__eq__(other)

  def __init__(self, key=lambda: uuid1().hex, data={}, db=None):
    if callable(key):
      key = key()

    if not isinstance(key, basestring):
      raise TypeError("Key must be a string (offender: {0}).".format(key))

    self.__dict__["key"] = key
    EmDocument.__init__(self, data)
    self.__dict__["db"] = db or self.__class__.db
    self.__dict__["_indexes"] = set()

  @classmethod
  def get(cls, key, verify_checksums=False, fill_cache=True, db=None):
    doc = cls(key=key, db=db)
    return doc.reload(verify_checksums, fill_cache, db)

  @classmethod
  def get_or_new(cls, key, verify_checksums=False, fill_cache=True, db=None):
    doc = cls(key=key, db=db)
    try:
      return doc.reload(verify_checksums, fill_cache, db)
    except NotFoundError:
      return doc

  @classmethod
  def _ensure_index_db_exists(cls, field):
    if field not in cls.index_dbs:
      raise KeyError("Index field '{0}' does not have a db defined!")

  @classmethod
  def index_lookup(cls, field, start_value, end_value=None):
    cls._ensure_index_db_exists(field)
    if end_value is None:
      try:
        keys = json.loads(cls.index_dbs[field].Get(start_value))
      except KeyError:
        keys = []

      for key in keys:
        doc = cls(key)
        yield doc.reload()

    else:
      for index_value, keys in cls.index_dbs[field].RangeIter(start_value, end_value):
        keys = json.loads(keys)
        for key in keys:
          doc = cls(key)
          yield doc.reload()


  def clear(self, to_default=True):
    EmDocument.clear(self, to_default)
    self._indexes = set()
    return self

  def reload(self, verify_checksums=False, fill_cache=True, db=None):
    db = db or self.db
    try:
      value = db.Get(self.key)
    except KeyError:
      raise NotFoundError("{0} not found".format(self.key))

    value = json.loads(value)

    if "_2i" in value:
      # json doesn't have tuple. We need to convert everything back into
      # tuple so we can use a set.
      self._indexes = set([(i[0], i[1])for i in value.pop("_2i")])

    self.deserialize(value)
    return self

  def save(self, sync=True, db=None, batch=False):
    value = self.serialize()
    value["_2i"] = list(self._indexes)
    value = json.dumps(value)

    for f, v in self._indexes:
      try:
        keys = json.loads(self.__class__.index_dbs[f].Get(v))
      except KeyError:
        keys = []

      if self.key not in keys: # slow. I know. We'll fix this eventually.
        keys.append(self.key)
        self.__class__._index_dbs_write_batches[f].Put(v, json.dumps(keys))

    if batch:
      self._write_batch.Put(self.key, value)
    else:
      db = db or self.db
      db.Put(self.key, value, sync)
      self._flush_indexes(sync)

    return self

  def delete(self, sync=True, db=None, batch=False):
    for field, value in self._indexes:
      try:
        keys = json.loads(self.__class__.index_dbs[field].Get(value))
      except KeyError:
        continue

      try:
        keys.remove(self.key)
      except ValueError:
        continue

      if len(keys) == 0:
        self.__class__._index_dbs_write_batches[field].Delete(value)
      else:
        self.__class__._index_dbs_write_batches[field].Put(value, json.dumps(keys))

    if batch:
      self._write_batch.Delete(self.key)
    else:
      db = db or self.db
      db.Delete(self.key, sync)
      self._flush_indexes(sync)
    return self

  @classmethod
  def delete_key(cls, key, sync=False, db=None, batch=False):
    if batch:
      cls._write_batch.Delete(key)
    else:
      db = db or cls.db
      db.Delete(cls.key, sync)

  def add_index(self, field, value): # TODO: add batch operation
    self._ensure_index_db_exists(field)
    self._indexes.add((field, value))
    return self

  def remove_index(self, field, value=None):
    self._ensure_index_db_exists(field)
    if value is None:
      self._indexes = {(f, v) for f, v in self._indexes if f != field}
    else:
      self._indexes.discard((field, value))
    return self

  def set_index(self, indexes):
    self._indexes = indexes

  def index(self, field=None):
    if field is None:
      return self._indexes
    else:
      return [v for f, v in self._indexes if f == field]

  indexes = index

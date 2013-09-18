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
  # We prefer ujson, then simplejson, then json
  import ujson as json
except ImportError:
  try:
    import simplejson as json
  except ImportError:
    import json

from uuid import uuid1
from copy import copy

from .properties.standard import BaseProperty, StringProperty, NumberProperty, ReferenceProperty, ListProperty
from .helpers import walk_parents
from .exceptions import ValidationError, NotFoundError, DatabaseError

from leveldb import WriteBatch, LevelDB

class EmDocumentMetaclass(type):
  def __new__(cls, clsname, parents, attrs):
    if clsname in ("Document", "EmDocument"):
      return type.__new__(cls, clsname, parents, attrs)

    meta = {}

    indexes = []

    all_parents = reversed(walk_parents(parents))

    for p_cls in all_parents:
      if hasattr(p_cls, "_meta"):
        meta.update(p_cls._meta)

      if hasattr(p_cls, "_indexes"):
        indexes += list(p_cls._indexes)

    for name in attrs.keys():
      if isinstance(attrs[name], BaseProperty):
        meta[name] = attrs.pop(name)
        if isinstance(meta[name], (StringProperty, NumberProperty, ListProperty, ReferenceProperty)) and meta[name]._index:
          indexes.append(name)

    attrs["_meta"] = meta
    attrs["defined_properties"] = meta.keys()
    attrs["_indexes"] = indexes
    return type.__new__(cls, clsname, parents, attrs)

  def __getattr__(self, name):
    if hasattr(self, "_meta") and name in self._meta:
      return self._meta[name]
    raise AttributeError("'{0}' does not exist for class '{1}'.".format(name, self.__name__))


class EmDocument(object):
  """Embedded document as a JSON object

  Class Variables:
    - `DEFINED_PROPERTIES_ONLY`: A boolean value indicating that the only
                                 properties allowed are the ones defined.
                                 Defaults to False. If a violation is found,
                                 an error will be raised on `serialize` (and
                                 `save` for Document) and False will be returned
                                 in `is_valid`. `invalids` will return
                                 `"_extra_props"` in the list.
    - defined_properties: A list of defined properties. For read only.
  """
  __metaclass__ = EmDocumentMetaclass

  DEFINED_PROPERTIES_ONLY = False

  def __init__(self, data={}):
    """Initializes a new EmDocument

    Args:
      data: A dictionary that's supposed to be initialized with the
            document as attributes.
    """
    self.clear()
    self.merge(data)

  def _validation_error(self, name, value):
    raise ValidationError("'{0}' doesn't pass validation for property '{1}'".format(value, name))

  def _attribute_not_found(self, name):
    raise AttributeError("Attribute '{0}' not found with '{1}'.".format(name, self.__class__.__name__))

  def serialize(self, dictionary=True, restricted=tuple()):
    """Serializes the object into a dictionary with all the proper conversions

    Args:
      dictionary: boolean. If True, this will return a dictionary, otherwise the
                  dictionary will be dumped by json.
      restricted: The properties to not output in the serialized output. Useful
                  in scenarios where you need to hide info from something.
                  An example would be you need to hide some sort of token from
                  the user but you need to return the object to them, without
                  the token. This defaults to tuple(), which means nothing is
                  restricted.
    Returns:
      A plain dictionary representation of the object after all the conversion
      to make it json friendly.
    """
    # Note that this doesn't call is_valid as it has built in validation.

    d = {}
    for name, value in self._data.iteritems():
      if name in restricted:
        continue
      if name in self._meta:
        if not self._meta[name].validate(value):
          self._validation_error(name, value)
        value = self._meta[name].to_db(value)
      elif self.DEFINED_PROPERTIES_ONLY:
        raise ValidationError("Property {} is not defined and {} has DEFINED_PROPERTIES_ONLY".format(name, self.__class__.__name__))

      d[name] = value

    return d if dictionary else json.dumps(d)

  def deserialize(self, data):
    """Deserializes the data. This uses the `from_db` method of all the
    properties. This differs from `merge` as this assumes that the data is from
    the database and will convert from db whereas merge assumes the the data
    is from input and will not do anything to it. (unless the property has
    `on_set`).

    Args:
      data: The data dictionary from the database.

    Returns:
      self, with its attributes populated.

    """
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
    """A convenient method that creates an object and deserializes the data.

    Args:
      data: The data to be deserialized

    Returns:
      A document with the data deserialized.
    """
    doc = cls()
    return doc.deserialize(data)

  def is_valid(self):
    """Test if all the attributes pass validation.

    Returns:
      True or False
    """
    for name in self._meta:
      if not self._validate_attribute(name):
        return False

    # Seems inefficient. Any better way to do this?
    if self.DEFINED_PROPERTIES_ONLY:
      for name in self._data:
        if name not in self._meta:
          return False

    return True

  def invalids(self):
    """Get all the attributes' names that are invalid.

    Returns:
      A list of attribute names that have invalid values.
    """
    invalid = []
    for name in self._meta:
      if not self._validate_attribute(name):
        invalid.append(name)

    # TODO: Refactor with is_valid
    if self.DEFINED_PROPERTIES_ONLY:
      for name in self._data:
        if name not in self._meta:
          invalid.append("_extra_props")
    return invalid

  def _validate_attribute(self, name):
    if name not in self._data:
      self._attribute_not_found(name)

    if name in self._meta:
      return self._meta[name].validate(self._data[name])

    return True

  def merge(self, data, merge_none=False):
    """Merge the data from a non-db source.

    This method treats all `None` values in data as if the key associated with
    that `None` value is not even present. This will cause us to automatically
    convert the value to that property's default value if available.

    If None is indeed what you want to merge as oppose to the default value
    for the property, set `merge_none` to `True`.

    Args:
      data: The data dictionary, a json string, or a foreign document to merge
            into the object.
      merge_none: Boolean. If set to True, None values will be merged as is
                  instead of being converted into the default value of that
                  property. Defaults to False.
    Returns:
      self
    """
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
    """Clears the object. Set all attributes to default or nothing.

    Args:
      to_default: Boolean. If True, all properties defined will be set to its
                  default value. Otherwise the document will be empty.

    Returns:
      self
    """
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
    """Sets the attribute of the object and calls `on_set` of the property
    if the property is defined and it has an `on_set` method.

    Args:
      name: name of the attribute
      value: the value of that attribute to set to.
    """
    if name[0] == "_" or name == "key":
      self.__dict__[name] = value
      return

    if name in self._meta:
      if hasattr(self._meta[name], "on_set"):
        value = self._meta[name].on_set(value)

    self._data[name] = value

  def __getattr__(self, name):
    """Get an attribute from the document.
    Note that if a property is defined and the value is not set. This will
    always return None. (Also remember that if a value is not set by you it
    does not mean that it is not initialized to its default value.)

    Args:
      name: the name of the attribute.
    Returns:
      The value of the attribute.
    """
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
        del self._data[name]
    else:
      self._attribute_not_found(name)

  __setitem__ = __setattr__
  __getitem__ = __getattr__
  __delitem__ = __delattr__

class DocumentMetaclass(EmDocumentMetaclass):
  def __new__(cls, clsname, parents, attrs):
    attrs["_write_batch"] = WriteBatch()
    attrs["_index_write_needed"] = False

    attrs["_indexdb_write_batch"] = WriteBatch()

    return EmDocumentMetaclass.__new__(cls, clsname, parents, attrs)

_INDEX_KEY = "{f}~{v}"

class Document(EmDocument):
  """The base Document class for custom classes to extend from.
  There are a couple of class variables that's required for this to work:
    - `db`: a `leveldb.LevelDB` instance that points to the database.
    - `indexdb`: a dictionary: 2i field => `leveldb.LevelDB` instance.
    - `OPEN_ONLY_WHEN_NEEDED`: This indicates that `db` and `indexdb` are paths
                               to the database and it will only open when we
                               write (no more locks! although race conditions)
                               At the end of the day I'm gonna write a leveldb
                               server based off of https://github.com/srinikom/leveldb-server
  """
  __metaclass__ = DocumentMetaclass

  OPEN_ONLY_WHEN_NEEDED = False

  @classmethod
  def establish_connection(cls):
    """If you didn't specify a LevelDB instance and just a path, use this to
    open a connection if OPEN_ONLY_WHEN_NEEDED is False. (It will also set it
    to False). Calling this multiple times will not be bad as this checks if
    cls.db/indexdb is a basestring or not.
    """
    if hasattr(cls, "db") and isinstance(cls.db, basestring):
      cls.db = LevelDB(cls.db)

    if hasattr(cls, "indexdb") and isinstance(cls.indexdb, basestring):
      cls.indexdb = LevelDB(cls.indexdb)

    cls.OPEN_ONLY_WHEN_NEEDED = False

  @classmethod
  def _get_indexdb(cls):
    return LevelDB(cls.indexdb) if cls.OPEN_ONLY_WHEN_NEEDED else cls.indexdb

  @classmethod
  def _get_db(cls, db=None):
    db = db or cls.db
    return LevelDB(db) if cls.OPEN_ONLY_WHEN_NEEDED else db

  @classmethod
  def _flush_indexes(cls, sync=True):
    if cls._index_write_needed:
      cls._get_indexdb().Write(cls._indexdb_write_batch, sync=sync)
      cls._indexdb_write_batch = WriteBatch()
      cls._index_write_needed = False

  @classmethod
  def flush(cls, sync=True, db=None):
    """Flushes all the batch operations.

    Args:
      sync: sync argument to pass to leveldb.
      db: The db to write to. Defaults to the default class database. The index
          dbs will not be affected.
    """
    db = db or cls._get_db()
    db.Write(cls._write_batch, sync=sync)
    cls._write_batch = WriteBatch()
    cls._flush_indexes(sync)

  @classmethod
  def reset_write_batch(cls):
    """Empties the current write batch.
    This means all the current writes are void"""
    cls._write_batch = WriteBatch()
    cls._indexdb_write_batch = WriteBatch()
    cls._index_write_needed = False

  def __init__(self, key=lambda: uuid1().hex, data={}, db=None):
    """Creates a new instance of a document.

    Args:
      key: the key to initialize this to. A function that takes no argument
           is also accepted and it should return the key for this object. This
           defaults to generating uuid1 as the key.
      data: The data dictionary to merge to.
      db: A db other than the class database. However, if you use batch this
          will be ignored. Defaults to the class db.
    """
    if callable(key):
      key = key()

    if not isinstance(key, basestring):
      raise TypeError("Key must be a string (offender: {0}).".format(key))

    self.__dict__["key"] = key
    EmDocument.__init__(self, data)
    self.__dict__["db"] = db
    self.__dict__["_old_indexes"] = {}

  @classmethod
  def get(cls, key, verify_checksums=False, fill_cache=True, db=None):
    """Gets a document from the database given a key.

    Args:
      key: the key to get.
      verify_checksums: See pyleveldb's documentation
      fill_cache: See pyleveldb's documentation
      db: A `leveldb.LevelDB` instance to get from. Defaults to the object/class
          db.
    Returns:
      The document associated with that key.
    Raises:
      NotFoundError: when the key is not found in db.
    """
    doc = cls(key=key, db=db)
    return doc.reload(verify_checksums, fill_cache, db)

  @classmethod
  def get_or_new(cls, key, verify_checksums=False, fill_cache=True, db=None):
    """Gets a document from the database given a key. If not found, create one.
    Note that this does not actually save.
    """
    doc = cls(key=key, db=db)
    try:
      return doc.reload(verify_checksums, fill_cache, db)
    except NotFoundError:
      return doc

  @classmethod
  def _ensure_indexdb_exists(cls, field=None):
    if not cls.indexdb:
      raise DatabaseError("indexdb is not defined for `{0}`".format(cls.__name__))

    if field in ("$key", "$bucket"):
      return

    if not (field and field in cls._meta and cls._meta[field]._index):
      raise DatabaseError("Field '{0}' is not indexed!".format(field))

  @classmethod
  def index_keys_only(cls, field, start_value, end_value=None):
    """Index lookup. Given a field and a value, find the associated document
    keys.

    Args:
      field: The field name
      start_value: the value to look for, or the beginning value for a range
      end_value: if not None, this is a ranged search, that is, all document with
                 of field and value between start_value and end_value will be
                 returned
    Returns:
      A list of all the keys associated.

    Raises:
      DatabaseError if no index database is defined.
    """
    cls._ensure_indexdb_exists(field)

    if field == "$bucket":
      return [key for key, _ in cls.db.RangeIter()]
    if field == "$key":
      return [key for key, _ in cls.db.RangeIter(start_value, end_value)]

    if isinstance(cls._meta[field], NumberProperty):
      start_value = float(start_value)
      if end_value is not None:
        end_value = float(end_value)

    if end_value is None:
      try:
        return json.loads(cls._get_indexdb().Get(_INDEX_KEY.format(f=field, v=start_value)))
      except KeyError:
        return []
    else:
      all_keys = []
      for index_value, keys in cls._get_indexdb().RangeIter(_INDEX_KEY.format(f=field, v=start_value), _INDEX_KEY.format(f=field, v=end_value)):
        all_keys.extend(keys)
      return all_keys

  @classmethod
  def index(cls, field, start_value, end_value=None):
    """Index lookup. Given a field and a value, find the associated documents

    Args:
      field: The field name
      start_value: the value to look for, or the beginning value for a range
      end_value: if not None, this is a ranged search, that is, all document with
                 of field and value between start_value and end_value will be
                 returned
    Returns:
      A generator that iterates through all the documents

    Raises:
      DatabaseError if no index database is defined.
    """
    cls._ensure_indexdb_exists(field)

    if field == "$bucket":
      for key, _ in cls.db.RangeIter():
        yield cls(key).reload()
    elif field == "$key":
      for key, _ in cls.db.RangeIter(start_value, end_value):
        yield cls(key).reload()
    else:
      if isinstance(cls._meta[field], NumberProperty):
        start_value = float(start_value)
        if end_value is not None:
          end_value = float(end_value)

      if end_value is None:
        try:
          keys = json.loads(cls._get_indexdb().Get(_INDEX_KEY.format(f=field, v=start_value)))
        except KeyError:
          keys = []

        for key in keys:
          yield cls(key).reload()

      else:
        for index_value, keys in cls._get_indexdb().RangeIter(_INDEX_KEY.format(f=field, v=start_value), _INDEX_KEY.format(f=field, v=end_value)):
          keys = json.loads(keys)
          for key in keys:
            yield cls(key).reload()

  def clear(self, to_default=True):
    EmDocument.clear(self, to_default)
    self._indexes = set()
    self._removed_indexes = set()
    return self

  def reload(self, verify_checksums=False, fill_cache=True, db=None):
    """Reloads the document from the database

    Args:
      verify_checksums: See pyleveldb's documentation
      fill_cache: See pyleveldb's documentation
      db: A `leveldb.LevelDB` instance to reload from. Defaults to the
          object/class db.
    """
    db = self.__class__._get_db(db or self.db)
    try:
      value = db.Get(self.key)
    except KeyError:
      raise NotFoundError("{0} not found".format(self.key))

    value = json.loads(value)

    self.deserialize(value)
    return self


  def _add_to_index_write_batch(self, field, value):
    if value is None: # We value is null. This is a refactoring step.
      return

    index_key = _INDEX_KEY.format(f=field, v=value)
    try:
      keys = json.loads(self.__class__._get_indexdb().Get(index_key))
    except KeyError:
      keys = []

    if self.key not in keys:
      keys.append(self.key)
      self.__class__._indexdb_write_batch.Put(index_key, json.dumps(keys))
      self.__class__._index_write_needed = True

  def _remove_from_index_write_batch(self, field, value):
    index_key = _INDEX_KEY.format(f=field, v=value)
    try:
      keys = json.loads(self.__class__._get_indexdb().Get(index_key))
    except KeyError:
      return # Consider as already removed...

    try:
      keys.remove(self.key)
    except ValueError:
      return # Consider as already removed...

    if len(keys) == 0:
      # do some housekeeping.
      self.__class__._indexdb_write_batch.Delete(index_key)
    else:
      self.__class__._indexdb_write_batch.Put(index_key, json.dumps(keys))

    self.__class__._index_write_needed = True

  def _build_indexes(self, data):
    indexes = {}
    for name in self.__class__._indexes:
      indexes[name] = copy(data.get(name, None))
    return indexes

  def _figure_out_index_writes(self, old, new):
    # Let the magic begin.
    # > also, this kinda behaviour will usually result in a conflict because
    #   we are caching some states and if it gets modified else where....
    # > GOOD THING LEVELDB IS SINGLE THREADED. AMIRITE? :D
    # > No. You're not right. If you have in the same application 2 copies
    #   of the same document... you see where I'm headed with this?
    # > Please don't do that.

    # old and new are all serialized data.
    # The only type difference is there is list, string, and numbers o.o

    for field, value in old.iteritems():
      if isinstance(value, (list, tuple)): # this looks hackish.
        old_values = set(value)

        _temp = new.get(field)
        new_values = set(_temp) if _temp else set()

        for v in (old_values - new_values):
          self._remove_from_index_write_batch(field, v)
        for v in (new_values - old_values):
          self._add_to_index_write_batch(field, v)
      else:
        # other types don't really matter.
        if field not in new:
          self._remove_from_index_write_batch(field, value)
        else:
          if value != new[field]:
            self._remove_from_index_write_batch(field, value)
            # None values should be handled by _add_to_index_write_batch and
            # it should simply return.
            self._add_to_index_write_batch(field, new[field])

    # Now we need to take a look at the new dictionary and make sure to add
    # anything that we missed. We already noted the change in field as well as
    # any field that is removed and became None in the lines above.

    # there's gotta be an easier way to do this?? :x

    for field, value in new.iteritems():
      if field not in old:
        # TODO: refactor
        if isinstance(value, (list, tuple)):
          for v in value:
            self._add_to_index_write_batch(field, v)
        else:
          self._add_to_index_write_batch(field, value)

  def save(self, sync=True, db=None, batch=False):
    """Saves the document to the database

    Args:
      sync: sync argument to pass to leveldb
      db: The db to save to. defaults to object/class db
      batch: If this is a batch operation. If True, it will be queued and
             actually stored when Document.flush (replace Document with your
             class name) is called. If True, sync and db will be ignored.
    Returns:
      self
    """
    value = self.serialize()

    new_indexes = self._build_indexes(value)
    self._figure_out_index_writes(self._old_indexes, new_indexes)
    # BUG: (?) Is it possible to fail something so badly that the _old_indexes
    # never gets flushed? Hopefully not.
    self._old_indexes = new_indexes

    value = json.dumps(value)

    if batch:
      self._write_batch.Put(self.key, value)
    else:
      db = self.__class__._get_db(db or self.db)
      db.Put(self.key, value, sync)
      self._flush_indexes(sync)

    return self

  def delete(self, sync=True, db=None, batch=False):
    """Deletes the object from the database.

    Args:
      sync: sync argument to pass to leveldb
      db: The db to delete from. defaults to object/class db
      batch: If this is a batch operation. If True, it will be queued and
             actually deleted when Document.flush (replace Document with your
             class name) is called. If True, sync and db will be ignored.
    Returns:
      self
    """
    self._figure_out_index_writes(self._old_indexes, {})
    self._old_indexes = {}

    if batch:
      self._write_batch.Delete(self.key)
    else:
      db = self.__class__._get_db(db or self.db)
      db.Delete(self.key, sync)
      self._flush_indexes(sync)

    self.clear(False)
    return self

  def serialize(self, dictionary=True, restricted=tuple(), include_key=False, expand=[]):
    """Serializes this object. Doc same as `EmDocument.serialize` except there
    is an extra argument.

    Args:
      Same as EmDocument other the following addition:
      include_key: A boolean indicate if the key should be serialized under
      "key". Defaults to False.
      expand: Defaults to an empty list. If this list is empty, all reference
              property will be serialized as keys.
              The list holds a list of dictionaries. These are the arguments to
              this method without the `expand`. The number of arguments will be
              the number of layers to expand down. For example, if there are two
              dictionaries of arguments, it will expand two levels down (that is,
              it will look at any reference properties for the immediate object
              and any reference properties of those objects). The order of the
              list is so that level 1 would be the first argument, level 2 the
              second and so forth.
    """
    if include_key:
      d = EmDocument.serialize(self, True, restricted)
      d["key"] = self.key
    else:
      d = EmDocument.serialize(self, True, restricted)

    if len(expand) > 0:
      kwargs = expand.pop(0)
      for name, attr in self._meta.iteritems():
        if isinstance(attr, ReferenceProperty) and name not in restricted and self[name] is not None:
          # we copy to make sure that there is no weird bug when we pop at
          # different points of the stack. Shallow copy is good enough as the
          # dicts themselves are readonly.
          kwargs["expand"] = copy(expand)
          d[name] = self[name].serialize(**kwargs)

    if not dictionary:
      return json.dumps(d)
    return d

  def deserialize(self, data):
    self._old_indexes = self._build_indexes(data)
    return EmDocument.deserialize(self, data)

  @classmethod
  def delete_key(cls, key, sync=False, db=None, batch=False):
    """Delete something from the database without loading it.

    Args:
      key: the key to delete.
      sync: sync argument to pass to leveldb
      db: The db to delete from. defaults to object/class db
      batch: If this is a batch operation. If True, it will be queued and
             actually deleted when Document.flush (replace Document with your
             class name) is called. If True, sync and db will be ignored.
    """
    if batch:
      cls._write_batch.Delete(key)
    else:
      db = cls._get_db(db)
      db.Delete(cls.key, sync)

  def __eq__(self, other):
    """Check equality. However, this only checks if the key are the same and
    not the content. If the content is different and the key is the same this
    will return True.

    Args:
      other: The other document

    Returns:
      True if the two document's key are the same, False otherwise.
    """
    if isinstance(other, Document):
      return self.key == other.key

    return False

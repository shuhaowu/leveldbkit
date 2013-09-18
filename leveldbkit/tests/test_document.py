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

from __future__ import absolute_import

import unittest
import os.path

from ..properties import *
from ..document import Document, EmDocument
from ..exceptions import NotFoundError

import json
import leveldb

test_dir = os.path.dirname(os.path.abspath(__file__))
test_db = leveldb.LevelDB("{0}/test.db".format(test_dir))

class SimpleDocument(Document):
  db = test_db
  s = StringProperty()
  i = NumberProperty()
  l = ListProperty()
  sr = StringProperty(required=True)
  sv = StringProperty(validators=lambda v: v == "valid")
  sd = StringProperty(default="default")

class SomeDocument(Document):
  db = leveldb.LevelDB("{0}/test1.db".format(test_dir))
  indexdb = leveldb.LevelDB("{0}/test_index.db".format(test_dir))

  test_str_index = StringProperty(index=True)
  test_number_index = NumberProperty(index=True)
  test_list_index = ListProperty(index=True)

class DocumentWithRef(Document):
  db = leveldb.LevelDB("{0}/test2.db".format(test_dir))

  ref = ReferenceProperty(SomeDocument)

class DocumentDbOnDemand(Document):
  db = "{0}/test3.db".format(test_dir)
  OPEN_ONLY_WHEN_NEEDED = True

  test = StringProperty()

class DocumentLater(Document):
  db = "{0}/test3.db".format(test_dir)

class Mixin(EmDocument):
  test = StringProperty(validators=lambda v: v == "test")
  test_index = StringProperty(index=True)

class DocumentWithMixin(Document, Mixin):
  db = leveldb.LevelDB("{0}/mixin.db".format(test_dir))
  indexdb = leveldb.LevelDB("{0}/test_mixin_index.db".format(test_dir))

class BasicDocumentTest(unittest.TestCase):
  def setUp(self):
    if not hasattr(self, "cleanups"):
      self.cleanups = []

  def tearDown(self):
    for doc in self.cleanups:
      doc.delete()

  def test_save_get_delete(self):
    doc = SimpleDocument()
    doc.s = "mrrow"
    doc.i = 1337
    doc.l = ["123123", 123123]
    doc.sr = "lolwut"
    doc.sv = "valid"
    doc.save()
    self.cleanups.append(doc)

    doc2 = SimpleDocument.get(doc.key)
    self.assertEquals(doc.s, doc2.s)
    self.assertEquals(doc.i, doc2.i)
    self.assertEquals(doc.l, doc2.l)
    self.assertEquals(doc.sr, doc2.sr)
    self.assertEquals(doc.sv, doc2.sv)
    self.assertEquals("default", doc2.sd)

    doc.delete()
    with self.assertRaises(NotFoundError):
      doc2.reload()

  def test_batch(self):
    doc = SomeDocument()
    doc.lol = "Yay"
    doc.save(batch=True)
    with self.assertRaises(NotFoundError):
      SomeDocument.get(doc.key)

    SomeDocument.flush()
    self.cleanups.append(doc)

    samedoc = SomeDocument.get(doc.key)
    self.assertEquals(doc.key, samedoc.key)
    self.assertEquals("Yay", doc.lol)

    doc.delete(batch=True)

    docagain = SomeDocument.get(doc.key)
    self.assertEquals("Yay", docagain.lol)

    SomeDocument.flush()

    with self.assertRaises(NotFoundError):
      SomeDocument.get(doc.key)

  def test_document_mixin_indexes_inheritance(self):
    doc = DocumentWithMixin()
    doc.test = "test"
    doc.test_index = "a"
    doc.save()
    self.cleanups.append(doc)
    keys = DocumentWithMixin.index_keys_only("test_index", "a")
    self.assertEquals(1, len(keys))
    self.assertEquals(doc.key, keys[0])

  def test_document_mixin(self):
    doc = DocumentWithMixin()
    doc.test = "test"
    self.assertTrue(doc.is_valid())
    doc.test = "lols"
    self.assertFalse(doc.is_valid())
    doc.test = "test"
    doc.save()
    self.cleanups.append(doc)

    doc2 = DocumentWithMixin.get(doc.key)
    self.assertEqual("test", doc2.test)

  def test_reference_document(self):
    doc = SomeDocument()
    doc.save()
    self.cleanups.append(doc)

    doc2 = DocumentWithRef()
    doc2.ref = doc
    doc2.save()
    self.cleanups.append(doc2)

    doc2_copy = DocumentWithRef.get(doc2.key)
    self.assertEquals(doc2.key, doc2_copy.key)
    self.assertEquals(doc.key, doc2_copy.ref.key)

  def test_2i_save_delete(self):
    doc = SomeDocument()
    doc.test_str_index = "meow"
    doc.test_number_index = 1337
    doc.test_list_index = ["hello", "world", 123]
    doc.save()
    self.cleanups.append(doc)

    def _test_keys_only(self, doc, field, value):
      keys = SomeDocument.index_keys_only(field, value)

      if doc:
        self.assertEquals(1, len(keys))
        self.assertEquals(doc.key, keys[0])
      else:
        self.assertEquals(0, len(keys))

    _test_keys_only(self, doc, "test_list_index", "hello")
    _test_keys_only(self, doc, "test_list_index", "world")
    _test_keys_only(self, doc, "test_list_index", 123)
    _test_keys_only(self, doc, "test_str_index", "meow")
    _test_keys_only(self, doc, "test_number_index", 1337)

    doc.test_str_index = "quack"
    doc.test_number_index = 1336
    doc.test_list_index = ["hello", "wut"]
    doc.save()

    _test_keys_only(self, doc, "test_str_index", "quack")
    _test_keys_only(self, None, "test_str_index", "meow")
    _test_keys_only(self, doc, "test_number_index", 1336)
    _test_keys_only(self, None, "test_number_index", 1337)
    _test_keys_only(self, doc, "test_list_index", "hello")
    _test_keys_only(self, doc, "test_list_index", "wut")
    _test_keys_only(self, None, "test_list_index", "world")
    _test_keys_only(self, None, "test_list_index", 123)

    # This is really bad in practise. Divergent copies are bad.
    samedoc = SomeDocument.get(doc.key)
    del samedoc.test_list_index
    del samedoc.test_number_index
    samedoc.save()

    _test_keys_only(self, None, "test_list_index", "hello")
    _test_keys_only(self, None, "test_list_index", "wut")
    _test_keys_only(self, None, "test_number_index", 1336)

    samedoc.delete()

    _test_keys_only(self, None, "test_str_index", "quack")
    _test_keys_only(self, None, "test_number_index", 1336)

  def test_2i_data_integrity(self):
    doc = SomeDocument()
    doc.test_str_index = "yay"
    doc.save()
    self.cleanups.append(doc)

    a = doc.indexdb.Get("{}~{}".format("test_str_index", "yay"))
    a = json.loads(a)
    self.assertEquals(1, len(a))
    self.assertEquals(doc.key, a[0])

    another_doc = SomeDocument()
    another_doc.test_str_index = "yay"
    another_doc.save()
    self.cleanups.append(another_doc)

    a = doc.indexdb.Get("{}~{}".format("test_str_index", "yay"))
    a = json.loads(a)
    self.assertEquals(2, len(a))
    self.assertTrue(doc.key in a)
    self.assertTrue(another_doc.key in a)

    # This better not shift the length to 3. heh.
    doc.save()

    a = doc.indexdb.Get("{}~{}".format("test_str_index", "yay"))
    a = json.loads(a)
    self.assertEquals(2, len(a))
    self.assertTrue(doc.key in a)
    self.assertTrue(another_doc.key in a)

  def test_2i_iterator(self):
    doc = SomeDocument()
    doc.test_str_index = "meow"
    doc.test_number_index = 1337
    doc.test_list_index = ["hello", "world", 123]
    doc.save()
    self.cleanups.append(doc)

    counter = 0
    for d in SomeDocument.index("test_str_index", "meow"):
      counter += 1
      self.assertEquals(doc.key, d.key)

    self.assertEquals(1, counter)

    counter = 0
    for d in SomeDocument.index("test_list_index", "hello", "world2"):
      counter += 1
      self.assertEquals(doc.key, d.key)

    self.assertEquals(2, counter)

  def test_db_load_ondemand(self):
    doc = DocumentDbOnDemand()
    db = leveldb.LevelDB(DocumentDbOnDemand.db)
    del db
    doc.save()
    self.cleanups.append(doc)
    db = leveldb.LevelDB(DocumentDbOnDemand.db)
    del db
    doc.test = "meow"
    doc.save()
    db = leveldb.LevelDB(DocumentDbOnDemand.db)
    del db

  def test_establish_db_connection_later(self):
    DocumentLater.establish_connection()
    self.assertTrue(isinstance(DocumentLater.db, leveldb.LevelDB))

  def test_2i_batch(self):
    doc = SomeDocument()
    doc.test_number_index = 12

    doc.save(batch=True)
    with self.assertRaises(KeyError):
      SomeDocument.indexdb.Get("test_number_index~12.0")

    SomeDocument.flush()
    self.cleanups.append(doc)

    v = SomeDocument.indexdb.Get("test_number_index~12.0")
    self.assertTrue(v)
    v = json.loads(v)
    self.assertEquals(1, len(v))
    self.assertEquals(doc.key, v[0])

  def test_serialize_expand(self):
    doc = DocumentWithRef()
    doc.ref = SomeDocument()

    serialized = doc.serialize(expand=[{}])
    self.assertTrue(isinstance(serialized["ref"], dict))

  def test_equal(self):
    doc = SomeDocument("test")
    doc_same = SomeDocument("test")
    self.assertTrue(doc == doc_same)

    doc2 = SomeDocument(key="test1")
    self.assertFalse(doc == doc2)

  def test_index_keys(self):
    SomeDocument("1").save()
    SomeDocument("2").save()
    SomeDocument("3").save()
    SomeDocument("4").save()
    SomeDocument("5").save()

    keys = SomeDocument.index_keys_only("$key", "2", "4")
    self.assertEquals(3, len(keys))
    self.assertEquals("2", keys[0])
    self.assertEquals("3", keys[1])
    self.assertEquals("4", keys[2])

    keys = SomeDocument.index("$key", "1", "4")
    i = 1
    for doc in keys:
      self.assertEqual(str(i), doc.key)
      i += 1

    self.assertEquals(5, i)

  def test_set_key(self):
    doc = SomeDocument()
    k = doc.key
    doc.key = "hello"
    self.assertNotEquals(k, doc.key)
    self.assertEquals("hello", doc.key)

  def test_index_buckets(self):
    SomeDocument("1").save()
    SomeDocument("2").save()
    SomeDocument("3").save()
    SomeDocument("4").save()
    SomeDocument("5").save()

    # since we have no buckets, we need to pass in a value. None will do.
    keys = SomeDocument.index_keys_only("$bucket", None)
    self.assertEquals(5, len(keys))

    self.assertEqual("1", keys[0])
    self.assertEqual("2", keys[1])
    self.assertEqual("3", keys[2])
    self.assertEqual("4", keys[3])
    self.assertEqual("5", keys[4])

    i = 1
    for doc in SomeDocument.index("$bucket", None):
      self.assertEqual(str(i), doc.key)
      i += 1

    self.assertEquals(6, i)

if __name__ == "__main__":
  unittest.main()

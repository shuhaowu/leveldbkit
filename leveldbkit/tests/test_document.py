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
from ..document import Document
from ..exceptions import NotFoundError

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
    pass

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

if __name__ == "__main__":
  unittest.main()
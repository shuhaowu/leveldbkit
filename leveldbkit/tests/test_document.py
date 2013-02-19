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
  index_dbs = {"test_field": leveldb.LevelDB("{0}/test_index.db".format(test_dir))}

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

    doc2 = DocumentWithRef()
    doc2.ref = doc
    doc2.save()

    doc2_copy = DocumentWithRef.get(doc2.key)
    self.assertEquals(doc2.key, doc2_copy.key)
    self.assertEquals(doc.key, doc2_copy.ref.key)

    self.cleanups.append(doc)
    self.cleanups.append(doc2)

  def test_2i(self):
    doc = SomeDocument()
    self.assertEquals(set(), doc.indexes())
    doc.add_index("test_field", "mrrowl")
    self.assertEquals(set((("test_field", "mrrowl"),)), doc.indexes())
    self.assertEquals(["mrrowl"], doc.indexes("test_field"))

    doc.save()

    doc2 = SomeDocument.get(doc.key)
    self.assertEquals(set((("test_field", "mrrowl"),)), doc2.indexes())
    self.assertEquals(["mrrowl"], doc2.indexes("test_field"))

    counter = 0
    for d in SomeDocument.index_lookup("test_field", "mrrowl"):
      self.assertEquals(doc2.key, d.key)
      counter += 1

    self.assertEquals(1, counter)

    doc2.remove_index("test_field", "mrrowl")
    doc2.save()

    self.assertEquals(set(), doc2.indexes())

    with self.assertRaises(KeyError):
      SomeDocument.index_dbs["test_field"].Get("test_field")

    doc2.reload()

    self.assertEquals(set(), doc2.indexes())

    self.cleanups.append(doc)


if __name__ == "__main__":
  unittest.main()
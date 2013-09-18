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

"""Top level leveldbkit module"""
from __future__ import absolute_import

from .document import EmDocument, Document
from .exceptions import *
from .properties.standard import BaseProperty, BooleanProperty, DictProperty, EmDocumentProperty, EmDocumentsListProperty, ListProperty, NumberProperty, ReferenceProperty, StringProperty, Property
from .properties.fancy import EnumProperty, DateTimeProperty, PasswordProperty

# PEP 386 versioning
VERSION = (0, 1, 3, "b")
__version__ = ('.'.join(map(str, VERSION[:3])) + '.'.join(VERSION[3:]))
__author__ = "Shuhao Wu"
__url__ = "https://github.com/shuhaowu/leveldbkit"

from __future__ import unicode_literals, absolute_import

import unittest
from io import BytesIO
from origins import io


b = BytesIO(b'''
id,uuid,label,type,description,parent,start,end,directed,dependence,properties
chinook,,Chinook,Database,,,,,,,
artist,,Artist,Table,,chinook,,,,,
artist/artistid,,Artist Id,Column,,artist,,,,,"primary_key=true, data_type=integer"
artist/name,,Name,Column,,artist,,,,,
album,,Album,Table,,chinook,,,,,
album/artistid,,Artist Id,Column,,album,,,,,
album/artistid:artist/artistid,,Foreign Key,foreignkey,,,album/artistid,artist/artistid,true,false,
postgres_artist,9d1a4c55-ff7c-4edb-9276-7d1dc393083b,,,,,,,,,
postgres_sqlite_artist,,Semantic Link,semlink,,,artist,postgres_artist,false,true,similarity=1.0
mysql_sqlite_artist,,Semantic Link,semlink,,,artist,1767f433-13bb-4b37-be2b-03d76e587937,false,true,similarity=1.0
'''.strip())  # noqa


class CsvImporter(unittest.TestCase):
    def test(self):
        data = io.csv.convert(b, {
            'id': 'test',
        })

        self.assertEqual(len(data['components']), 7)
        self.assertEqual(len(data['relationships']), 3)

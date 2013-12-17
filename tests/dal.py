import unittest
from origins.dal import recordtuple


class RecordTupleTestCase(unittest.TestCase):
    def test_new(self):
        r0 = recordtuple()
        self.assertFalse(all([r0._len, r0._fields, r0._fieldmap]))

        r2 = recordtuple(['f1', 'f2'])
        self.assertEqual(r2._len, 2)
        self.assertEqual(r2._fields, ('f1', 'f2'))
        self.assertEqual(r2._fieldmap, {'f1': 0, 'f2': 1})

    def test_len(self):
        r = recordtuple()
        self.assertRaises(TypeError, r, 1)

    def test_map(self):
        r = recordtuple(['f1', 'f2'])
        self.assertEqual(r(1, 2)['f1'], 1)
        self.assertEqual(r(1, 2)[0], 1)

    def test_dict(self):
        r = recordtuple(['f1', 'f2'])
        self.assertEqual(dict(r(1, 2).__dict__), {'f1': 1, 'f2': 2})

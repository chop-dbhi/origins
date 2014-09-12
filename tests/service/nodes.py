from .base import ServiceTestCase


class NodesTestCase(ServiceTestCase):
    path = '/nodes/'

    def test_get(self):
        r, d = self.get()

        self.assertEqual(d, [])
        self.assertEqual(r.status_code, 200)

    def test_get_search(self):
        _, ben = self.post(data={'label': 'Ben'})
        _, bella = self.post(data={'label': 'Bella'})
        _, jen = self.post(data={'label': 'Jen'})

        r, d = self.get(params={'query': 'be'})

        self.assertEqual(r.status_code, 200)
        self.assertCountEqual(d, [ben, bella])

        r, d = self.get(params={'query': 'en'})

        self.assertEqual(r.status_code, 200)
        self.assertCountEqual(d, [ben, jen])

    def test_post(self):
        r, n = self.post(data={})

        self.assertEqual(r.status_code, 201)
        self.assertIn('uuid', n)

        _, d = self.get()

        self.assertEqual(d, [n])


class NodeTestCase(ServiceTestCase):
    path = '/nodes/{uuid}/'

    def setUp(self):
        super(NodeTestCase, self).setUp()

        _, self.d = self.post('/nodes/', data={})

    def test_get(self):
        r, d = self.get({
            'uuid': self.d['uuid'],
        })

        self.assertEqual(r.status_code, 200)
        self.assertEqual(d, self.d)

    def test_put(self):
        r, d = self.put({
            'uuid': self.d['uuid'],
        }, data={'label': 'Test'})

        self.assertEqual(r.status_code, 200)
        self.assertEqual(d['label'], 'Test')

    def test_delete(self):
        r, d = self.delete({
            'uuid': self.d['uuid'],
        })

        self.assertEqual(r.status_code, 204)

        # Still accessible directly
        r, d = self.get({'uuid': self.d['uuid']})
        self.assertEqual(r.status_code, 200)
        self.assertIsNotNone(d['invalidation'])

        # Not in the set
        _, d = self.get('/nodes/')
        self.assertEqual(d, [])

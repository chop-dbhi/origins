from .base import ServiceTestCase


class EdgesTestCase(ServiceTestCase):
    path = '/edges/'

    def setUp(self):
        super(EdgesTestCase, self).setUp()

        _, self.a = self.post('/nodes/', data={})
        _, self.b = self.post('/nodes/', data={})

    def test_get(self):
        r, d = self.get()

        self.assertEqual(d, [])
        self.assertEqual(r.status_code, 200)

    def test_post(self):
        r, n = self.post(data={
            'start': self.a['uuid'],
            'end': self.b['uuid'],
        })

        self.assertEqual(r.status_code, 201)
        self.assertIn('uuid', n)

        _, d = self.get()

        self.assertEqual(d, [n])


class EdgeTestCase(ServiceTestCase):
    path = '/edges/{uuid}/'

    def setUp(self):
        super(EdgeTestCase, self).setUp()

        _, self.a = self.post('/nodes/', data={})
        _, self.b = self.post('/nodes/', data={})

        _, self.d = self.post('/edges/', data={
            'start': self.a['uuid'],
            'end': self.b['uuid'],
        })

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
        _, d = self.get('/edges/')
        self.assertEqual(d, [])

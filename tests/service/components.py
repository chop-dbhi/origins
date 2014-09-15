from .base import ServiceTestCase


class ComponentsTestCase(ServiceTestCase):
    path = '/components/'

    def test_get(self):
        r, d = self.get()

        self.assertEqual(r.status_code, 200)
        self.assertEqual(d, [])

    def test_post(self):
        # Managing resource
        _, r = self.post('/resources/')

        r, d = self.post(data={
            'resource': r['uuid'],
        })

        self.assertEqual(r.status_code, 201)
        self.assertIn('uuid', d)


class ComponentTestCase(ServiceTestCase):
    path = '/components/{uuid}/'

    def setUp(self):
        super(ComponentTestCase, self).setUp()

        # Managing resource
        _, r = self.post('/resources/')

        _, self.d = self.post('/components/', data={
            'resource': r['uuid'],
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

        r, d = self.get({'uuid': self.d['uuid']})
        self.assertEqual(r.status_code, 200)

        # Not visible in the set
        _, d = self.get('/components/')
        self.assertEqual(d, [])

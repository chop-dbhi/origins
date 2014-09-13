from .base import ServiceTestCase


class RelationshipsTestCase(ServiceTestCase):
    path = '/relationships/'

    def test_get(self):
        r, d = self.get()

        self.assertEqual(r.status_code, 200)
        self.assertEqual(d, [])

    def test_post(self):
        # Managing resource
        _, r = self.post('/resources/', data={})

        # Two components to relate
        _, x = self.post('/components/', data={
            'resource': r['uuid'],
        })

        _, y = self.post('/components/', data={
            'resource': r['uuid'],
        })

        r, d = self.post(data={
            'resource': r['uuid'],
            'start': x['uuid'],
            'end': y['uuid'],
        })

        self.assertEqual(r.status_code, 201)
        self.assertIn('uuid', d)


class RelationshipTestCase(ServiceTestCase):
    path = '/relationships/{uuid}/'

    def setUp(self):
        super(RelationshipTestCase, self).setUp()

        # Managing resource
        _, r = self.post('/resources/', data={})

        # Two components to relate
        _, x = self.post('/components/', data={
            'resource': r['uuid'],
        })

        _, y = self.post('/components/', data={
            'resource': r['uuid'],
        })

        _, self.d = self.post('/relationships/', data={
            'resource': r['uuid'],
            'start': x['uuid'],
            'end': y['uuid'],
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
        _, d = self.get('/relationships/')
        self.assertEqual(d, [])

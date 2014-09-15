from .base import ServiceTestCase


class CollectionsTestCase(ServiceTestCase):
    path = '/collections/'

    def test_get(self):
        r, d = self.get()

        self.assertEqual(r.status_code, 200)
        self.assertEqual(d, [])

    def test_post(self):
        r, d = self.post(data={
            'id': 'foo',
            'properties': {
                'bar': 1,
            },
        })

        self.assertEqual(r.status_code, 201)

        self.assertIn('uuid', d)
        self.assertEqual(d['id'], 'foo')
        self.assertEqual(d['properties'], {'bar': 1})

        # Ensure it shows up the GET
        r, d = self.get()
        self.assertEqual(len(d), 1)

    def test_post_type(self):
        _, r = self.post(data={
            'type': 'Foo',
        })

        # Get all collections
        _, d = self.get()
        self.assertEqual(d, [r])

        # Get collections by type
        _, d = self.get(params={'type': 'Foo'})
        self.assertEqual(d, [r])

        _, d = self.get(params={'type': 'Bar'})
        self.assertEqual(d, [])


class CollectionTestCase(ServiceTestCase):
    path = '/collections/{uuid}/'

    def setUp(self):
        super(CollectionTestCase, self).setUp()

        _, self.d = self.post('/collections/')

    def test_get(self):
        r, d = self.get({
            'uuid': self.d['uuid'],
        })

        self.assertEqual(r.status_code, 200)
        self.assertEqual(d, self.d)

    def test_put(self):
        r, d = self.put({
            'uuid': self.d['uuid'],
        }, data={'type': 'Test'})

        self.assertEqual(r.status_code, 200)
        self.assertEqual(d['type'], 'Test')

    def test_delete(self):
        r, d = self.delete({
            'uuid': self.d['uuid'],
        })

        self.assertEqual(r.status_code, 204)

        # Still visible directly
        r, d = self.get({'uuid': self.d['uuid']})
        self.assertEqual(r.status_code, 200)
        self.assertIsNotNone(d['invalidation'])

        # Not visible in the set
        _, d = self.get('/collections/')
        self.assertEqual(d, [])


class CollectionResourcesTestCase(ServiceTestCase):
    path = '/collections/{uuid}/resources/'

    def setUp(self):
        super(CollectionResourcesTestCase, self).setUp()

        _, self.d = self.post('/collections/')
        _, self.r = self.post('/resources/')
        self.post({'uuid': self.d['uuid']}, data={
            'resource': self.r['uuid'],
        })

    def test_get(self):
        r, d = self.get({
            'uuid': self.d['uuid'],
        })

        self.assertEqual(r.status_code, 200)
        self.assertEqual(d, [self.r])

    def test_get_404(self):
        r, d = self.get({
            'uuid': 'abc123',
        })

        self.assertEqual(r.status_code, 404)

    def test_post(self):
        r, d = self.post({
            'uuid': self.d['uuid'],
        }, data={'resource': self.r['uuid']})

        self.assertEqual(r.status_code, 204)

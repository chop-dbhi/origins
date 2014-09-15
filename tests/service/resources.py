from .base import ServiceTestCase


class ResourcesTestCase(ServiceTestCase):
    path = '/resources/'

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

        # Get all resources
        _, d = self.get()
        self.assertEqual(d, [r])

        # Get resources by type
        _, d = self.get(params={'type': 'Foo'})
        self.assertEqual(d, [r])

        _, d = self.get(params={'type': 'Bar'})
        self.assertEqual(d, [])


class ResourceTestCase(ServiceTestCase):
    path = '/resources/{uuid}/'

    def setUp(self):
        super(ResourceTestCase, self).setUp()

        _, self.d = self.post('/resources/')

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
        _, d = self.get('/resources/')
        self.assertEqual(d, [])


class ResourceComponentsTestCase(ServiceTestCase):
    path = '/resources/{uuid}/components/'

    def setUp(self):
        super(ResourceComponentsTestCase, self).setUp()

        _, self.d = self.post('/resources/')
        _, self.c = self.post({'uuid': self.d['uuid']})

    def test_get(self):
        r, d = self.get({
            'uuid': self.d['uuid'],
        })

        self.assertEqual(r.status_code, 200)
        self.assertEqual(d, [self.c])

    def test_get_404(self):
        r, d = self.get({
            'uuid': 'abc123',
        })

        self.assertEqual(r.status_code, 404)

    def test_post(self):
        r, d = self.post({
            'uuid': self.d['uuid'],
        })

        self.assertEqual(r.status_code, 201)
        self.assertTrue(d)

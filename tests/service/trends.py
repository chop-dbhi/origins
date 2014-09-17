from .base import ServiceTestCase


class TrendsTestCase(ServiceTestCase):
    def test_get(self):
        r, _ = self.get('/trends/')
        self.assertEqual(r.status_code, 200)

        r, _ = self.get('/trends/connected-components/')
        self.assertEqual(r.status_code, 200)

        r, _ = self.get('/trends/used-components/')
        self.assertEqual(r.status_code, 200)

        r, _ = self.get('/trends/connected-resources/')
        self.assertEqual(r.status_code, 200)

        r, _ = self.get('/trends/used-resources/')
        self.assertEqual(r.status_code, 200)

        r, _ = self.get('/trends/component-sources/')
        self.assertEqual(r.status_code, 200)

        r, _ = self.get('/trends/resource-types/')
        self.assertEqual(r.status_code, 200)

        r, _ = self.get('/trends/component-types/')
        self.assertEqual(r.status_code, 200)

        r, _ = self.get('/trends/relationship-types/')
        self.assertEqual(r.status_code, 200)

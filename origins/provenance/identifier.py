class QualifiedName(object):
    def __init__(self, namespace, local):
        self.namespace = namespace
        self.local = local

        if namespace:
            self.uri = '{{{}}}{}'.format(namespace.uri, local)
            self.prefixed = '{}:{}'.format(namespace.prefix, local)
        else:
            self.uri = self.prefixed = local

    def __str__(self):
        return self.prefixed

    def __bytes__(self):
        return self.prefixed.encode('utf-8')

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self.prefixed)

    def __hash__(self):
        return hash(self.prefixed)

    def __eq__(self, other):
        if not isinstance(other, QualifiedName):
            return False
        return self.uri == other.uri

    def __ne__(self, other):
        return not self.__eq__(other)


class UnqualifiedName(QualifiedName):
    "Name that does have a prefix associated with it."
    def __init__(self, name):
        super(UnqualifiedName, self).__init__(None, name)


class UnknownName(QualifiedName):
    "Name that has a prefix, but does not have an associated namespace."
    def __init__(self, name):
        super(UnknownName, self).__init__(None, name)


class AnonymousName(UnknownName):
    "Specialized unknown type for declared anonymous names with a prefix of _."


class Namespace(object):
    def __init__(self, prefix, uri):
        self.prefix = prefix
        self.uri = uri
        self._cache = {}

    def __eq__(self, other):
        if not isinstance(other, Namespace):
            return False
        return self.uri == other.uri

    def __hash__(self):
        return hash((self.uri, self.prefix))

    def __repr__(self):
        return '<{}: {} {{{}}}>'.format(self.__class__.__name__,
                                        self.prefix, self.uri)

    def __getitem__(self, local):
        if local in self._cache:
            return self._cache[local]

        qname = QualifiedName(self, local)
        self._cache[local] = qname
        return qname

    def qname(self, identifier):
        if isinstance(identifier, (str, bytes)):
            uri = identifier
        elif isinstance(identifier, QualifiedName):
            uri = identifier.uri
        else:
            uri = ''

        if uri.startswith(self.uri):
            return QualifiedName(self, uri[len(self.uri):])


class Namespaces(object):
    def __init__(self, namespaces=None):
        self._cache = {}

        if namespaces:
            for prefix in namespaces:
                self.add(prefix, namespaces[prefix])

    def __getitem__(self, prefix):
        return self._cache[prefix]

    def __contains__(self, prefix):
        return prefix in self._cache

    def __iter__(self):
        return iter(self._cache.values())

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__,
                                 repr(self._cache))

    def add(self, prefix, uri):
        if prefix not in self._cache:
            ns = Namespace(prefix, uri)
            self._cache[prefix] = ns

        return self._cache[prefix]

    def extend(self, namespaces=None):
        copy = self.__class__()
        copy._cache = self._cache.copy()

        if namespaces:
            for prefix in namespaces:
                copy.add(prefix, namespaces[prefix])

        return copy

    def qname(self, identifier, default=None):
        # Prefixed identifier, find in cache
        if ':' in identifier:
            prefix, local = identifier.split(':', 1)

            if prefix in self._cache:
                return self._cache[prefix][local]

            if prefix == '_':
                return AnonymousName(local)

            return UnknownName(identifier)

        # Try to get a qualified name from the namespaces
        for ns in self._cache.values():
            qname = ns.qname(identifier)

            if qname:
                return qname

        if default:
            return self._cache[default][identifier]

        return UnqualifiedName(identifier)

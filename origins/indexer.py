from __future__ import unicode_literals, print_function

try:
    str = unicode
except NameError:
    pass


def build(relationships):
    """Builds an index based on the relationships passed in. Relationships
    must have the format:

        ( <source_key>, <target_key>, ... )

    where `source_key` and `target_key` are the *index keys* and will vary
    depending on the requirements of the index. The `...` denotes additional
    values can be contained in the tuple and will be passed through.

    For example, if the relationships are of foreign key references between
    tables in a relational database, the values would be of table names.

        (
            'Album',
            'Artist',
            ...
        )

    The output is a `dict` of *sources* to a `dict` of *targets* based on the
    keys defined.

    The reference stored for a **forward relationship** is the relationship
    itself:

        {
            'Album': {
                'Artist': ('Album', 'Artist', ...),
            }
        }

    (The keys are preserved in the tuple for differentiating forward and
    reverse relationships.)

    A **reverse relationship** is stored as a reference to the index key of
    forward relationship:

        {
            'Artist': {
                'Album': 'Album',
            }
        }

    A **relationship path** is stored as a reference to the next index key in
    the path to the target. For example, if a path `Artist -> Album -> Track`
    exists, the reference between `Artist` and `Track` is `Album`:

        {
            'Artist': {
                'Track': 'Album',
            }
        }

    """
    index = {}

    for rel in relationships:
        source, target = rel[:2]

        index.setdefault(source, {})
        index.setdefault(target, {})

        # Forward reference
        if not index[source].get(target):
            index[source][target] = rel

        # Reverse relationship for target. This is simply
        # a reference to the resource with the foward relationship.
        # Parsers must reverse the foward relationship to get the
        # valid directed relationship.
        if not index[target].get(source):
            index[target][source] = source

    return index


def crawl(index):
    """Given an `index` all sources are crawled for all other targets.
    References to the shortest path are filled in as they are found.
    """
    copy = {}

    for root in index.keys():
        copy[root] = _crawl(index, root)

    return copy


def _crawl(index, root):
    "Crawls paths for a single root."
    copy = index[root].copy()

    # The possible targets are all roots in the index
    possible_targets = set(index)

    # Relationships for this root that already exist in the
    # index are ignored
    existing_targets = set(index[root])

    # The targets to pursue to fill the index
    targets = possible_targets - existing_targets

    for target in targets:
        path = _path_to_target(index, root, target)

        if path:
            copy[target] = path[1]

    return copy


def _path_to_target(index, root, target, ignore=None, length=1):
    "Finds the shortest path to the target from the root."
    # Get the reference to the target locally
    reference = index[root].get(target)

    # Forward relationship
    if isinstance(reference, (list, tuple)):
        return length, target

    # Reverse relationship
    if isinstance(reference, str) and reference == target:
        return length, target

    # Other than with forward relationship, self relationships are not
    # expressed using any other reference.
    if root == target:
        return

    # Ignored targets are usually ones to prevent circular
    # references
    ignored_targets = set(ignore or [])

    # Relationships for this root that already exist in the
    # index are ignored
    existing_targets = set(index[root])

    # The potential 'sources' to the target are the existing
    # targets excluding the ignored ones. These act as the
    # sources for finding a path to the target.
    sources = existing_targets - ignored_targets

    shortest = None

    # Traverse all neighbors and check if a path is found
    for source in sources:
        result = _path_to_target(index, source, target, ignore=[root],
                                 length=length + 1)

        if result and (shortest is None or result[0] < shortest[0]):
            shortest = (result[0], source)

    return shortest


def resolve(index):
    """Given an `index` of references, *resolve it* by building the paths
    from each each source to each target. This enables taking an arbitrary
    source and target element in the index and getting the path between them.
    """
    copy = {}

    for x in index:
        copy[x] = {}

        for y in index:
            path = _resolve_path(index, x, y)

            if path:
                copy[x][y] = tuple(path)

    return copy


def _resolve_path(index, root, target, path=None):
    """Returns the path from the root to the target, resolving the
    intermediate references along the way.
    """
    if path is None:
        path = []

    reference = index[root].get(target)

    # No relationship in index
    if not reference:
        return

    # Forward reference
    if isinstance(reference, (list, tuple)):
        path.append(reference)
        return path

    if isinstance(reference, str):
        # Reverse reference
        if reference == target:
            reference = index[target][root]
            reverse = [reference[1], reference[0]]
            reverse.extend(reference[2:])
            path.append(tuple(reverse))
            return path

        # This implies the path exists through the reference.
        # Resolve the reference relative to the root, then traverse
        # reference
        subpath = _resolve_path(index, root, reference)

        if subpath:
            path.extend(subpath)
            return _resolve_path(index, reference, target, path=path)

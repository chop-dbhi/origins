$(function() {

    var defaultPage = 1,
        defaultLimit = 100,
        pagerLimit = 10,
        defaultKeys = ['label', 'uri'];


    var searchInput = $('#search'),
        searchResults = $('#results'),
        pagerNext = $('#pager-next'),
        pagerPrevious = $('#pager-previous');


    var resultTemplate = _.template('<div class="list-group-item">' +
        '<h4 class="list-group-item-heading" title="<%= data.label %>"><%= data.label %></h4>' +
        '<div class="list-group-item-text" title="<%= data.uri %>"><%= data.uri %></div>' +
        '</div>', null, {variable: 'data'});


    var sendRequest = function(term, page, limit) {
        term = $.trim(term || '');
        page = page || defaultPage;
        limit = limit || defaultLimit;

        var query = 'MATCH (n:Element) ',
            params = {},
            skip = (page - 1) * limit;

        if (term) {
            query += 'WHERE n.label =~ { search } ';
            params.search = '(?i).*' + term + '.*';
        }

        query += 'RETURN n.label, n.uri'; // SKIP ' + skip + ' LIMIT ' + limit;

        return $.ajax({
            url: 'http://localhost:7474/db/data/cypher',
            dataType: 'json',
            type: 'POST',
            data: JSON.stringify({
                query: query,
                params: params
            }),
            contentType: 'application/json',
            success: function(resp) {
                var data;
                searchResults.empty();

                if (resp.data.length) {
                    _.each(resp.data, function(values, i) {
                        data = _.object(_.zip(defaultKeys, values));
                        searchResults.append(resultTemplate(data));
                    });
                } else {
                    searchResults.append(resultTemplate({label: '<em>No results..</em>'}));
                }
            }
        });
    };

    searchInput.on('keyup', _.debounce(function(event) {
        event.preventDefault();
        sendRequest(searchInput.val());
    }, 200));

    // Pre-fill results
    sendRequest();

});

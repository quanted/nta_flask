
def validate_json(request):
    # Ensure a request body was POSTed
    if request is not None:
        try:
            request = json.loads(request)
        except TypeError:
            if type(request) is dict:
                pass
            else:
                return None
    else:
        raise TypeError
    # Ensure the request body has 'search_by' and 'query' keys in the JSON
    try:
        search_by = request['search_by']
    except TypeError:
        raise Exception('The POST request is missing the search_by item')
    try:
        query = request['query']
    except TypeError:
        raise Exception('The POST request is missing the query item')
    return search_by, query

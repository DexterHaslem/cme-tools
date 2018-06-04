def get_network_lines(resp):
    # DictReader needs an iterable of lines, but network response
    # is list of bytes you cant pass directly in, convert to generator of decoded lines
    for line in resp:
        yield line.decode('utf-8')

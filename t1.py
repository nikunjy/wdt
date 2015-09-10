#! /usr/bin/env python

from urlparse import urlparse
from urlparse import parse_qs


def main():
    connection_url = "wdt://testing-worker-linux-docker-cac0bc23-3207-linux-6?ports=34975,58748,42332,57632,44086,47499,49369,56966&protocol=16&id=566563919"
    print(connection_url)
    parse_result = urlparse(connection_url, "wdt", True)
    print(parse_result)
    query_parse_result = parse_qs(parse_result.query)
    port_to_block = query_parse_result['ports'][0].split(',')[0]
    print(port_to_block)

main()

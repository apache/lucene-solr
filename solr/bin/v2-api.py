#!/usr/bin/env python3.6

import argparse
import json

import requests


class _CustomType:
    """Helper class to define custom types for ArgParse"""
    NAME = None  # To be set in subclasses

    @property
    def __name__(self):
        return self.NAME


class _UnknownType(_CustomType):
    """Type for unknown types"""
    def __init__(self, solr_type):
        self.NAME = "solr-" + solr_type

    def __call__(self, string):
        return str(string)


class _ObjectType(_CustomType):
    """Json object encoded in a string"""
    NAME = "json-object"

    def __call__(self, string):
        try:
            value = json.loads(string)
        except json.JSONDecodeError:
            raise argparse.ArgumentTypeError("%r is not valid JSON" % string)
        if type(value) == dict:
            return string
        else:
            raise argparse.ArgumentTypeError("%r is not a JSON object" % string)


# Mapping from solr type representation to ArgParser types
_TYPE_MAPPING = dict(
    integer=int,
    string=str,
    boolean=bool,
    object=_ObjectType(),
    # TODO add others as 'array'
)


def build_parser():
    parser = argparse.ArgumentParser(
        description="""
            CLI tool to interact with Solr Admin REST API.
        """,
        epilog="""
            All options and parameters are generated dynamically.
            Check the Solr Reference guide for further documentation on the API.
        """
    )

    # TODO: we can just set this config for each "sub-url" and add support for cores, etc.
    endpoint_config = {
        "name": "collection",
        "url": "collections",
        "title": "collections",
        "description": "List of commands to manipulate collections",
    }

    subparsers = parser.add_subparsers(
        title=endpoint_config["title"],
        description=endpoint_config["description"],
        dest="command"
    )

    url = f"http://localhost:8983/v2/{endpoint_config['url']}/_introspect"
    response = requests.get(url).json()
    for spec in response['spec']:
        # TODO: support actions on the root (only commands supported atm)
        # TODO: support operations on sub-urls

        for c_name, c_spec in spec.get('commands', {}).items():
            command_help = c_spec["description"] + "\n\n" + c_spec["documentation"]
            if endpoint_config["name"] in c_name:
                full_name = c_name
            else:
                full_name = f"{endpoint_config['name']}:{c_name}"

            subparser = subparsers.add_parser(
                full_name, help=command_help,
                formatter_class=argparse.MetavarTypeHelpFormatter,
            )

            for p_name, p_value in c_spec.get('properties', {}).items():
                solr_prop_type = p_value['type']
                property_type = _TYPE_MAPPING.get(solr_prop_type,
                                                  _UnknownType(solr_prop_type))

                # Add required args as positional
                if p_name in c_spec.get('required', []):
                    subparser.add_argument(p_name, type=property_type,
                                           help=p_value['description'])
                # Optional args are passed as flags
                else:
                    subparser.add_argument('--' + p_name, type=property_type,
                                           help=p_value['description'])

            subparser.add_argument('--http_method', type=str, choices=spec["methods"],
                                   default=spec["methods"][0],
                                   help="HTTP Method to use.")

    return parser


def main():
    parser = build_parser()
    args = vars(parser.parse_args())
    command = args.pop("command")
    if not command:
        parser.print_help()
        return
    method = args.pop("http_method")
    url = 'http://localhost:8983/v2/collections/' + command
    payload = {command: {k:v for k,v in args.items() if v is not None}}
    print(f"curl {url} -X {method} -d '{payload}'")  # We'll need to space things like "

if __name__ == '__main__':
    main()

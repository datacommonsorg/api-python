# Copyright 2022 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from unittest import mock

import datacommons


class TestProperties(unittest.TestCase):

    @mock.patch("datacommons.node._post")
    def test_with_data(self, _post):

        def side_effect(path, data):
            if path == "/v1/bulk/properties/out" and data == {
                    "nodes": ["City"]
            }:
                return {
                    "data": [{
                        "node":
                            "City",
                        "properties": [
                            "name", "provenance", "subClassOf", "typeOf"
                        ]
                    }]
                }

        _post.side_effect = side_effect
        response = datacommons.properties(["City"])
        assert response == {
            "City": ["name", "provenance", "subClassOf", "typeOf"]
        }


class TestPropertyValues(unittest.TestCase):

    @mock.patch("datacommons.node._post")
    def test_with_data(self, _post):

        def side_effect(path, data):
            print(path)
            if path == "/v1/bulk/property/values/out" and data == {
                    "nodes": ["geoId/06"],
                    "property": "name",
            }:
                return {
                    "data": [{
                        "node":
                            "geoId/06",
                        "values": [{
                            "provenanceId": "dc/5n63hr1",
                            "value": "California"
                        }]
                    }]
                }

        _post.side_effect = side_effect
        response = datacommons.property_values(["geoId/06"], "name")
        assert response == {"geoId/06": ["California"]}


class TestTriples(unittest.TestCase):

    @mock.patch("datacommons.node._post")
    def test_with_data(self, _post):

        def side_effect(path, data):
            print(path)
            if path == "/v1/bulk/triples/out" and data == {
                    "nodes": ["Class"],
            }:
                return {
                    "data": [{
                        "node": "Class",
                        "triples": {
                            "typeOf": {
                                "nodes": [{
                                    "name": "Class",
                                    "types": ["Class"],
                                    "dcid": "Class",
                                    "provenanceId": "dc/5l5zxr1"
                                }, {
                                    "name": "Class",
                                    "types": ["Class"],
                                    "dcid": "Class",
                                    "provenanceId": "dc/5l5zxr1"
                                }]
                            },
                            "isPartOf": {
                                "nodes": [{
                                    "provenanceId": "dc/5l5zxr1",
                                    "value": "http://meta.schema.org"
                                }]
                            },
                            "name": {
                                "nodes": [{
                                    "provenanceId": "dc/5l5zxr1",
                                    "value": "Class"
                                }]
                            },
                            "provenance": {
                                "nodes": [{
                                    "name": "BaseSchema",
                                    "types": ["Provenance"],
                                    "dcid": "dc/5l5zxr1",
                                    "provenanceId": "dc/5l5zxr1"
                                }]
                            },
                            "sameAs": {
                                "nodes": [{
                                    "provenanceId":
                                        "dc/5l5zxr1",
                                    "value":
                                        "http://www.w3.org/2000/01/rdf-schema"
                                }]
                            },
                            "subClassOf": {
                                "nodes": [{
                                    "name": "Intangible",
                                    "types": ["Class"],
                                    "dcid": "Intangible",
                                    "provenanceId": "dc/5l5zxr1"
                                }]
                            }
                        }
                    }]
                }

        _post.side_effect = side_effect
        response = datacommons.triples(["Class"])
        assert response == {
            "Class": {
                'isPartOf': [{
                    'provenanceId': 'dc/5l5zxr1',
                    'value': 'http://meta.schema.org'
                }],
                'name': [{
                    'provenanceId': 'dc/5l5zxr1',
                    'value': 'Class'
                }],
                'provenance': [{
                    'dcid': 'dc/5l5zxr1',
                    'name': 'BaseSchema',
                    'provenanceId': 'dc/5l5zxr1',
                    'types': ['Provenance']
                }],
                'sameAs': [{
                    'provenanceId': 'dc/5l5zxr1',
                    'value': 'http://www.w3.org/2000/01/rdf-schema'
                }],
                'subClassOf': [{
                    'dcid': 'Intangible',
                    'name': 'Intangible',
                    'provenanceId': 'dc/5l5zxr1',
                    'types': ['Class']
                }],
                'typeOf': [{
                    'dcid': 'Class',
                    'name': 'Class',
                    'provenanceId': 'dc/5l5zxr1',
                    'types': ['Class']
                }, {
                    'dcid': 'Class',
                    'name': 'Class',
                    'provenanceId': 'dc/5l5zxr1',
                    'types': ['Class']
                }]
            },
        }

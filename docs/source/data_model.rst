Getting Started with the Data Model
===================================

In this tutorial, we will introduce how Data Commons stores data in its open
knowledge graph.

Important Terms
---------------

The following terms are defined in this tutorial.

+-----------------+-----------------------------------------------------------+
| Term            | Description                                               |
+=================+===========================================================+
| Knowledge Graph | The graph structure storing all data in Data Commons.     |
+-----------------+-----------------------------------------------------------+
| Node            | The representation of an entity in the knowledge graph.   |
+-----------------+-----------------------------------------------------------+
| DCID            | The unique identifier assigned to each node in the Data   |
|                 | Commons knowledge graph. Short for Data Commons           |
|                 | Identifier.                                               |
+-----------------+-----------------------------------------------------------+
| Type            | The class describing a node. For example, the node for    |
|                 | "California" has type "State".                            |
+-----------------+-----------------------------------------------------------+
| Property        | The relation that associates two nodes together.          |
+-----------------+-----------------------------------------------------------+
| Property Label  | Another name for *property*.                              |
+-----------------+-----------------------------------------------------------+
| Property Value  | The set of nodes adjacent to a given node along edges of  |
|                 | a given *property*.                                       |
+-----------------+-----------------------------------------------------------+
| Triple          | A compact representation of a statement in the Data       |
|                 | Commons knowledge graph of the form                       |
|                 |                                                           |
|                 | (subject, property, object)                               |
|                 |                                                           |
+-----------------+-----------------------------------------------------------+

Overview of the Graph
---------------------

Data Commons organizes its data as an open access *knowledge graph*. It contains
statements about real world objects such as

- `"Alameda County" <https://browser.datacommons.org/kg?dcid=geoId/06001>`_
  is contained in the State of
  `"California" <https://browser.datacommons.org/kg?dcid=geoId/06>`_
- The latitude of
  `"Berkeley" <https://browser.datacommons.org/kg?dcid=geoId/0606000>`_, CA
  is 37.8703
- The
  `population of all persons in Maryland <https://browser.datacommons.org/kg?dcid=dc/o/6w1c9qk7hxjch>`_
  has a total count of 5,996,080.

`Entities <https://en.wikipedia.org/wiki/Entity>`_ such as "Alameda County",
"California", and "Berkeley" are represented as **nodes** in the Data Commons
knowledge graph. There are two important details about a node

1.  Every node is uniquely identified by a **dcid** which is short for Data
    Commons Identifier. The dcid identifying "California" is :code:`geoId/06`
2.  Every node has a **class** that broadly describes the category of entities
    that it is an instance of. For example, "California" has type
    `State <https://browser.datacommons.org/kg?dcid=State>`_.

Relations between entities are represented as a directed edge between two nodes
in the graph. These relations are called **properties** or **property labels**.
A portion of the Data Commons graph capturing the statement

  "Alameda County is contained in the California"

can thus be visualized as the following:

.. image:: https://storage.googleapis.com/notebook-resources/image-1.png
   :alt: A view of the statement "Alameda County is contained in California"
   :align: center

Here "Alameda County" and "California" are nodes while
`"containedInPlace" <https://browser.datacommons.org/kg?dcid=containedInPlace>`_
is a property denoting that the node adjacent to the tail of the edge is
contained in the node adjacent to the head.

Property Values
+++++++++++++++

When accessing nodes in the graph, it is often useful to describe a set of nodes
adjacent to a given node. One may wish to query for all cities that are
contained in a certain county, ask for all schools within a school district,
etc. Given a node and a property, we denote the **property value** as the set
of all nodes that are adjacent to the given node along an edge labeled by the
given property.

For example, the following are a few property values of "Alameda County"
along the property "containedInPlace".

- `California <https://browser.datacommons.org/kg?dcid=geoId/06>`_
- `Berkeley <https://browser.datacommons.org/kg?dcid=geoId/0606000>`_
- `Oakland <https://browser.datacommons.org/kg?dcid=geoId/0653000>`_
- `Emeryville <https://browser.datacommons.org/kg?dcid=geoId/0622594>`_

The graph around Alameda County looks like the following.

.. image:: https://storage.googleapis.com/notebook-resources/readthedocs-image-2.png
   :alt: A view of the graph around Alameda County
   :align: center

An important thing to note is that direction matters! Berkeley is certainly
contained in Alameda County, but California is *not* contained in Alameda
County. Alameda County is contained in California, but it is *not* contained
in Berkeley!

When asking for property values one may thus wish to distinguish by the
*orientation* or direction of the edge. The property values of "Alameda County"
along *outgoing* edges labeled by "containedInPlace" includes California while
property values along *incoming* edges include Berkeley, Oakland, and
Emeryville.

Triples
+++++++

Relations in the graph can be compactly described in the format of a **triple**.
Triples are 3-tuples that take the form `(subject, property, object)`.

- The *subject* and *object* are two nodes in the Data Commons graph.
- The *property* is the property labeling the edge oriented from subject to
  object.

The statement "Alameda County is contained in the California" can be represented
as a triple of the following form

  ("Alameda County", "containedInPlace", "California")

Indeed, one could represent the entire Data Commons graph as a collection of
triples.

More Information
----------------

Data Commons leverages the `Schema.org <https://schema.org>`_ vocabulary to
provide a common set of types and properties. The Data Model used by Data
Commons also closely resembles the Schema.org data model. One may refer to
documentation on the
`Schema.org data model <https://schema.org/docs/datamodel.html>`_
to learn more.

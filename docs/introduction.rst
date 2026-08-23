Introduction
============

``tiny_pacs`` is a small, pure-Python PACS (Picture Archiving and
Communication System). A PACS is the system hospitals and imaging centres
use to receive, archive and distribute medical images — in DICOM terms, a
network service that other modalities and workstations talk to.

``tiny_pacs`` implements a DICOM Service Class Provider (SCP) for the most
common services:

* Verification (C-ECHO)
* Storage (C-STORE)
* Query/Retrieve: C-FIND, C-MOVE and C-GET at the PATIENT, STUDY, SERIES
  and IMAGE levels
* Storage Commitment

Supported DICOM services are provided out of the box; the goal of the
project is a small, readable and easy-to-extend PACS rather than an
exhaustive implementation of the standard.

Architecture
------------

Instead of a single monolith, the server is composed of *components* that
talk to each other through a
`trolleybus <https://pypi.org/project/trolleybus/>`_ event bus. Incoming
DICOM requests are turned into events; components subscribe to the events
they care about and react to them. DICOM networking is provided by
`pynetdicom2 <https://github.com/blanebf/pynetdicom2>`_, dataset handling
by `pydicom <https://pydicom.github.io/>`_ and persistence by
`peewee <https://github.com/coleifer/peewee>`_.

The built-in components are:

.. list-table::
   :header-rows: 1

   * - Component
     - Description
   * - ``Database``
     - Manages the database connection (SQLite by default, or PostgreSQL),
       transactions and table creation. On startup it collects the models of
       all components and creates their tables.
   * - ``Devices``
     - Registry of remote DICOM devices by AE title, used as C-MOVE
       destinations and for outgoing connections. When ``auto_add`` is
       enabled, calling AE titles are registered automatically.
   * - ``PACS``
     - The PACS services themselves: handling of C-STORE, C-FIND, C-MOVE,
       C-GET and Storage Commitment requests on top of the
       ``Patient``/``Study``/``Series``/``Instance`` database models.
   * - ``FileStorage``
     - Stores incoming datasets on disk: one file per SOP Instance in daily
       sub-folders of ``storage_dir``.
   * - ``InMemoryStorage``
     - Keeps incoming datasets in RAM. Intended for testing.
   * - ``TempFileStorage``
     - Stores incoming datasets in temporary files and removes them on
       shutdown. Intended for testing.

Enable exactly one storage component — all storage components react to the
same events, so enabling more than one stores every dataset multiple times.

Components are configured through YAML or JSON files and described with
`pydantic <https://docs.pydantic.dev>`_ models, so configuration is
validated at load time. Each component supplies its own config model, which
is what makes components independently configurable, replaceable and
extensible (see :doc:`tutorial/extending`).

Requirements
------------

Python ``>= 3.10``. Runtime dependencies: ``pydicom`` 3.x, ``pynetdicom2``
0.9.x, ``peewee`` 4.x, ``PyYAML`` 6.x, ``trolleybus`` 0.2 and ``pydantic``
2.x.

For the PostgreSQL backend install a driver separately, e.g.
``pip install psycopg2-binary`` — see :doc:`tutorial/installation`.

Where to go next
----------------

* :doc:`tutorial/installation` — how to install ``tiny_pacs``
* :doc:`tutorial/configuration` — how to configure the server
* :doc:`tutorial/running` — how to start the server and talk to it
* :doc:`tutorial/extending` — how to add your own components
* :doc:`api/index` — the API reference

tiny_pacs
=========

.. image:: https://github.com/blanebf/tiny_pacs/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/blanebf/tiny_pacs/actions/workflows/ci.yml
   :alt: CI

``tiny_pacs`` is a small, pure-Python PACS (Picture Archiving and Communication
System). It implements a DICOM Service Class Provider (SCP) for the most common
services:

* Verification (C-ECHO)
* Storage (C-STORE)
* Query/Retrieve: C-FIND, C-MOVE and C-GET at the PATIENT, STUDY, SERIES and
  IMAGE levels
* Storage Commitment

Instead of a single monolith, the server is composed of *components* that talk
to each other through a `trolleybus <https://pypi.org/project/trolleybus/>`_
event bus. DICOM networking is provided by
`pynetdicom2 <https://github.com/blanebf/pynetdicom2>`_, dataset handling by
`pydicom <https://pydicom.github.io/>`_ and persistence by
`peewee <https://github.com/coleifer/peewee>`_.

Features
--------

* Modular design: database, device registry, PACS logic and storage are
  individual components that can be configured, replaced or extended
  independently
* SQLite (file or in-memory, the default) and PostgreSQL backends with
  connection pooling
* Storage backends: on-disk files, in-memory datasets or temporary files
* Interactive configuration wizard
* YAML or JSON configuration files

Requirements
------------

Python ``>= 3.10``. Runtime dependencies: ``pydicom`` 3.x, ``pynetdicom2``
0.9.x, ``peewee`` 4.x, ``PyYAML`` 6.x and ``trolleybus`` 0.2.

For the PostgreSQL backend install a driver separately, e.g.
``pip install psycopg2-binary``.

Installation
------------

From the git repository:

.. code-block:: bash

    pip install git+https://github.com/blanebf/tiny_pacs.git

For development, with `Poetry <https://python-poetry.org/>`_:

.. code-block:: bash

    git clone git@github.com:blanebf/tiny_pacs.git
    cd tiny_pacs
    poetry install

Quick start
-----------

Start the server with the built-in defaults — AE title ``TINY_PACS``, port
``11112``, an in-memory SQLite database and in-memory storage:

.. code-block:: bash

    tiny-pacs

Override the AE title and/or the port from the command line:

.. code-block:: bash

    tiny-pacs -a MY_PACS -p 4242

Load configuration from a file (YAML by extension, JSON for ``*.json``):

.. code-block:: bash

    tiny-pacs -c config.yaml

Or configure everything interactively; the wizard offers to save the resulting
configuration to a file before starting the server:

.. code-block:: bash

    tiny-pacs -i

Configuration
-------------

A configuration file has three optional top-level sections: ``ae``, ``log``
and ``components``. Anything that is not provided falls back to the built-in
defaults.

``ae``
~~~~~~

Application entity settings:

* ``ae_title`` — a list of AE titles the server answers to
  (default: ``['TINY_PACS']``)
* ``port`` — the SCP TCP port (default: ``11112``)
* ``max_pdu_length`` — maximum PDU length in bytes (default: ``65536``)
* ``dump_ds`` — dump datasets and PDUs to the log (default: ``true``)
* ``supported_ts`` — a list of supported transfer syntax UIDs (default: the
  common uncompressed, JPEG, JPEG-LS, JPEG 2000, MPEG and RLE syntaxes)

``log``
~~~~~~~

A standard Python ``logging.config.dictConfig`` schema. The default
configuration installs a console handler with DEBUG level:

.. code-block:: yaml

    log:
      version: 1
      formatters:
        simple:
          format: '%(asctime)s - %(levelname)-8s - %(name)-15s - %(message)s'
      handlers:
        console:
          class: logging.StreamHandler
          level: DEBUG
          formatter: simple
          stream: ext://sys.stdout
      root:
        level: DEBUG
        handlers: [console]

``components``
~~~~~~~~~~~~~~

A mapping of component name (see the `component registry`_) to its
configuration. Every component accepts an ``on`` flag and is skipped unless it
is set to ``true``. When no components are configured at all, the default set
is used: ``Database``, ``Devices``, ``PACS`` and ``InMemoryStorage``.

Example
~~~~~~~

.. code-block:: yaml

    ae:
      ae_title: [TINY_PACS]
      port: 11112
      max_pdu_length: 65536
      dump_ds: false
    log:
      version: 1
      formatters:
        simple:
          format: '%(asctime)s - %(levelname)-8s - %(name)-15s - %(message)s'
      handlers:
        console:
          class: logging.StreamHandler
          level: INFO
          formatter: simple
          stream: ext://sys.stdout
      root:
        level: INFO
        handlers: [console]
    components:
      Database:
        on: true
        driver: sqlite            # or postgres
        db_name: pacs.db          # SQLite database name, default 'pacs.db'
        mode: rwc                 # 'rwc' persists to the db_name file; the
                                  # default 'memory' keeps data in RAM only
      Devices:
        on: true
        auto_add: true            # register calling AE titles automatically
        default_port: 11113       # port used for auto-added devices
        devices:
          WORKSTATION:
            aet: WORKSTATION
            address: 192.168.1.10
            port: 11113
      PACS:
        on: true
      FileStorage:
        on: true
        storage_dir: /var/lib/tiny_pacs/storage

The PostgreSQL driver takes connection parameters instead:

.. code-block:: yaml

    components:
      Database:
        on: true
        driver: postgres
        db_name: tiny_pacs_db
        host: localhost
        port: 5432
        user: postgres
        password: postgres
        max_conn: 20

Component registry
------------------

+---------------------+-----------------------------------------------------------+
| Component           | Description                                               |
+=====================+===========================================================+
| ``Database``        | Manages the database connection (SQLite by default, or    |
|                     | PostgreSQL), transactions and table creation. On startup  |
|                     | it collects the models of all components and creates      |
|                     | their tables.                                             |
+---------------------+-----------------------------------------------------------+
| ``Devices``         | Registry of remote DICOM devices by AE title, used as     |
|                     | C-MOVE destinations and for outgoing connections. When    |
|                     | ``auto_add`` is enabled, calling AE titles are registered |
|                     | automatically with ``default_port``.                      |
+---------------------+-----------------------------------------------------------+
| ``PACS``            | The PACS services themselves: handling of C-STORE,        |
|                     | C-FIND, C-MOVE, C-GET and Storage Commitment requests on  |
|                     | top of the ``Patient``/``Study``/``Series``/``Instance``  |
|                     | database models.                                          |
+---------------------+-----------------------------------------------------------+
| ``FileStorage``     | Stores incoming datasets on disk: one file per SOP        |
|                     | Instance in daily (``YYYYMMDD``) sub-folders of           |
|                     | ``storage_dir``. If no directory is configured, a         |
|                     | temporary one is created and removed on shutdown.         |
+---------------------+-----------------------------------------------------------+
| ``InMemoryStorage`` | Keeps incoming datasets in RAM. Intended for testing.     |
+---------------------+-----------------------------------------------------------+
| ``TempFileStorage`` | Stores incoming datasets in temporary files and removes   |
|                     | them on shutdown. Intended for testing.                   |
+---------------------+-----------------------------------------------------------+

Enable exactly one storage component — all storage components react to the
same events, so enabling more than one stores every dataset multiple times.

Talking to tiny_pacs: command line examples
-------------------------------------------

The examples below use the CLI shipped with ``pynetdicom2``; DCMTK tools
(``echoscu``, ``storescu``, ``findscu``, ``movescu``) work just as well.

C-ECHO (verification):

.. code-block:: bash

    python -m pynetdicom2 verify --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112

C-STORE:

.. code-block:: bash

    python -m pynetdicom2 store --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112 --file_or_dir image.dcm

C-FIND (list patients whose name matches ``DOE*``):

.. code-block:: bash

    python -m pynetdicom2 find --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112 --level patient \
        --attr 'PatientName=DOE*' 'PatientID='

C-MOVE (retrieve one study, receiving the datasets on a local storage SCP):

.. code-block:: bash

    python -m pynetdicom2 move --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112 --level study \
        --attr 'StudyInstanceUID=1.2.3.4' \
        --local_port 11113 --storage_dir ./received

The server knows C-MOVE destinations only through its ``Devices`` registry —
``--local_aet`` above works because ``auto_add`` registers the calling AE
title, and ``Devices.default_port`` must then match ``--local_port``
(or the device must be pre-configured explicitly).

C-GET is supported on the server side as well and can be exercised with any
C-GET capable service class user.

Python API
----------

``tiny_pacs`` ships a small DICOM client for programmatic access:

.. code-block:: python

    import pydicom
    from tiny_pacs.client import DICOMClient

    remote = {'aet': 'TINY_PACS', 'address': 'localhost', 'port': 11112}
    client = DICOMClient('CLIENT', remote)

    # C-ECHO
    client.echo()

    # C-STORE
    client.store('image.dcm')

    # C-FIND
    query = pydicom.Dataset()
    query.QueryRetrieveLevel = 'STUDY'
    query.StudyDate = ''
    for match in client.find(query):
        print(match.StudyInstanceUID)

    # C-MOVE to a known destination
    move = pydicom.Dataset()
    move.QueryRetrieveLevel = 'STUDY'
    move.StudyInstanceUID = '1.2.3.4'
    client.move(move, dest_ae='WORKSTATION')

License
-------

``tiny_pacs`` is licensed under the MIT license, see the ``LICENSE`` file.

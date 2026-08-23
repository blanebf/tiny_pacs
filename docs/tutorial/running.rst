Running tiny_pacs
=================

This tutorial starts the PACS server and exercises the DICOM services it
provides: C-ECHO, C-STORE, C-FIND and C-MOVE.

Starting the server
-------------------

The CLI provides two commands: ``run`` starts the server and ``config``
generates a configuration file (see :doc:`configuration`). Running
``tiny-pacs`` without a command is equivalent to ``tiny-pacs run``.

Start the server with the built-in defaults — AE title ``TINY_PACS``, port
``11112``, an in-memory SQLite database and in-memory storage:

.. code-block:: bash

    tiny-pacs run

Override the AE title and/or the port from the command line:

.. code-block:: bash

    tiny-pacs run -a MY_PACS -p 4242

Load configuration from a file (YAML by extension, JSON for ``*.json``):

.. code-block:: bash

    tiny-pacs run -c config.yaml

``-c`` accepts several files; they are applied in order:

.. code-block:: bash

    tiny-pacs run -c common.yaml overrides.yaml

Or run the interactive wizard and start the server with its answers:

.. code-block:: bash

    tiny-pacs run -i

Each command describes its options in its help:

.. code-block:: bash

    tiny-pacs run --help
    tiny-pacs config --help

Checking connectivity: C-ECHO
-----------------------------

With the server running, verify the association from another terminal. The
examples below use the CLI shipped with ``pynetdicom2``; DCMTK tools
(``echoscu``, ``storescu``, ``findscu``, ``movescu``) work just as well.

.. code-block:: bash

    python -m pynetdicom2 verify --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112

A successful verification means the server accepts associations under the
requested AE title.

Storing images: C-STORE
-----------------------

Send a DICOM file (or a whole directory) to the server:

.. code-block:: bash

    python -m pynetdicom2 store --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112 --file_or_dir image.dcm

With the default components the dataset is kept in the in-memory storage;
configure ``FileStorage`` (see :doc:`configuration`) to persist datasets on
disk.

Querying: C-FIND
----------------

List patients whose name matches ``DOE*``:

.. code-block:: bash

    python -m pynetdicom2 find --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112 --level patient \
        --attr 'PatientName=DOE*' 'PatientID='

Retrieving: C-MOVE
------------------

Retrieve one study to a registered destination:

.. code-block:: bash

    python -m pynetdicom2 move --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112 --level study \
        --attr 'StudyInstanceUID=1.2.3.4'

The server knows C-MOVE destinations only through its ``Devices`` registry —
either pre-configure the destination device, or rely on ``auto_add`` to
register it on its first association. Note that in ``pynetdicom2`` 0.9.8 the
CLI's own move receiver (``--local_port``) does not accept storage
associations, so use a real SCP (for example a second ``tiny_pacs``
instance) as the destination.

C-GET is supported on the server side as well and can be exercised with any
C-GET capable service class user.

Using the Python client
-----------------------

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

See :class:`tiny_pacs.client.DICOMClient` for the full client API.

Embedding the server
--------------------

The server can also be started from your own Python code, which is the
starting point for extending ``tiny_pacs`` with custom components:

.. code-block:: python

    from tiny_pacs import config, server

    conf = config.Config()
    conf.update_config('config.yaml')

    srv = server.Server(conf)
    srv.start()          # start the serving thread and return
    # ... or srv.start_with_block() to run until Ctrl-C

Next steps
----------

Continue with :doc:`extending` to add your own components to the server.

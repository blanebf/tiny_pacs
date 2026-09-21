DICOMweb: ``tiny-pacs-dicomweb``
================================

``tiny-pacs-dicomweb`` exposes the PACS archive over the PS3.18
RESTful services — **QIDO-RS** (query), **WADO-RS**/**WADO-URI**
(retrieve) and **STOW-RS** (store) — served from inside the running
server process, beside the DIMSE AE. Any DICOMweb-capable viewer,
script or integration engine can browse, download and push studies
over plain HTTP, while the archive itself stays the same one the
C-STORE/C-FIND/C-MOVE services use.

The extension contributes:

* ``DICOMWeb`` — the component providing the DICOMweb services as a
  plain WSGI application (bottle) to the **core shared ``HttpServer``
  component** through :class:`~tiny_pacs.events.HttpAppsRegistry`,
  mounted at the configurable ``prefix`` (default ``/dicomweb``). It
  runs no server of its own: binding, the waitress worker pool and
  every bind-level policy belong to the core server.

It imports the core only. Queries travel on the core archive query
events (:class:`~tiny_pacs.events.ArchiveStudyQuery` a.s.o., answered
by the built-in ``PACS`` component), retrieval on
:class:`~tiny_pacs.events.GetFiles` (answered by whichever storage
component holds the files), storage on
:class:`~tiny_pacs.events.GetFile` +
:class:`~tiny_pacs.events.StoreDataset` with ``origin='stow'`` (the
same pipeline an incoming C-STORE feeds), basic auth on
:class:`~tiny_pacs.events.UserVerify` (served, for example, by the
``Users`` component of ``tiny-pacs-identity``) and service actions
announce themselves through :class:`~tiny_pacs.events.AuditRecord`
(recorded, for example, by ``tiny-pacs-audit``). Missing listeners
degrade to plain 503 answers — never to stack traces.

One HTTP server for everything
------------------------------

Like the administration console, DICOMweb mounts on the single core
``HttpServer`` component: one port, one waitress worker pool, one
reverse-proxy upstream for all HTTP front-ends (the console at ``/``,
DICOMweb at ``/dicomweb`` by default). The dispatcher routes by longest
URL prefix; see :doc:`extensions-admin-web` for the canonical
description. Enabling both extensions shares everything — and the
console can discover the DICOMweb mount at runtime through
:class:`~tiny_pacs.events.HttpMountsQuery`.

Installation
------------

.. code-block:: bash

    pip install tiny-pacs-dicomweb

Dependencies are the core plus ``bottle``. The WSGI server itself
(waitress) belongs to the core HTTP extra: ``pip install
tiny_pacs[dicomweb]`` pulls the extension *and* ``tiny_pacs[http]`` in
one go. Installing the extension never changes server behaviour: the
component stays disabled until enabled in the configuration.

Configuration
-------------

.. code-block:: yaml

    components:
      HttpServer:              # core: shared by every HTTP front-end
        on: true
        host: 127.0.0.1        # loopback behind a TLS proxy (default)
        port: 11113
        allow_remote: false
      DICOMWeb:
        on: true
        prefix: /dicomweb      # mount point on the shared server
        auth: none             # none | basic | token
        tokens: []             # static bearer tokens for auth: token
        max_part_size: 67108864   # STOW-RS request body byte cap

Behaviour worth knowing:

* the ``prefix`` must start with a slash and must not be ``/`` (the
  root belongs to the catch-all front-end, e.g. the console);
* ``auth: basic`` without any component answering ``UserVerify`` makes
  the component **fail closed**: it warns and registers no app at all —
  the shared server, other mounts and the DICOM AE keep running;
* ``auth: token`` requires at least one non-empty token at
  configuration validation;
* STOW-RS request bodies are capped at ``max_part_size`` bytes (413
  beyond it); the cap bounds every single part as well;
* ``HttpServer.port: 0`` binds an ephemeral port (useful for tests);
  a port conflict is logged CRITICAL by the core and disables only the
  HTTP server, never the AE.

QIDO-RS: querying the archive
-----------------------------

All paths below are prefix-relative (``/dicomweb`` by default). The
three query levels answer DICOM JSON (PS3.18 F) serialized from the
archive's own DICOM view — the same data the C-FIND front-end serves:

.. list-table::
   :header-rows: 1

   * - Request
     - Answers
   * - ``GET /studies``
     - studies, one JSON object per study
   * - ``GET /studies/{studyUID}/series``
     - series of one study
   * - ``GET /studies/{studyUID}/series/{seriesUID}/instances``
     - instances of one series
   * - ``GET /studies/{studyUID}`` a.s.o. with
       ``Accept: application/dicom+json``
     - the object-metadata form of the same resources (negotiated
       against the WADO-RS retrieval on the identical route)

Supported matching parameters (unknown ones are ignored; empty results
answer **204 No Content**):

* ``PatientID``, ``PatientName`` (substring match),
  ``PatientBirthDate`` (strict ``YYYYMMDD``), ``AccessionNumber``;
* ``StudyDate`` as a single date or range: ``YYYYMMDD``,
  ``YYYYMMDD-YYYYMMDD``, ``YYYYMMDD-`` / ``-YYYYMMDD``;
* ``Modality`` / ``ModalitiesInStudy``, ``StudyInstanceUID``,
  ``SeriesInstanceUID``, ``SOPInstanceUID`` — the UID keys of lower
  levels also filter upper levels (a study search by
  ``SOPInstanceUID`` finds the study owning that instance);
* ``limit``/``offset`` pagination (default 1000 per page, hard caps
  10 000/100 000); ``includefield`` is accepted (every stored column
  is returned anyway; tags beyond the stored columns are ignored, a
  documented v1 limitation) and ``fuzzymatching`` is accepted but has
  no effect (``PatientName`` always matches by substring).

Example:

.. code-block:: bash

    curl -H 'Accept: application/dicom+json' \
      'http://127.0.0.1:11113/dicomweb/studies?PatientID=P1&StudyDate=20240101-20240131&limit=50'

WADO-RS / WADO-URI: retrieving objects
--------------------------------------

Retrieval resolves the requested object through the archive query
events, asks the storage components for the files through ``GetFiles``
— aggregated across *every* installed storage, in batches, exactly
like the core C-GET/C-MOVE paths — and **streams** them: large studies
never materialize in memory:

.. list-table::
   :header-rows: 1

   * - Request
     - Response
   * - ``GET /studies/{studyUID}``
     - every instance of the study, ``multipart/related`` of
       ``application/dicom`` parts
   * - ``GET /studies/{studyUID}/series/{seriesUID}``
     - every instance of the series
   * - ``GET /studies/{studyUID}/series/{seriesUID}/instances/{sopUID}``
     - the instance — single ``application/dicom``, or
       ``multipart/related`` when the ``Accept`` asks for it
   * - ``GET /wado?requestType=WADO&studyUID=...&seriesUID=...&objectUID=...``
     - the WADO-URI form: ``contentType=application/dicom`` (default,
       single object) or ``contentType=multipart/related``; a
       ``transferSyntax`` parameter is honoured for the stored syntax

v1 serves the **stored transfer syntax only** (no transcoding): a
request is refused with **406** only when *no* acceptable
representation can be produced (an ``Accept`` listing several ranges is
served if any one is producible; ``transfer-syntax=*`` — and a range
without the parameter — is unconstrained). Unknown objects answer 404;
frame-level retrieval (``/frames``), rendered images, bulk data and
DICOM XML are explicit non-goals of v1.

The WADO-URI endpoint is also the attach point of a future web viewer:
served on the same origin as the administration console, it needs no
proxy rules of its own.

STOW-RS: pushing objects in
---------------------------

``POST /studies`` (service form) and ``POST /studies/{studyUID}``
(targeted form) accept ``multipart/related`` bodies of
``application/dicom`` parts. Every part is parsed with pydicom and
pushed through the **existing** store pipeline:

* the stored file is materialized via ``GetFile`` — preamble and file
  meta come from the storage component, exactly like in the C-STORE
  path, and the part's dataset bytes are persisted verbatim (any
  transfer syntax, including encapsulated ones);
* the archive rows are recorded via ``StoreDataset`` with
  ``origin='stow'``, so ``StoreDone``/``StoreFailure``, duplicate
  handling (a duplicate with ``overwrite`` disabled becomes a
  per-part failure) and every downstream subscriber behave as for a
  DIMSE store;
* pushed objects are immediately visible to C-FIND, QIDO-RS and
  WADO-RS alike.

The response is the PS3.18 Annex H per-instance summary: a
``Referenced SOP Sequence`` (tag ``00081199``) of successes with their
retrieve URLs and a ``Failed SOP Sequence`` (``00081198``) carrying a
``Failure Reason`` (``00081197``) — ``0x0112`` for a part that is not
parseable DICOM, ``0x0110`` for storage failures (including refused
duplicates and study mismatches in the targeted form). The HTTP status
follows PS3.18 Table 10.5.3-1: **200** when every part stored, **202**
on partial success and **409** when every part failed. Parse and store
failures are **per-part**: they never become 500s. Parts declaring the
deflated transfer syntax are refused (``0x0112``) without ever being
inflated — decompression is not bounded by the body cap; malformed
transfer-syntax declarations are refused likewise.

.. code-block:: bash

    curl -X POST \
      -H 'Content-Type: multipart/related; type="application/dicom"; boundary=boundary' \
      --data-binary @stow-body.bin \
      http://127.0.0.1:11113/dicomweb/studies

Authentication
--------------

.. list-table::
   :header-rows: 1

   * - ``auth``
     - Behaviour
   * - ``none`` (default)
     - every request is served anonymously. The documented pattern:
       run behind an **authenticating reverse proxy** (see below)
   * - ``basic``
     - HTTP basic credentials verified through the core ``UserVerify``
       event (e.g. the ``Users`` component of ``tiny-pacs-identity``);
       fails closed with a startup warning when no user registry is
       installed
   * - ``token``
     - static bearer tokens from the ``tokens`` list, compared in
       constant time — the service-to-service pattern for STOW scripts
       and QIDO dashboards

Failures answer **401 with an empty body** — no detail about *why* a
credential was refused — plus the scheme's ``WWW-Authenticate``
header. waitress does not terminate TLS: every non-``none``
deployment belongs behind TLS (the proxy recipe below).

Audit
-----

Association-level audit does not cover HTTP, so the service emits
:class:`~tiny_pacs.events.AuditRecord` for its actions: category
``service``, events ``stow`` and ``qido``, with the authenticated
username when available and small machine-readable details (counts and
UIDs — never names, never bodies, never secrets). With
``tiny-pacs-audit`` installed, ``tiny-pacs audit query --category
service`` shows the web activity.

Security notes
--------------

* downloaded file paths come from ``GetFiles`` results only — no
  client-supplied string ever opens a file;
* request bodies never reach the log; the request log records method,
  path, status and duration, with headers only at DEBUG and
  ``Authorization`` stripped;
* QIDO/WADO expose exactly what the PACS archive holds — DICOMweb is
  one anonymous "device" from the network's point of view until the
  access-control plans land; restrict it the way you restrict the AE
  (firewall/proxy in front of the loopback bind);
* STOW-RS input is size-capped (``max_part_size``) and parse failures
  are per-part errors, not exceptions.

Deployment: TLS-terminating reverse proxy
-----------------------------------------

The supported production deployment is the loopback bind behind a
TLS-terminating reverse proxy — a **single upstream** for every
front-end:

.. code-block:: nginx

    server {
        listen 443 ssl;
        listen [::]:443 ssl;
        server_name pacs.example.org;

        ssl_certificate     /etc/ssl/private/tiny_pacs.crt;
        ssl_certificate_key /etc/ssl/private/tiny_pacs.key;

        # STOW-RS pushes can be large
        client_max_body_size 512m;

        location / {
            proxy_pass http://127.0.0.1:11113;
            proxy_set_header Host $host;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            # streaming retrievals must not be buffered
            proxy_buffering off;
            proxy_read_timeout 300s;
        }
    }

The STOW-RS retrieve URLs are built from ``X-Forwarded-Proto``/
``X-Forwarded-Host`` when present, so they point at the externally
visible service root. An authenticating proxy (oauth2-proxy, Authelia,
the nginx ``auth_request`` module, ...) in front of ``location /``
adds the identity layer that pairs with ``auth: none``.

Limitations (v1)
----------------

* the stored transfer syntax only — no transcoding, no ``/frames``,
  rendered, bulk-data or deflate endpoints, no DICOM XML;
* ``includefield`` beyond the stored columns is ignored; wildcard
  matching (``*``/``?``) in matching parameters is not translated;
* association-free device identity for STOW (TLS client-certificate
  mapping) is deferred — ``auth: token`` covers v1;
* single-process: like the whole server, the services share the one
  waitress pool of the core ``HttpServer``.

See also
--------

* :doc:`extensions-admin-web` — the administration console sharing the
  same HTTP server;
* :doc:`extensions-identity` — the user registry answering
  ``UserVerify`` for ``auth: basic``;
* :doc:`extensions-audit` — records the ``stow``/``qido`` service
  actions.

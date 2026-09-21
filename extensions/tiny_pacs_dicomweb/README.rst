tiny_pacs_dicomweb
==================

DICOMweb (PS3.18) front-end for `tiny_pacs
<https://github.com/blanebf/tiny_pacs>`_: query, retrieve and store
DICOM objects over HTTP/JSON — **QIDO-RS**, **WADO-RS** (+ WADO-URI) and
**STOW-RS** — served from inside the PACS server process, beside the
DIMSE AE.

What it provides
----------------

* The ``DICOMWeb`` component — a `bottle <https://bottlepy.org/>`_
  application contributed to the core shared ``HttpServer`` component
  (answered through the ``HttpAppsRegistry`` event, mounted at the
  configurable ``prefix``, default ``/dicomweb``). It runs no server of
  its own: binding, the waitress worker pool and every bind-level policy
  live in the core server, and the documented deployment is a
  TLS-terminating reverse proxy in front of it.
* **QIDO-RS**: ``GET {prefix}/studies``,
  ``GET {prefix}/studies/{uid}/series`` and
  ``.../series/{uid}/instances`` with the standard matching parameters
  (``PatientID``, ``StudyDate`` ranges, ``ModalitiesInStudy``, UID
  keys, ``limit``/``offset``, ``includefield``) answered as DICOM JSON
  serialized from the archive's own DICOM view — the same data the
  C-FIND front-end serves.
* **WADO-RS / WADO-URI**: study/series/instance retrieval streamed as
  ``multipart/related`` (or single ``application/dicom``) from the
  storage components; the stored transfer syntax only in v1 (406 where
  an ``Accept``/``transferSyntax`` cannot be matched). ``GET
  {prefix}/wado?requestType=WADO&...`` serves the URI form on the same
  code path — the attach point of a future web viewer.
* **STOW-RS**: ``POST {prefix}/studies[/{studyUID}]`` pushes DICOM
  objects through the *existing* store pipeline
  (``GetFile`` + ``StoreDataset`` with ``origin='stow'``), answering the
  PS3.18 H per-instance success/failure summary with status 200 (all
  stored), 202 (partial) or 409 (all failed) — never 500s for broken
  parts. Deflated-transfer-syntax parts are refused without inflating
  them (bounded parsing).

Event-based only: queries travel on the archive query events
(``ArchiveStudyQuery`` a.s.o.), retrieval on ``GetFiles``, storage on
``GetFile``/``StoreDataset``, basic auth on ``UserVerify`` and service
actions announce themselves through ``AuditRecord``. The runtime package
imports the core only — whichever extension provides the listeners is an
installation concern, never an import concern.

Authentication
--------------

``auth: none`` (default) for the authenticating-reverse-proxy pattern;
``auth: basic`` checks credentials through the core ``UserVerify`` event
and fails closed when no user registry is installed (the component then
mounts no app at all); ``auth: token`` accepts configured static bearer
tokens for service-to-service use. Failures answer 401 with no body
details. Every non-``none`` deployment belongs behind TLS.

Installation
------------

.. code-block:: bash

    pip install tiny-pacs-dicomweb

Requires ``tiny_pacs >=0.4,<0.5`` and ``bottle``. HTTP serving comes
from the core ``HttpServer`` component and its ``tiny_pacs[http]`` extra
(waitress), which ``tiny_pacs[dicomweb]`` implies. Installing the
extension never changes server behaviour: the component stays disabled
until enabled in the configuration.

Configuration
-------------

.. code-block:: yaml

    components:
      HttpServer:              # core: shared by every HTTP front-end
        on: true
        host: 127.0.0.1        # reverse-proxy pattern; waitress has no TLS
        port: 11113
        allow_remote: false
      DICOMWeb:
        on: true
        prefix: /dicomweb      # mount point on the shared server
        auth: none             # none | basic | token
        tokens: []             # static bearer tokens for auth: token
        max_part_size: 67108864    # STOW request body byte cap

Documentation
-------------

See the `DICOMweb tutorial
<https://tiny-pacs.readthedocs.io/en/latest/tutorial/extensions-dicomweb.html>`_
in the tiny_pacs documentation, including the TLS-terminating
reverse-proxy recipe.

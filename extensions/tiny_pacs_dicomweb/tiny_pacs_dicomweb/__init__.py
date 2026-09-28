"""tiny-pacs-dicomweb: PS3.18 RESTful services front-end.

The extension plugs into tiny_pacs through entry points: the
:class:`~tiny_pacs_dicomweb.component.DICOMWeb` component contributes a
DICOMweb (QIDO-RS, WADO-RS/WADO-URI, STOW-RS) WSGI application to the
core shared HTTP server (:class:`tiny_pacs.http.HttpServer`) — it runs
no server of its own.

It imports the core only: queries travel on the archive query events,
retrieval on :class:`~tiny_pacs.events.GetFiles`, storage on
:class:`~tiny_pacs.events.StoreDataset` (with ``origin='stow'``) and
file materialization on :class:`~tiny_pacs.events.GetFile`; whichever
extension provides the listeners (the first-party storage/identity/
audit packages or any future replacement) is an installation concern,
never an import concern.
"""
__version__ = '0.1.0'

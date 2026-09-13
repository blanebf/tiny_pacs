/* Vanilla JS of the tiny_pacs administration console.
   No frameworks, no CDNs, no build step. Served under a
   script-src 'self' Content-Security-Policy. */
(function () {
  'use strict';

  function csrfToken() {
    var meta = document.querySelector('meta[name="csrf-token"]');
    return meta ? meta.getAttribute('content') : '';
  }

  function deviceEcho(button) {
    var aet = button.getAttribute('data-echo-aet');
    var out = document.querySelector('.echo-result');
    if (!aet || !out) {
      return;
    }
    button.disabled = true;
    out.textContent = 'Sending C-ECHO…';
    fetch('/api/echo/' + encodeURIComponent(aet), {
      method: 'POST',
      headers: { 'X-CSRF-Token': csrfToken() }
    }).then(function (response) {
      return response.json();
    }).then(function (data) {
      if (data.ok) {
        out.textContent = 'C-ECHO succeeded in ' + data.elapsed_ms + ' ms';
      } else {
        out.textContent = 'C-ECHO failed: ' +
          (data.error || 'unknown error');
      }
    }).catch(function () {
      out.textContent = 'C-ECHO request failed';
    }).then(function () {
      button.disabled = false;
    });
  }

  document.querySelectorAll('[data-echo-aet]').forEach(function (button) {
    button.addEventListener('click', function () {
      deviceEcho(button);
    });
  });

  document.querySelectorAll('form[data-confirm]').forEach(function (form) {
    form.addEventListener('submit', function (event) {
      var message = form.getAttribute('data-confirm');
      if (message && !window.confirm(message)) {
        event.preventDefault();
      }
    });
  });
})();

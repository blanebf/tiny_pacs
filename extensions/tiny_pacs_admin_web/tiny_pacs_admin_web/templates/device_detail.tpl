<h2>Device {{ device['aet'] }}</h2>
<section class="card">
  <table class="kv">
    <tr><th>AE title</th><td>{{ device['aet'] }}</td></tr>
    <tr><th>Address</th><td>{{ device['address'] }}</td></tr>
    <tr><th>Port</th><td>{{ device['port'] }}</td></tr>
    <tr><th>Identity policy</th><td>{{ device['identity'] or '-' }}</td></tr>
    <tr><th>Outgoing username</th><td>{{ device['username'] or '-' }}</td></tr>
    <tr><th>Outgoing password</th>
        <td>{{ '********' if device['password_set'] else 'not set' }}</td></tr>
    % skip = ('aet', 'address', 'port', 'identity', 'username', 'password_set')
    % for key, value in device.items():
    % if key not in skip:
    <tr><th>{{ key }}</th><td>{{ value }}</td></tr>
    % end
    % end
  </table>
</section>
<section class="card">
  <h3>Connectivity</h3>
  % if echo:
    % if is_admin:
  <button type="button" data-echo-aet="{{ device['aet'] }}">Send C-ECHO</button>
  <span class="echo-result" role="status"></span>
    % else:
  <p class="notice">C-ECHO is available to administrators only.</p>
    % end
  % else:
  <p class="notice">Device echo is not available on this server.</p>
  % end
</section>
% if is_admin:
<section class="card">
  <p><a class="button" href="/devices/{{ quote(device['aet']) }}/edit">Edit device</a></p>
  <form method="post" action="/devices/{{ quote(device['aet']) }}/delete"
        data-confirm="Delete device {{ device['aet'] }}? This cannot be undone.">
    <input type="hidden" name="csrf_token" value="{{ csrf }}">
    <button type="submit" class="danger">Delete device</button>
  </form>
</section>
% end
<p><a href="/devices/">Back to devices</a></p>

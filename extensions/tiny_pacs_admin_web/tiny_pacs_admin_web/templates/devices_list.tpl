<h2>Devices</h2>
% if is_admin:
<p><a class="button" href="/devices/add">Add device</a></p>
% end
% if not devices:
<p class="notice">No devices registered.</p>
% else:
<table>
  <thead>
    <tr>
      <th>AET</th><th>Address</th><th>Port</th><th>Identity</th>
      <th>Outgoing user</th><th>Outgoing password</th>
    </tr>
  </thead>
  <tbody>
  % for device in devices:
  <tr>
    <td><a href="/devices/{{ quote(device['aet']) }}">{{ device['aet'] }}</a></td>
    <td>{{ device['address'] }}</td>
    <td>{{ device['port'] }}</td>
    <td>{{ device['identity'] }}</td>
    <td>{{ device['username'] }}</td>
    <td>{{ '********' if device['password_set'] else '-' }}</td>
  </tr>
  % end
  </tbody>
</table>
% end

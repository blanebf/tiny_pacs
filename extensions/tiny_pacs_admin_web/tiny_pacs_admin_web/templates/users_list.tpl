<h2>Users</h2>
% if is_admin:
<p><a class="button" href="/users/add">Add user</a></p>
% else:
<p class="notice">Read-only view of the user registry.</p>
% end
% if not users:
<p class="notice">No users registered.</p>
% else:
<table>
  <thead>
    <tr>
      <th>Username</th><th>State</th><th>Created</th><th>Last login</th>
      <th>Console role</th>
      % if is_admin:
      <th>Actions</th>
      % end
    </tr>
  </thead>
  <tbody>
  % for user in users:
  <tr>
    <td>{{ user['username'] }}</td>
    <td>{{ 'active' if user['is_active'] else 'inactive' }}</td>
    <td>{{ user['created'] }}</td>
    <td>{{ user['last_login'] }}</td>
    <td>{{ user['console_role'] }}</td>
    % if is_admin:
    <td class="actions">
      <a href="/users/{{ quote(user['username']) }}/password">Password</a>
      <form method="post" action="/users/{{ quote(user['username']) }}/toggle"
            class="inline-form">
        <input type="hidden" name="csrf_token" value="{{ csrf }}">
        <button type="submit" class="linklike">
          {{ 'Deactivate' if user['is_active'] else 'Activate' }}</button>
      </form>
    </td>
    % end
    </tr>
  % end
  </tbody>
</table>
% end

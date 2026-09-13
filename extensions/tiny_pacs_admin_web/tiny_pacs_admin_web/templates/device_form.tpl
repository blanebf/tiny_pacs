% if mode == 'add':
<h2>Add device</h2>
% else:
<h2>Edit device {{ aet }}</h2>
% end
% if error:
<p class="error" role="alert">{{ error }}</p>
% end
% if mode == 'add':
<form method="post" action="/devices/add" class="card">
% else:
<form method="post" action="/devices/{{ quote(aet) }}/edit" class="card">
% end
  <input type="hidden" name="csrf_token" value="{{ csrf }}">
  <label for="aet">AE title</label>
  % if mode == 'add':
  <input id="aet" name="aet" type="text" maxlength="16" required
         value="{{ values.get('aet', '') }}">
  % else:
  <input id="aet" type="text" value="{{ aet }}" readonly>
  % end
  <label for="address">Address</label>
  <input id="address" name="address" type="text" maxlength="255" required
         value="{{ values.get('address', '') }}">
  <label for="port">Port</label>
  <input id="port" name="port" type="number" min="1" max="65535"
         value="{{ values.get('port', '') }}">
  <label for="identity">Identity policy</label>
  <select id="identity" name="identity">
    % for policy in identity_policies:
    <option value="{{ policy }}" {{ 'selected' if values.get('identity', 'none') == policy else '' }}>{{ policy }}</option>
    % end
  </select>
  <label for="username">Outgoing identity username</label>
  <input id="username" name="username" type="text" maxlength="64"
         autocomplete="off" value="{{ values.get('username', '') }}">
  % if mode == 'add':
  <label for="password">Outgoing identity password</label>
  <input id="password" name="password" type="password"
         autocomplete="new-password">
  % else:
  <p class="hint">Outgoing password:
    {{ '********' if values.get('password_set') else 'not set' }}</p>
  <label for="new_password">New outgoing password</label>
  <input id="new_password" name="new_password" type="password"
         autocomplete="new-password">
  <p class="hint">Leave empty to keep the stored password.</p>
  % end
  <p><button type="submit">{{ 'Add device' if mode == 'add' else 'Save changes' }}</button>
  <a href="/devices/">Cancel</a></p>
</form>

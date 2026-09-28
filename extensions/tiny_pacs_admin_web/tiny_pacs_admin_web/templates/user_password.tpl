<h2>New password for {{ username }}</h2>
% if error:
<p class="error" role="alert">{{ error }}</p>
% end
<form method="post" action="/users/{{ quote(username) }}/password"
      class="card">
  <input type="hidden" name="csrf_token" value="{{ csrf }}">
  <label for="password">New password</label>
  <input id="password" name="password" type="password"
         autocomplete="new-password" required>
  <p><button type="submit">Change password</button>
  <a href="/users/">Cancel</a></p>
</form>
<p class="hint">Changing the password signs the user out of every other
console session on this server.</p>

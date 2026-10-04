<h2>Add user</h2>
% if error:
<p class="error" role="alert">{{ error }}</p>
% end
<form method="post" action="/users/add" class="card">
  <input type="hidden" name="csrf_token" value="{{ csrf }}">
  <label for="username">Username</label>
  <input id="username" name="username" type="text" maxlength="64" required
         value="{{ username }}">
  <label for="password">Password</label>
  <input id="password" name="password" type="password"
         autocomplete="new-password" required>
  <p><button type="submit">Add user</button>
  <a href="/users/">Cancel</a></p>
</form>
<p class="hint">Console login additionally requires a grant:
<code>tiny-pacs web-admin grant USERNAME --role admin|viewer</code>.</p>

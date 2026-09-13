<h2>Sign in</h2>
% if error:
<p class="error" role="alert">{{ error }}</p>
% end
<form method="post" action="/login" class="card login-form">
  <label for="username">Username</label>
  <input id="username" name="username" type="text" autocomplete="username"
         required maxlength="64">
  <label for="password">Password</label>
  <input id="password" name="password" type="password"
         autocomplete="current-password" required maxlength="4096">
  <p><button type="submit">Sign in</button></p>
</form>
<p class="hint">Console access must be granted with
<code>tiny-pacs web-admin grant USERNAME --role admin|viewer</code>.</p>

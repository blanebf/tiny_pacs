<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="csrf-token" content="{{ csrf }}">
<meta name="robots" content="noindex, nofollow">
<title>{{ title }} — tiny_pacs administration</title>
<link rel="stylesheet" href="/static/style.css">
<script src="/static/app.js" defer></script>
</head>
<body>
<header class="topbar">
  <span class="brand">tiny_pacs <span class="sub">administration</span></span>
  % if session:
  <nav class="mainnav">
    <a href="/">Dashboard</a>
    % if features['devices']:
    <a href="/devices/">Devices</a>
    % end
    % if features['archive']:
    <a href="/archive/">Archive</a>
    % end
    % if features['users'] and (is_admin or show_users_to_viewer):
    <a href="/users/">Users</a>
    % end
    <form method="post" action="/logout" class="inline-form">
      <input type="hidden" name="csrf_token" value="{{ csrf }}">
      <button type="submit" class="linklike">Log out — {{ session.username }} ({{ session.role }})</button>
    </form>
  </nav>
  % end
</header>
<main>
{{!body}}
</main>
</body>
</html>

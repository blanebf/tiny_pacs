<h2>Dashboard</h2>
<section class="card">
  <h3>Server</h3>
  <table class="kv">
    <tr><th>Main AE title</th><td>{{ main_aet }}</td></tr>
    <tr><th>HTTP mounts</th><td>{{ http_mounts }}</td></tr>
  </table>
</section>
<section class="card">
  <h3>Features</h3>
  <table>
    <thead><tr><th>Feature</th><th>Available</th></tr></thead>
    <tbody>
    % for name in sorted(features):
    <tr>
      <td>{{ name }}</td>
      <td>{{ 'yes' if features[name] else 'no — enable the component that serves it' }}</td>
    </tr>
    % end
    </tbody>
  </table>
</section>
<section class="card">
  <h3>Storage</h3>
  % if storage:
  <table class="kv">
    <tr><th>Records (total / stored / failed)</th>
        <td>{{ storage['records_total'] }} / {{ storage['records_stored'] }} / {{ storage['records_failed'] }}</td></tr>
    <tr><th>Oldest record</th><td>{{ storage['oldest'] or '-' }}</td></tr>
    <tr><th>Newest record</th><td>{{ storage['newest'] or '-' }}</td></tr>
    % if storage['storage_dir']:
    <tr><th>Storage directory</th><td>{{ storage['storage_dir'] }}</td></tr>
    <tr><th>Files on disk</th><td>{{ storage['file_count'] }}</td></tr>
    <tr><th>Bytes on disk</th><td>{{ storage['file_bytes'] }}</td></tr>
    % end
  </table>
  % else:
  <p class="notice">Storage statistics are not available.</p>
  % end
</section>
<section class="card">
  <h3>Components</h3>
  <table>
    <thead><tr><th>Component</th><th>Origin</th><th>Schema version</th></tr></thead>
    <tbody>
    % versions = dict(schema_versions)
    % for item in components:
    <tr>
      <td>{{ item['name'] }}</td>
      <td>{{ item['origin'] }}</td>
      <td>{{ versions.get(item['name'], '-') }}</td>
    </tr>
    % end
    </tbody>
  </table>
</section>
<section class="card">
  <h3>Database tables</h3>
  % if table_counts:
  <table>
    <thead><tr><th>Table</th><th>Rows</th></tr></thead>
    <tbody>
    % for name, count in table_counts:
    <tr><td>{{ name }}</td><td>{{ count }}</td></tr>
    % end
    </tbody>
  </table>
  % else:
  <p class="notice">Row counts are not available.</p>
  % end
</section>

<h2>{{ level_title }}</h2>
% if error:
<p class="error" role="alert">{{ error }}</p>
% end
<form method="get" action="{{ form_target }}" class="card archive-search">
  <div class="form-grid">
  % for f in form_fields:
    <div class="field">
      <label for="af-{{ f['name'] }}">{{ f['label'] }}</label>
      <input id="af-{{ f['name'] }}" name="{{ f['name'] }}"
             type="{{ f['type'] }}" value="{{ f['value'] }}"
             {{ 'readonly' if f['readonly'] else '' }}>
    </div>
  % end
  </div>
  <p class="search-actions">
    <button type="submit">Search</button>
    <a href="{{ form_target }}">Reset</a>
  </p>
</form>
% if searched:
<p class="notice">{{ total }} match(es) — page {{ page['page'] }} of {{ page['pages'] }}</p>
% if rows:
<table class="archive-table">
  <thead>
    <tr>
      % for column in columns:
      <th>{{ column }}</th>
      % end
      % if link_label:
      <th class="browse-col">&nbsp;</th>
      % end
    </tr>
  </thead>
  <tbody>
  % for row in rows:
  <tr>
    % for cell in row['cells']:
    <td>{{ cell }}</td>
    % end
    % if link_label:
    <td class="browse-col">
      % if row['link']:
      <a href="{{ row['link'] }}">{{ link_label }}</a>
      % end
    </td>
    % end
  </tr>
  % end
  </tbody>
</table>
% else:
<p class="notice">No matching records.</p>
% end
% if page['has_prev'] or page['has_next']:
<nav class="pagination">
  % if page['has_prev']:
  <a href="{{ form_target }}?offset={{ page['prev_offset'] }}{{ ('&' + qs) if qs else '' }}">&larr; Previous page</a>
  % end
  % if page['has_next']:
  <a href="{{ form_target }}?offset={{ page['next_offset'] }}{{ ('&' + qs) if qs else '' }}">Next page &rarr;</a>
  % end
</nav>
% end
% end
% if level != 'patient':
<p><a href="/archive/">Back to patients</a></p>
% end

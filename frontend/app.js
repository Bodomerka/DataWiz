let sessionId = null;

const uploadForm = document.getElementById('upload-form');
const fileInput = document.getElementById('file-input');
const fileNameSpan = document.getElementById('file-name');
const uploadStatus = document.getElementById('upload-status');
const schemaDiv = document.getElementById('schema');
const previewDiv = document.getElementById('preview');

const chatWindow = document.getElementById('chat-window');
const chatText = document.getElementById('chat-text');
const sendBtn = document.getElementById('send-btn');

function printMessage(text, who = 'bot') {
  const div = document.createElement('div');
  div.className = `msg ${who}`;
  div.textContent = text;
  chatWindow.appendChild(div);
  chatWindow.scrollTop = chatWindow.scrollHeight;
}

function printCode(code) {
  const pre = document.createElement('pre');
  pre.className = 'code';
  pre.textContent = code;
  chatWindow.appendChild(pre);
  chatWindow.scrollTop = chatWindow.scrollHeight;
}

// Custom file input label update
fileInput.addEventListener('change', () => {
  const name = fileInput.files && fileInput.files[0] ? fileInput.files[0].name : 'Файл не обрано';
  fileNameSpan.textContent = name;
});

// Collapsible preview
function attachPreviewToggle() {
  const existing = document.getElementById('preview-toggle');
  if (existing) existing.remove();
  if (!previewDiv.innerHTML) return;
  const btn = document.createElement('button');
  btn.id = 'preview-toggle';
  btn.className = 'btn toggle';
  btn.setAttribute('aria-expanded', 'true');
  btn.textContent = 'Згорнути попередній перегляд';
  btn.addEventListener('click', () => {
    const expanded = btn.getAttribute('aria-expanded') === 'true';
    btn.setAttribute('aria-expanded', expanded ? 'false' : 'true');
    const body = previewDiv.querySelector('.preview-body');
    if (body) body.style.display = expanded ? 'none' : '';
    btn.textContent = expanded ? 'Розгорнути попередній перегляд' : 'Згорнути попередній перегляд';
  });
  previewDiv.prepend(btn);
}

function renderTablePreview(preview) {
  if (!preview || !preview.columns || !preview.rows) return '';
  const header = `<tr>${preview.columns.map(c => `<th>${c}</th>`).join('')}</tr>`;
  const rows = preview.rows.map(r => `<tr>${preview.columns.map(c => `<td>${r[c] ?? ''}</td>`).join('')}</tr>`).join('');
  return `<div class="preview-body"><table>${header}${rows}</table></div>`;
}

uploadForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  const file = fileInput.files[0];
  if (!file) {
    uploadStatus.textContent = 'Оберіть файл.';
    return;
  }
  const form = new FormData();
  form.append('file', file);
  uploadStatus.textContent = 'Завантаження...';
  schemaDiv.innerHTML = '';
  previewDiv.innerHTML = '';
  try {
    const resp = await fetch('/api/upload', { method: 'POST', body: form });
    if (!resp.ok) throw new Error(await resp.text());
    const json = await resp.json();
    sessionId = json.session_id;
    uploadStatus.textContent = `Сесію створено: ${sessionId}`;
    const schemaList = (json.schema || []).map(s => `<li><b>${s.name}</b> <small>(${s.pandas_dtype}, ${s.kind})</small></li>`).join('');
    schemaDiv.innerHTML = `<h3>Схема</h3><ul>${schemaList}</ul>`;
    previewDiv.innerHTML = `<h3>Попередній перегляд</h3>${renderTablePreview(json.preview)}`;
    attachPreviewToggle();
    printMessage('Файл завантажено. Можете ставити питання.', 'bot');
  } catch (err) {
    console.error(err);
    uploadStatus.textContent = 'Помилка завантаження: ' + (err.message || err);
  }
});

async function sendMessage() {
  const text = chatText.value.trim();
  if (!text) return;
  if (!sessionId) {
    printMessage('Спершу завантажте таблицю.', 'bot');
    return;
  }
  printMessage(text, 'user');
  chatText.value = '';
  sendBtn.disabled = true;
  try {
    const resp = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: sessionId, message: text }),
    });
    const json = await resp.json();
    if (!resp.ok) throw new Error(json.detail || 'Помилка');
    printMessage(json.answer || '(немає відповіді)', 'bot');
    if (json.sql) {
      printCode(json.sql);
    }
    if (json.result_preview) {
      const html = renderTablePreview(json.result_preview);
      const wrap = document.createElement('div');
      wrap.innerHTML = html;
      chatWindow.appendChild(wrap);
    }
    if (json.explanation) {
      printMessage('Пояснення: ' + json.explanation, 'bot');
    }
  } catch (err) {
    console.error(err);
    printMessage('Помилка: ' + (err.message || err), 'bot');
  } finally {
    sendBtn.disabled = false;
  }
}

sendBtn.addEventListener('click', sendMessage);
chatText.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') sendMessage();
});



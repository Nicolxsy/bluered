const state = {
  activeTab: "session",
  session: null,
  transfers: new Map(),
  activeRoom: "general",
  selectedCommunityId: null,
  reviewTargetHash: null,
  reviewTargetName: "",
  selectedRating: 5,
  ws: null,
};

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatBytes(bytes) {
  const n = Number(bytes || 0);
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let val = n / 1024;
  let idx = 0;
  while (val >= 1024 && idx < units.length - 1) {
    val /= 1024;
    idx += 1;
  }
  return `${val.toFixed(1)} ${units[idx]}`;
}

function showToast(message, isError = false) {
  const region = document.getElementById("toast-region");
  const el = document.createElement("div");
  el.className = `toast${isError ? " is-error" : ""}`;
  el.textContent = message;
  region.appendChild(el);
  setTimeout(() => el.remove(), 3800);
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.detail || `Error HTTP ${res.status}`);
  }
  return data;
}

/* Navigation */
function switchTab(tabName) {
  state.activeTab = tabName;
  document.querySelectorAll(".nav-item").forEach((btn) => {
    const active = btn.dataset.tab === tabName;
    btn.classList.toggle("is-active", active);
    btn.setAttribute("aria-selected", String(active));
  });
  document.querySelectorAll(".tab-panel").forEach((panel) => {
    const active = panel.id === `tab-${tabName}`;
    panel.hidden = !active;
    panel.classList.toggle("is-active", active);
  });

  if (tabName === "session") loadSession();
  if (tabName === "folders") loadFolders();
  if (tabName === "transfers") loadTransfers();
  if (tabName === "community") loadCommunityTab();
  if (tabName === "library") loadLibrary();
}

/* 1. Session & Presence */
async function loadSession() {
  try {
    const data = await api("/api/session");
    state.session = data;
    renderSession(data);
  } catch (err) {
    showToast(err.message, true);
  }
}

function renderSession(data) {
  const status = (data.presence_status || "OFFLINE").toUpperCase();
  document.getElementById("sidebar-peer-label").textContent = `${data.username} (${status})`;
  document.getElementById("sidebar-tcp-meta").textContent = `TCP ${data.advertised_host}:${data.tcp_port}`;

  const pill = document.getElementById("session-presence-pill");
  pill.textContent = status;
  pill.className = `pill pill-${status.toLowerCase()}`;

  document.getElementById("auth-username").value = data.username || "";
  document.getElementById("meta-peer-id").textContent = data.peer_id || "—";
  document.getElementById("meta-endpoint").textContent = `${data.advertised_host}:${data.tcp_port}`;
  document.getElementById("meta-slots").textContent = `${data.active_uploads} / ${data.max_upload_slots}`;
  document.getElementById("transfers-slots-label").textContent = `Subidas activas: ${data.active_uploads} / ${data.max_upload_slots}`;

  const bloom = data.bloom || {};
  document.getElementById("bloom-size-bits").textContent = `${bloom.size_bits || 0} bits`;
  document.getElementById("bloom-hash-k").textContent = String(bloom.hash_functions_k || 0);
  document.getElementById("bloom-rejects").textContent = String(bloom.bloom_rejects || 0);
  document.getElementById("bloom-hits").textContent = String(bloom.bloom_hits || 0);
  document.getElementById("bloom-sqlite").textContent = String(bloom.sqlite_queries || 0);

  document.querySelectorAll(".seg-btn").forEach((btn) => {
    btn.classList.toggle("is-active", btn.dataset.presence === status);
  });
}

/* 2. Shared Folders */
async function loadFolders() {
  try {
    const data = await api("/api/folders");
    const list = document.getElementById("shared-dirs-list");
    list.innerHTML = (data.directories || [])
      .map(
        (dir) => `
        <li class="chip">
          <span>${escapeHtml(dir)}</span>
          <button type="button" data-remove-dir="${escapeHtml(dir)}" aria-label="Quitar carpeta">×</button>
        </li>`
      )
      .join("");

    const files = data.files || [];
    document.getElementById("indexed-count-label").textContent = `${files.length} archivos`;
    const tbody = document.getElementById("shared-files-tbody");
    if (!files.length) {
      tbody.innerHTML = `<tr><td colspan="4" class="empty-cell">No hay archivos compartidos indexados.</td></tr>`;
      return;
    }
    tbody.innerHTML = files
      .map(
        (f) => `
        <tr>
          <td>${escapeHtml(f.file_name)}</td>
          <td><span class="pill">${escapeHtml(f.category)}</span></td>
          <td class="num">${formatBytes(f.size_bytes)}</td>
          <td class="mono">${escapeHtml(f.file_hash.slice(0, 16))}…</td>
        </tr>`
      )
      .join("");
  } catch (err) {
    showToast(err.message, true);
  }
}

/* 3. Global Search, Browse Peer & Reviews */
async function runSearch(query) {
  const tbody = document.getElementById("search-results-tbody");
  tbody.innerHTML = `<tr><td colspan="5" class="empty-cell">Consultando peers en la red…</td></tr>`;
  try {
    const data = await api(`/api/search?q=${encodeURIComponent(query)}`);
    const results = data.results || [];
    document.getElementById("search-count-label").textContent = `${results.length} resultados para "${query}"`;
    if (!results.length) {
      tbody.innerHTML = `<tr><td colspan="5" class="empty-cell">Sin coincidencias para "${escapeHtml(query)}".</td></tr>`;
      return;
    }
    tbody.innerHTML = results
      .map((r) => {
        const ratingLabel =
          r.avg_rating !== null && r.avg_rating !== undefined
            ? `${Number(r.avg_rating).toFixed(1)} ★ (${r.review_count})`
            : `Sin reseñas (0)`;
        const payload = escapeHtml(JSON.stringify(r));
        return `
        <tr>
          <td>
            <div><strong>${escapeHtml(r.file_name)}</strong></div>
            <div class="mono muted">${escapeHtml(r.file_hash.slice(0, 16))}…</div>
          </td>
          <td>${escapeHtml(r.peer_username)} <span class="mono muted">${escapeHtml(r.peer_ip)}:${r.peer_port}</span></td>
          <td class="num">${formatBytes(r.size_bytes)}</td>
          <td class="tabular">${escapeHtml(ratingLabel)}</td>
          <td class="actions-col">
            <button class="btn btn-sm btn-secondary" type="button" data-open-reviews="${escapeHtml(r.file_hash)}" data-file-name="${escapeHtml(r.file_name)}">Reseñas</button>
            <button class="btn btn-sm btn-secondary" type="button" data-browse-peer="${payload}">Explorar Usuario</button>
            <button class="btn btn-sm btn-primary" type="button" data-download-item="${payload}">Descargar P2P</button>
          </td>
        </tr>`;
      })
      .join("");
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="5" class="empty-cell">${escapeHtml(err.message)}</td></tr>`;
    showToast(err.message, true);
  }
}

async function openReviewsModal(fileHash, fileName) {
  state.reviewTargetHash = fileHash;
  state.reviewTargetName = fileName || fileHash.slice(0, 12);
  document.getElementById("reviews-modal-title").textContent = `Reseñas — ${state.reviewTargetName}`;
  document.getElementById("reviews-modal-hash").textContent = fileHash;
  document.getElementById("review-comment").value = "";
  const dialog = document.getElementById("modal-reviews");
  dialog.showModal();
  await refreshReviewsModal();
}

async function refreshReviewsModal() {
  const listEl = document.getElementById("reviews-list");
  const summaryEl = document.getElementById("reviews-summary");
  listEl.innerHTML = `<div class="muted">Cargando reseñas…</div>`;
  try {
    const data = await api(`/api/reviews/${encodeURIComponent(state.reviewTargetHash)}`);
    summaryEl.textContent =
      data.avg_rating !== null && data.avg_rating !== undefined
        ? `Promedio: ${Number(data.avg_rating).toFixed(2)} ★ (${data.review_count} reseñas)`
        : "Sin calificaciones todavía.";
    const reviews = data.reviews || [];
    if (!reviews.length) {
      listEl.innerHTML = `<div class="muted">Sé el primero en calificar este archivo.</div>`;
      return;
    }
    listEl.innerHTML = reviews
      .map(
        (rev) => `
        <article class="review-card">
          <div><strong>${"★".repeat(rev.rating)}${"☆".repeat(5 - rev.rating)}</strong> · <span class="mono muted">${escapeHtml(rev.author_id)}</span></div>
          <p style="margin:6px 0 0">${escapeHtml(rev.comment)}</p>
        </article>`
      )
      .join("");
  } catch (err) {
    summaryEl.textContent = err.message;
    listEl.innerHTML = "";
  }
}

async function openBrowseModal(peerInfo) {
  const dialog = document.getElementById("modal-browse");
  document.getElementById("browse-modal-title").textContent = `Archivos de ${peerInfo.peer_username}`;
  document.getElementById("browse-modal-endpoint").textContent = `${peerInfo.peer_ip}:${peerInfo.peer_port}`;
  const tbody = document.getElementById("browse-files-tbody");
  tbody.innerHTML = `<tr><td colspan="4" class="empty-cell">Consultando catálogo P2P…</td></tr>`;
  dialog.showModal();
  try {
    const data = await api("/api/peers/browse", {
      method: "POST",
      body: JSON.stringify({
        peer_id: peerInfo.peer_id,
        username: peerInfo.peer_username,
        ip_address: peerInfo.peer_ip,
        tcp_port: peerInfo.peer_port,
      }),
    });
    const files = data.files || [];
    if (!files.length) {
      tbody.innerHTML = `<tr><td colspan="4" class="empty-cell">Este peer no comparte archivos actualmente.</td></tr>`;
      return;
    }
    tbody.innerHTML = files
      .map((f) => {
        const dlPayload = escapeHtml(
          JSON.stringify({
            peer_id: peerInfo.peer_id,
            peer_username: peerInfo.peer_username,
            peer_ip: peerInfo.peer_ip,
            peer_port: peerInfo.peer_port,
            file_hash: f.file_hash,
            file_name: f.file_name,
            size_bytes: f.size_bytes,
          })
        );
        return `
        <tr>
          <td>${escapeHtml(f.file_name)}</td>
          <td><span class="pill">${escapeHtml(f.category)}</span></td>
          <td class="num">${formatBytes(f.size_bytes)}</td>
          <td class="actions-col">
            <button class="btn btn-sm btn-primary" type="button" data-download-item="${dlPayload}">Descargar P2P</button>
          </td>
        </tr>`;
      })
      .join("");
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="4" class="empty-cell">${escapeHtml(err.message)}</td></tr>`;
  }
}

/* 4. Transfers & Queues */
async function startDownload(item) {
  try {
    const task = await api("/api/transfers/download", {
      method: "POST",
      body: JSON.stringify({
        peer_id: item.peer_id,
        peer_username: item.peer_username,
        peer_ip: item.peer_ip,
        peer_port: item.peer_port,
        file_hash: item.file_hash,
        file_name: item.file_name,
        size_bytes: item.size_bytes,
        conflict_policy: "keep_both",
        wait: false,
      }),
    });
    state.transfers.set(task.task_id, task);
    renderTransfers();
    showToast(`Descarga iniciada: ${item.file_name}`);
    switchTab("transfers");
  } catch (err) {
    showToast(err.message, true);
  }
}

async function loadTransfers() {
  try {
    const data = await api("/api/transfers");
    state.transfers.clear();
    (data.transfers || []).forEach((t) => state.transfers.set(t.task_id, t));
    document.getElementById("transfers-slots-label").textContent = `Subidas activas: ${data.active_uploads} / ${data.max_upload_slots}`;
    renderTransfers();
  } catch (err) {
    showToast(err.message, true);
  }
}

function renderTransfers() {
  const container = document.getElementById("transfers-list");
  const tasks = Array.from(state.transfers.values());
  const activeCount = tasks.filter((t) =>
    ["QUEUED", "HANDSHAKING", "TRANSFERRING", "RESUMING"].includes(t.state)
  ).length;
  const badge = document.getElementById("nav-transfer-count");
  badge.textContent = String(activeCount);
  badge.hidden = activeCount === 0;

  if (!tasks.length) {
    container.innerHTML = `<div class="surface empty-state">No hay transferencias registradas en esta sesión.</div>`;
    return;
  }

  container.innerHTML = tasks
    .map((t) => {
      const pct = Math.min(100, Math.max(0, Number(t.progress_percent || 0)));
      const stateLower = String(t.state || "QUEUED").toLowerCase();
      const fillClass =
        t.state === "COMPLETED" ? " is-complete" : t.state === "FAILED" ? " is-error" : "";
      const queueLabel =
        t.queue_position !== null && t.queue_position !== undefined
          ? ` · Cola #${t.queue_position}`
          : "";
      const errHtml = t.error_message
        ? `<div class="muted" style="color:var(--brand-red)">${escapeHtml(t.error_message)}</div>`
        : "";
      const canPause = ["QUEUED", "TRANSFERRING", "RESUMING"].includes(t.state);
      const canResume = ["PAUSED", "FAILED"].includes(t.state);
      const canCancel = !["COMPLETED", "CANCELLED"].includes(t.state);
      return `
      <article class="transfer-row">
        <div class="transfer-head">
          <div>
            <div class="transfer-title">${escapeHtml(t.file_name)}</div>
            <div class="mono muted">${escapeHtml(t.file_hash.slice(0, 16))}… · Peer ${escapeHtml(t.remote_peer_id)}${escapeHtml(queueLabel)}</div>
          </div>
          <span class="pill pill-${escapeHtml(stateLower)}">${escapeHtml(t.state)}</span>
        </div>
        <div class="progress-track">
          <div class="progress-fill${fillClass}" style="width:${pct}%"></div>
        </div>
        ${errHtml}
        <div class="transfer-foot tabular">
          <span>${formatBytes(t.bytes_transferred)} / ${formatBytes(t.total_bytes)} (${pct.toFixed(1)}%)</span>
          <div class="btn-row">
            <button class="btn btn-sm btn-secondary" type="button" data-tx-action="pause" data-tx-id="${escapeHtml(t.task_id)}" ${canPause ? "" : "disabled"}>Pausar</button>
            <button class="btn btn-sm btn-secondary" type="button" data-tx-action="resume" data-tx-id="${escapeHtml(t.task_id)}" ${canResume ? "" : "disabled"}>Reanudar</button>
            <button class="btn btn-sm btn-danger" type="button" data-tx-action="cancel" data-tx-id="${escapeHtml(t.task_id)}" ${canCancel ? "" : "disabled"}>Cancelar</button>
            <button class="btn btn-sm btn-primary" type="button" data-view-transfer="${escapeHtml(t.task_id)}" ${t.state === "COMPLETED" ? "" : "disabled"}>Ver contenido</button>
          </div>
        </div>
      </article>`;
    })
    .join("");
}

/* 5. Communities, Debates, Rooms & Private Chat */
async function loadCommunityTab() {
  try {
    const select = document.getElementById("debate-comm-id");
    const commId = state.selectedCommunityId || (select ? select.value : "");
    const commUrl = commId
      ? `/api/communities?community_id=${encodeURIComponent(commId)}`
      : "/api/communities";
    const [commData, roomsData, pmData] = await Promise.all([
      api(commUrl),
      api("/api/rooms"),
      api("/api/chat/private"),
    ]);
    renderCommunities(commData);
    renderRoomMessages(roomsData.messages || []);
    renderPrivateChat(pmData);
  } catch (err) {
    showToast(err.message, true);
  }
}

function renderCommunities(data) {
  const comms = data.communities || [];
  const select = document.getElementById("debate-comm-id");
  const prevSelected = state.selectedCommunityId || (select ? select.value : "");

  const list = document.getElementById("communities-list");
  list.innerHTML = comms.length
    ? comms
        .map(
          (c) => `
          <li class="community-item${c.id === prevSelected ? " is-selected" : ""}" data-select-community="${escapeHtml(c.id)}">
            <strong>${escapeHtml(c.name)}</strong>
            <div class="muted">${escapeHtml(c.description || "Sin descripción")}</div>
          </li>`
        )
        .join("")
    : `<li class="muted">Sin comunidades creadas todavía.</li>`;

  select.innerHTML = comms.length
    ? comms.map((c) => `<option value="${escapeHtml(c.id)}">${escapeHtml(c.name)}</option>`).join("")
    : `<option value="">General</option>`;

  if (prevSelected && comms.some((c) => c.id === prevSelected)) {
    select.value = prevSelected;
    state.selectedCommunityId = prevSelected;
  } else if (comms.length) {
    select.value = comms[0].id;
    state.selectedCommunityId = comms[0].id;
  } else {
    state.selectedCommunityId = null;
  }

  const debates = data.debates || [];
  const debatesEl = document.getElementById("debates-list");
  if (!debates.length) {
    debatesEl.innerHTML = `<div class="muted">Sin debates en esta comunidad todavía.</div>`;
    return;
  }
  debatesEl.innerHTML = debates
    .map((d) => {
      const replies = d.replies || [];
      const repliesHtml = replies.length
        ? `<div class="debate-replies">${replies
            .map(
              (r) => `
              <div class="debate-reply">
                <div class="debate-reply-meta"><strong>${escapeHtml(r.author_username)}</strong> <span class="muted">${escapeHtml(r.title || "")}</span></div>
                <p style="margin:4px 0 0">${escapeHtml(r.body)}</p>
              </div>`
            )
            .join("")}</div>`
        : "";
      return `
      <article class="debate-item">
        <strong>${escapeHtml(d.title)}</strong> <span class="muted">por ${escapeHtml(d.author_username)}</span>
        <p style="margin:4px 0 8px">${escapeHtml(d.body)}</p>
        <div class="muted">${replies.length} respuestas</div>
        ${repliesHtml}
        <form class="inline-form debate-reply-form" data-reply-post-id="${escapeHtml(d.id)}">
          <input type="text" name="body" required placeholder="Responder al debate…" />
          <button class="btn btn-sm btn-secondary" type="submit">Responder</button>
        </form>
      </article>`;
    })
    .join("");
}

function renderRoomMessages(messages) {
  const log = document.getElementById("room-messages-log");
  if (!messages.length) {
    log.innerHTML = `<div class="muted">Sin mensajes en la sala ${escapeHtml(state.activeRoom)}.</div>`;
    return;
  }
  log.innerHTML = messages
    .map(
      (m) =>
        `<div class="chat-entry"><strong>${escapeHtml(m.sender_username)}</strong> <span class="muted">[${escapeHtml(m.room_id)}]</span>: ${escapeHtml(m.content)}</div>`
    )
    .join("");
  log.scrollTop = log.scrollHeight;
}

function renderPrivateChat(data) {
  const blocked = data.blocked_peers || [];
  document.getElementById("blocked-peers-bar").innerHTML = blocked
    .map(
      (pid) => `
      <span class="chip">
        <span>Bloqueado: ${escapeHtml(pid)}</span>
        <button type="button" data-unblock-peer="${escapeHtml(pid)}" aria-label="Desbloquear">×</button>
      </span>`
    )
    .join("");

  const msgs = data.messages || [];
  const log = document.getElementById("private-messages-log");
  if (!msgs.length) {
    log.innerHTML = `<div class="muted">Sin mensajes privados P2P registrados.</div>`;
    return;
  }
  log.innerHTML = msgs
    .map(
      (m) =>
        `<div class="chat-entry"><strong>${escapeHtml(m.sender_id)} → ${escapeHtml(m.recipient_id)}</strong>: ${escapeHtml(m.content)} <span class="muted">(${m.delivered ? "entregado" : "en cola offline"})</span></div>`
    )
    .join("");
  log.scrollTop = log.scrollHeight;
}

/* 6. Personal Library */
async function loadLibrary(refresh = true) {
  try {
    const data = await api(`/api/library?refresh=${refresh}`);
    const items = data.items || [];
    document.getElementById("library-count-label").textContent = `${items.length} elementos`;
    const tbody = document.getElementById("library-tbody");
    if (!items.length) {
      tbody.innerHTML = `<tr><td colspan="4" class="empty-cell">Aún no has guardado ni descargado recursos en tu biblioteca.</td></tr>`;
      return;
    }
    tbody.innerHTML = items
      .map(
        (item) => `
        <tr>
          <td><strong>${escapeHtml(item.title)}</strong></td>
          <td><span class="pill">${escapeHtml(item.source || "preserved")}</span></td>
          <td class="mono">${escapeHtml(item.file_hash.slice(0, 16))}…</td>
          <td class="actions-col">
            <button class="btn btn-sm btn-secondary" type="button" data-open-reviews="${escapeHtml(item.file_hash)}" data-file-name="${escapeHtml(item.title)}">Reseñar</button>
            ${item.source === "downloaded" ? `<button class="btn btn-sm btn-primary" type="button" data-view-download="${escapeHtml(item.file_hash)}">Ver contenido</button>` : ""}
          </td>
        </tr>`
      )
      .join("");
  } catch (err) {
    showToast(err.message, true);
  }
}

/* Local WebSocket (/ws/ui) */
function connectUiSocket() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${proto}//${location.host}/ws/ui`);
  state.ws = ws;

  ws.onopen = () => {
    document.getElementById("ws-dot").className = "status-dot dot-online";
    document.getElementById("ws-text").textContent = "WS local en tiempo real";
  };

  ws.onmessage = (ev) => {
    let msg;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      return;
    }
    if (msg.event === "snapshot" && msg.data) {
      (msg.data.transfers || []).forEach((t) => state.transfers.set(t.task_id, t));
      renderTransfers();
    } else if (
      ["transfer_state_changed", "transfer_progress", "transfer_error"].includes(msg.event) &&
      msg.data
    ) {
      state.transfers.set(msg.data.task_id, msg.data);
      renderTransfers();
      if (msg.event === "transfer_error") {
        showToast(`Error en ${msg.data.file_name}: ${msg.data.error_message}`, true);
      }
    } else if (msg.event === "presence_update") {
      loadSession();
    } else if (
      msg.event === "room_message" ||
      msg.event === "private_message" ||
      msg.event === "community_update" ||
      msg.event === "debate_update"
    ) {
      if (state.activeTab === "community") loadCommunityTab();
    } else if (msg.event === "library_update") {
      if (state.activeTab === "library") loadLibrary(false);
    }
  };

  ws.onclose = () => {
    document.getElementById("ws-dot").className = "status-dot dot-offline";
    document.getElementById("ws-text").textContent = "Reconectando WS local…";
    setTimeout(connectUiSocket, 2500);
  };
}

/* Event Wiring */
document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll(".nav-item").forEach((btn) => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  document.getElementById("btn-refresh-session").addEventListener("click", loadSession);

  document.getElementById("form-auth").addEventListener("submit", async (e) => {
    e.preventDefault();
    const username = document.getElementById("auth-username").value.trim();
    const password = document.getElementById("auth-password").value;
    try {
      await api("/api/session/login", {
        method: "POST",
        body: JSON.stringify({ username, password }),
      });
      showToast(`Sesión iniciada como ${username}`);
      await loadSession();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("btn-register").addEventListener("click", async () => {
    const username = document.getElementById("auth-username").value.trim();
    const password = document.getElementById("auth-password").value;
    try {
      await api("/api/session/register", {
        method: "POST",
        body: JSON.stringify({ username, password }),
      });
      showToast(`Cuenta registrada: ${username}`);
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("btn-logout").addEventListener("click", async () => {
    try {
      await api("/api/session/logout", { method: "POST" });
      showToast("Sesión cerrada");
      await loadSession();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.querySelectorAll(".seg-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      try {
        await api("/api/session/presence", {
          method: "POST",
          body: JSON.stringify({ status: btn.dataset.presence }),
        });
        await loadSession();
      } catch (err) {
        showToast(err.message, true);
      }
    });
  });

  document.getElementById("form-add-folder").addEventListener("submit", async (e) => {
    e.preventDefault();
    const input = document.getElementById("input-folder-path");
    try {
      const res = await api("/api/folders", {
        method: "POST",
        body: JSON.stringify({ path: input.value.trim() }),
      });
      input.value = "";
      showToast(`Carpeta indexada (${res.indexed_count} nuevos)`);
      await loadFolders();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("btn-reindex").addEventListener("click", async () => {
    try {
      const res = await api("/api/folders/reindex", { method: "POST" });
      showToast(`Reindexación completa (${res.total_files} archivos)`);
      await loadFolders();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("form-search").addEventListener("submit", (e) => {
    e.preventDefault();
    const q = document.getElementById("input-search-query").value.trim();
    if (q) runSearch(q);
  });

  document.getElementById("btn-refresh-transfers").addEventListener("click", loadTransfers);

  document.querySelectorAll("#star-picker .star-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.selectedRating = Number(btn.dataset.rating);
      document.querySelectorAll("#star-picker .star-btn").forEach((s) => {
        s.classList.toggle("is-selected", Number(s.dataset.rating) <= state.selectedRating);
      });
    });
  });

  document.getElementById("form-review").addEventListener("submit", async (e) => {
    e.preventDefault();
    const comment = document.getElementById("review-comment").value.trim();
    try {
      await api(`/api/reviews/${encodeURIComponent(state.reviewTargetHash)}`, {
        method: "POST",
        body: JSON.stringify({ rating: state.selectedRating, comment }),
      });
      document.getElementById("review-comment").value = "";
      showToast("Reseña publicada");
      await refreshReviewsModal();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("form-community").addEventListener("submit", async (e) => {
    e.preventDefault();
    const name = document.getElementById("comm-name").value.trim();
    const description = document.getElementById("comm-desc").value.trim();
    try {
      const created = await api("/api/communities", {
        method: "POST",
        body: JSON.stringify({ name, description }),
      });
      if (created && created.id) {
        state.selectedCommunityId = created.id;
      }
      document.getElementById("comm-name").value = "";
      document.getElementById("comm-desc").value = "";
      showToast(`Comunidad creada: ${name}`);
      await loadCommunityTab();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("debate-comm-id").addEventListener("change", (e) => {
    state.selectedCommunityId = e.target.value || null;
    loadCommunityTab();
  });

  document.getElementById("form-debate").addEventListener("submit", async (e) => {
    e.preventDefault();
    const commId = document.getElementById("debate-comm-id").value || "general";
    state.selectedCommunityId = commId;
    const title = document.getElementById("debate-title").value.trim();
    const body = document.getElementById("debate-body").value.trim();
    try {
      await api(`/api/communities/${encodeURIComponent(commId)}/debates`, {
        method: "POST",
        body: JSON.stringify({ title, body }),
      });
      document.getElementById("debate-title").value = "";
      document.getElementById("debate-body").value = "";
      showToast("Debate publicado");
      await loadCommunityTab();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("debates-list").addEventListener("submit", async (e) => {
    const form = e.target.closest(".debate-reply-form");
    if (!form) return;
    e.preventDefault();
    const postId = form.dataset.replyPostId;
    const input = form.querySelector("input[name='body']");
    const body = input ? input.value.trim() : "";
    if (!postId || !body) return;
    try {
      await api(`/api/debates/${encodeURIComponent(postId)}/replies`, {
        method: "POST",
        body: JSON.stringify({ body }),
      });
      if (input) input.value = "";
      showToast("Respuesta publicada");
      await loadCommunityTab();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("form-room-join").addEventListener("submit", async (e) => {
    e.preventDefault();
    const roomId = document.getElementById("room-join-id").value.trim();
    try {
      await api(`/api/rooms/${encodeURIComponent(roomId)}/join`, { method: "POST" });
      state.activeRoom = roomId;
      showToast(`Conectado a sala ${roomId}`);
      await loadCommunityTab();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("form-room-msg").addEventListener("submit", async (e) => {
    e.preventDefault();
    const input = document.getElementById("room-msg-input");
    try {
      await api(`/api/rooms/${encodeURIComponent(state.activeRoom)}/messages`, {
        method: "POST",
        body: JSON.stringify({ content: input.value.trim() }),
      });
      input.value = "";
      await loadCommunityTab();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("form-private-chat").addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await api("/api/chat/private", {
        method: "POST",
        body: JSON.stringify({
          peer_id: document.getElementById("pm-peer-id").value.trim(),
          peer_username: document.getElementById("pm-peer-username").value.trim(),
          peer_ip: document.getElementById("pm-peer-ip").value.trim(),
          peer_port: Number(document.getElementById("pm-peer-port").value),
          content: document.getElementById("pm-content").value.trim(),
        }),
      });
      document.getElementById("pm-content").value = "";
      await loadCommunityTab();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("btn-block-peer").addEventListener("click", async () => {
    const peerId = document.getElementById("pm-peer-id").value.trim();
    if (!peerId) return showToast("Indica un Peer ID para bloquear", true);
    try {
      await api(`/api/peers/${encodeURIComponent(peerId)}/block`, { method: "POST" });
      showToast(`Peer bloqueado: ${peerId}`);
      await loadCommunityTab();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  document.getElementById("form-library-save").addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await api("/api/library", {
        method: "POST",
        body: JSON.stringify({
          file_hash: document.getElementById("lib-hash").value.trim(),
          title: document.getElementById("lib-title").value.trim(),
        }),
      });
      document.getElementById("lib-hash").value = "";
      document.getElementById("lib-title").value = "";
      showToast("Recurso preservado en biblioteca");
      await loadLibrary();
    } catch (err) {
      showToast(err.message, true);
    }
  });

  /* Delegated clicks */
  document.addEventListener("click", async (e) => {
    const closeBtn = e.target.closest("[data-close-modal]");
    if (closeBtn) {
      document.getElementById(closeBtn.dataset.closeModal).close();
      return;
    }

    const revBtn = e.target.closest("[data-open-reviews]");
    if (revBtn) {
      openReviewsModal(revBtn.dataset.openReviews, revBtn.dataset.fileName);
      return;
    }

    const browseBtn = e.target.closest("[data-browse-peer]");
    if (browseBtn) {
      openBrowseModal(JSON.parse(browseBtn.dataset.browsePeer));
      return;
    }

    const dlBtn = e.target.closest("[data-download-item]");
    if (dlBtn) {
      startDownload(JSON.parse(dlBtn.dataset.downloadItem));
      return;
    }

    const viewTx = e.target.closest("[data-view-transfer]");
    const viewDl = e.target.closest("[data-view-download]");
    if (viewTx || viewDl) {
      const url = viewTx
        ? `/api/transfers/${encodeURIComponent(viewTx.dataset.viewTransfer)}/content`
        : `/api/downloads/${encodeURIComponent(viewDl.dataset.viewDownload)}/content`;
      try {
        const info = await api(url);
        document.getElementById("content-modal-title").textContent = info.file_name;
        document.getElementById("content-modal-meta").textContent =
          `${formatBytes(info.size_bytes)}${info.truncated ? " · mostrando los primeros 64 KB" : ""}`;
        document.getElementById("content-modal-body").textContent = info.is_text
          ? info.content
          : "Archivo binario: no se puede mostrar como texto.";
        document.getElementById("modal-content").showModal();
      } catch (err) {
        showToast(err.message, true);
      }
      return;
    }

    const txBtn = e.target.closest("[data-tx-action]");
    if (txBtn) {
      try {
        const updated = await api(
          `/api/transfers/${encodeURIComponent(txBtn.dataset.txId)}/${txBtn.dataset.txAction}`,
          { method: "POST" }
        );
        state.transfers.set(updated.task_id, updated);
        renderTransfers();
      } catch (err) {
        showToast(err.message, true);
      }
      return;
    }

    const rmDirBtn = e.target.closest("[data-remove-dir]");
    if (rmDirBtn) {
      try {
        await api("/api/folders", {
          method: "DELETE",
          body: JSON.stringify({ path: rmDirBtn.dataset.removeDir }),
        });
        await loadFolders();
      } catch (err) {
        showToast(err.message, true);
      }
      return;
    }

    const selectComm = e.target.closest("[data-select-community]");
    if (selectComm) {
      state.selectedCommunityId = selectComm.dataset.selectCommunity;
      loadCommunityTab();
      return;
    }

    const unblockBtn = e.target.closest("[data-unblock-peer]");
    if (unblockBtn) {
      try {
        await api(`/api/peers/${encodeURIComponent(unblockBtn.dataset.unblockPeer)}/unblock`, {
          method: "POST",
        });
        await loadCommunityTab();
      } catch (err) {
        showToast(err.message, true);
      }
    }
  });

  loadSession();
  loadTransfers();
  connectUiSocket();
});

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api } from "./api.js";
import { useTaskStream } from "./useTaskStream.js";

const STATUSES = ["pending", "running", "success", "failed", "dead_letter", "cancelled"];
const STATUS_LABEL = {
  pending: "En attente",
  running: "En cours",
  success: "Succes",
  failed: "Retry prevu",
  dead_letter: "Dead-letter",
  cancelled: "Annulee",
};

const EXAMPLES = {
  sleep: { seconds: 2 },
  flaky: { failure_rate: 0.5, seconds: 1 },
  send_email: { to: "demo@example.com", subject: "Bonjour", body: "Envoye par un worker." },
  resize_image: { url: "https://picsum.photos/1200/800", width: 300, height: 300 },
};

const PRESETS = [
  { label: "10 x sleep", type: "sleep", payload: { seconds: 2 }, count: 10 },
  { label: "5 x flaky 50%", type: "flaky", payload: { failure_rate: 0.5, seconds: 1 }, count: 5 },
  { label: "1 x toujours en echec", type: "flaky", payload: { failure_rate: 1, seconds: 0.5 }, count: 1 },
];

const MAX_ROWS = 200;

function timeAgo(iso) {
  const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (s < 60) return `il y a ${s}s`;
  if (s < 3600) return `il y a ${Math.floor(s / 60)} min`;
  return new Date(iso).toLocaleString("fr-FR");
}

function resultSummary(task) {
  const r = task.result;
  if (!r) return "";
  if (r.error) return r.retry_in ? `${r.error} (retry dans ${r.retry_in}s)` : r.error;
  return JSON.stringify(r);
}

function useNow(intervalMs) {
  const [, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
}

function StatusBadge({ status }) {
  return <span className={`badge badge-${status}`}>{STATUS_LABEL[status] ?? status}</span>;
}

function StatsBar({ stats, filter, onFilter }) {
  const by = stats?.by_status ?? {};
  return (
    <div className="stats">
      {STATUSES.map((s) => (
        <button
          key={s}
          className={`stat stat-${s} ${filter === s ? "active" : ""}`}
          onClick={() => onFilter(filter === s ? null : s)}
          title="Filtrer par statut"
        >
          <span className="stat-value">{by[s] ?? 0}</span>
          <span className="stat-label">{STATUS_LABEL[s]}</span>
        </button>
      ))}
      <div className="stat stat-queue">
        <span className="stat-value">{stats?.queue_depth ?? 0}</span>
        <span className="stat-label">Dans la file</span>
      </div>
      <div className="stat stat-queue">
        <span className="stat-value">{stats?.delayed ?? 0}</span>
        <span className="stat-label">Backoff</span>
      </div>
    </div>
  );
}

function SubmitForm({ onSubmitted, onError }) {
  const [type, setType] = useState("sleep");
  const [payload, setPayload] = useState(JSON.stringify(EXAMPLES.sleep, null, 2));
  const [maxAttempts, setMaxAttempts] = useState(3);
  const [count, setCount] = useState(1);
  const [busy, setBusy] = useState(false);

  const changeType = (t) => {
    setType(t);
    setPayload(JSON.stringify(EXAMPLES[t], null, 2));
  };

  const submit = async (task, n) => {
    setBusy(true);
    try {
      await Promise.all(Array.from({ length: n }, () => api.createTask(task)));
      onSubmitted();
    } catch (err) {
      onError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const onSubmit = (e) => {
    e.preventDefault();
    let parsed;
    try {
      parsed = JSON.parse(payload || "{}");
    } catch {
      onError("Payload : JSON invalide");
      return;
    }
    submit({ type, payload: parsed, max_attempts: Number(maxAttempts) }, Number(count));
  };

  return (
    <form className="panel form" onSubmit={onSubmit}>
      <h2>Nouvelle tache</h2>
      <label>
        Type
        <select value={type} onChange={(e) => changeType(e.target.value)}>
          {Object.keys(EXAMPLES).map((t) => (
            <option key={t}>{t}</option>
          ))}
        </select>
      </label>
      <label>
        Payload (JSON)
        <textarea value={payload} onChange={(e) => setPayload(e.target.value)} rows={6} spellCheck={false} />
      </label>
      <div className="row">
        <label>
          Max tentatives
          <input type="number" min={1} max={20} value={maxAttempts} onChange={(e) => setMaxAttempts(e.target.value)} />
        </label>
        <label>
          Nombre
          <input type="number" min={1} max={100} value={count} onChange={(e) => setCount(e.target.value)} />
        </label>
      </div>
      <button className="primary" disabled={busy}>
        {busy ? "Envoi..." : "Soumettre"}
      </button>
      <div className="presets">
        <span>Demos rapides</span>
        {PRESETS.map((p) => (
          <button
            type="button"
            key={p.label}
            disabled={busy}
            onClick={() => submit({ type: p.type, payload: p.payload, max_attempts: 3 }, p.count)}
          >
            {p.label}
          </button>
        ))}
      </div>
    </form>
  );
}

function TaskRow({ task, onRetry, onCancel }) {
  const [open, setOpen] = useState(false);
  const imageOutput = task.status === "success" && task.result?.output;
  return (
    <>
      <tr className={`task-row ${open ? "open" : ""}`} onClick={() => setOpen((o) => !o)}>
        <td className="mono" title={task.id}>
          {task.id.slice(0, 8)}
        </td>
        <td>{task.type}</td>
        <td>
          <StatusBadge status={task.status} />
        </td>
        <td className="num">
          {task.attempts}/{task.max_attempts}
        </td>
        <td className="muted">{timeAgo(task.updated_at)}</td>
        <td className="result" title={resultSummary(task)}>
          {resultSummary(task)}
        </td>
        <td className="actions" onClick={(e) => e.stopPropagation()}>
          {task.status === "dead_letter" && <button onClick={() => onRetry(task.id)}>Relancer</button>}
          {task.status === "pending" && (
            <button className="danger" onClick={() => onCancel(task.id)}>
              Annuler
            </button>
          )}
        </td>
      </tr>
      {open && (
        <tr className="details">
          <td colSpan={7}>
            <div className="details-grid">
              <div>
                <h4>Payload</h4>
                <pre>{JSON.stringify(task.payload, null, 2)}</pre>
              </div>
              <div>
                <h4>Resultat</h4>
                <pre>{JSON.stringify(task.result, null, 2) ?? "-"}</pre>
                {imageOutput && <img src={api.outputUrl(task.result.output)} alt="Image redimensionnee" />}
              </div>
              <div className="meta">
                <div>
                  <b>id</b> <span className="mono">{task.id}</span>
                </div>
                <div>
                  <b>cree</b> {new Date(task.created_at).toLocaleString("fr-FR")}
                </div>
                <div>
                  <b>maj</b> {new Date(task.updated_at).toLocaleString("fr-FR")}
                </div>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

export default function App() {
  const [tasks, setTasks] = useState(() => new Map());
  const [stats, setStats] = useState(null);
  const [filter, setFilter] = useState(null);
  const [error, setError] = useState(null);
  const [health, setHealth] = useState(null);
  const statsTimer = useRef(null);
  useNow(5000);

  const refreshStats = useCallback(() => {
    api.stats().then(setStats).catch(() => {});
  }, []);

  // Regroupe les rafales d'evenements en un seul appel /stats.
  const scheduleStats = useCallback(() => {
    clearTimeout(statsTimer.current);
    statsTimer.current = setTimeout(refreshStats, 250);
  }, [refreshStats]);

  const loadTasks = useCallback(() => {
    api
      .listTasks({ status: filter })
      .then((page) => setTasks(new Map(page.items.map((t) => [t.id, t]))))
      .catch((err) => setError(`Chargement impossible : ${err.message}`));
    refreshStats();
  }, [filter, refreshStats]);

  useEffect(loadTasks, [loadTasks]);

  useEffect(() => {
    const check = () => api.health().then(setHealth).catch(() => setHealth(null));
    check();
    const id = setInterval(check, 10000);
    return () => clearInterval(id);
  }, []);

  const onEvent = useCallback(
    (task) => {
      setTasks((prev) => {
        const next = new Map(prev);
        next.set(task.id, task);
        return next;
      });
      scheduleStats();
    },
    [scheduleStats],
  );

  const wsState = useTaskStream(onEvent, loadTasks);

  const visible = useMemo(
    () =>
      [...tasks.values()]
        .filter((t) => !filter || t.status === filter)
        .sort((a, b) => b.created_at.localeCompare(a.created_at))
        .slice(0, MAX_ROWS),
    [tasks, filter],
  );

  const act = (fn) => (id) => fn(id).catch((err) => setError(err.message));

  return (
    <div className="app">
      <header>
        <div>
          <h1>Job Scheduler</h1>
          <p className="subtitle">API FastAPI · file Redis · workers paralleles · PostgreSQL</p>
        </div>
        <div className="indicators">
          <span className={`pill ${wsState}`}>
            <i /> {wsState === "live" ? "Temps reel" : wsState === "connecting" ? "Connexion..." : "Reconnexion..."}
          </span>
          {health && (
            <span className={`pill ${health.status === "ok" ? "live" : "reconnecting"}`}>
              <i /> Redis {health.redis ? "OK" : "KO"} · DB {health.database ? "OK" : "KO"}
            </span>
          )}
        </div>
      </header>

      {error && (
        <div className="error" role="alert">
          {error}
          <button onClick={() => setError(null)} aria-label="Fermer">
            ×
          </button>
        </div>
      )}

      <StatsBar stats={stats} filter={filter} onFilter={setFilter} />

      <main>
        <SubmitForm onSubmitted={scheduleStats} onError={setError} />
        <section className="panel tasks">
          <div className="tasks-header">
            <h2>
              Taches {filter && <StatusBadge status={filter} />}
              {filter && (
                <button className="link" onClick={() => setFilter(null)}>
                  tout afficher
                </button>
              )}
            </h2>
            <span className="muted">{visible.length} affichees</span>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Type</th>
                  <th>Statut</th>
                  <th>Essais</th>
                  <th>Maj</th>
                  <th>Resultat / erreur</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {visible.map((t) => (
                  <TaskRow key={t.id} task={t} onRetry={act(api.retryTask)} onCancel={act(api.cancelTask)} />
                ))}
                {visible.length === 0 && (
                  <tr>
                    <td colSpan={7} className="empty">
                      Aucune tache. Soumettez-en une a gauche.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  );
}

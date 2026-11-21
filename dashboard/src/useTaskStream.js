import { useEffect, useRef, useState } from "react";
import { api } from "./api.js";

// Ouvre /ws/tasks et appelle onEvent(task) a chaque changement de statut.
// Reconnexion automatique avec backoff exponentiel (1s, 2s, 4s... max 15s).
export function useTaskStream(onEvent, onReconnect) {
  const [state, setState] = useState("connecting");
  const handlers = useRef({ onEvent, onReconnect });
  handlers.current = { onEvent, onReconnect };

  useEffect(() => {
    let ws;
    let retryTimer;
    let attempt = 0;
    let closed = false;

    const connect = () => {
      setState(attempt === 0 ? "connecting" : "reconnecting");
      ws = new WebSocket(api.wsUrl());
      ws.onopen = () => {
        if (attempt > 0) handlers.current.onReconnect?.();
        attempt = 0;
        setState("live");
      };
      ws.onmessage = (msg) => {
        try {
          handlers.current.onEvent(JSON.parse(msg.data));
        } catch {
          /* message invalide ignore */
        }
      };
      ws.onclose = () => {
        if (closed) return;
        setState("reconnecting");
        const delay = Math.min(15000, 1000 * 2 ** attempt);
        attempt += 1;
        retryTimer = setTimeout(connect, delay);
      };
    };

    connect();
    return () => {
      closed = true;
      clearTimeout(retryTimer);
      ws?.close();
    };
  }, []);

  return state;
}

"""API FastAPI du Job Scheduler."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.db import TaskRepository
from api.models import Health, Stats, TaskCreate, TaskCreated, TaskList, TaskOut
from api.queue import TaskQueue
from shared.config import Settings, get_settings
from shared.constants import EVENTS_CHANNEL, TaskStatus

log = logging.getLogger("api")


class EventHub:
    """Diffuse les evenements Redis Pub/Sub a tous les clients WebSocket connectes."""

    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self.clients.discard(ws)

    async def broadcast(self, message: str) -> None:
        for ws in list(self.clients):
            try:
                await ws.send_text(message)
            except Exception:
                self.disconnect(ws)


async def relay_events(queue: TaskQueue, hub: EventHub) -> None:
    """Boucle de fond : Redis Pub/Sub -> WebSockets. Se reconnecte en cas d'erreur."""
    while True:
        try:
            pubsub = queue.redis.pubsub()
            await pubsub.subscribe(EVENTS_CHANNEL)
            async for message in pubsub.listen():
                if message["type"] == "message":
                    await hub.broadcast(message["data"])
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("relay d'evenements interrompu, reconnexion dans 1s")
            await asyncio.sleep(1)


def create_app(
    repo: TaskRepository | None = None,
    queue: TaskQueue | None = None,
    settings: Settings | None = None,
    relay: bool = True,
) -> FastAPI:
    """Fabrique d'application. Les tests injectent `repo`/`queue` factices."""
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        owns_repo, owns_queue = repo is None, queue is None
        app.state.repo = repo or await TaskRepository.connect(settings.database_url)
        app.state.queue = queue or TaskQueue.from_url(settings.redis_url)
        await app.state.repo.init_schema()
        relay_task = asyncio.create_task(relay_events(app.state.queue, app.state.hub)) if relay else None
        try:
            yield
        finally:
            if relay_task:
                relay_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await relay_task
            if owns_queue:
                await app.state.queue.close()
            if owns_repo:
                await app.state.repo.close()

    app = FastAPI(
        title="Job Scheduler",
        description="File de taches asynchrones : API FastAPI + Redis + workers + PostgreSQL.",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.hub = EventHub()
    # Permet aux tests d'utiliser l'app sans lifespan.
    if repo is not None:
        app.state.repo = repo
    if queue is not None:
        app.state.queue = queue

    app.add_middleware(
        CORSMiddleware,
        allow_origins=os.environ.get("CORS_ORIGINS", "*").split(","),
        allow_methods=["*"],
        allow_headers=["*"],
    )

    os.makedirs(settings.output_dir, exist_ok=True)
    app.mount("/outputs", StaticFiles(directory=settings.output_dir), name="outputs")

    def get_repo(request: Request) -> TaskRepository:
        return request.app.state.repo

    def get_queue(request: Request) -> TaskQueue:
        return request.app.state.queue

    @app.post("/tasks", response_model=TaskCreated, status_code=201, tags=["tasks"])
    async def create_task(
        body: TaskCreate,
        repo: TaskRepository = Depends(get_repo),
        queue: TaskQueue = Depends(get_queue),
    ):
        """Cree la tache en base, la pousse dans Redis et rend la main immediatement."""
        max_attempts = body.max_attempts or settings.default_max_attempts
        task = await repo.create(body.type, body.payload, max_attempts)
        await queue.enqueue(str(task["id"]))
        await queue.publish(task)
        return TaskCreated(id=task["id"], status=task["status"])

    @app.get("/tasks", response_model=TaskList, tags=["tasks"])
    async def list_tasks(
        status: TaskStatus | None = None,
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
        repo: TaskRepository = Depends(get_repo),
    ):
        items, total = await repo.list(status.value if status else None, limit, offset)
        return TaskList(items=items, total=total, limit=limit, offset=offset)

    @app.get("/tasks/{task_id}", response_model=TaskOut, tags=["tasks"])
    async def get_task(task_id: UUID, repo: TaskRepository = Depends(get_repo)):
        task = await repo.get(task_id)
        if task is None:
            raise HTTPException(404, "tache introuvable")
        return task

    @app.post("/tasks/{task_id}/retry", response_model=TaskOut, tags=["tasks"])
    async def retry_task(
        task_id: UUID,
        repo: TaskRepository = Depends(get_repo),
        queue: TaskQueue = Depends(get_queue),
    ):
        """Relance manuellement une tache en dead_letter (compteur de tentatives remis a 0)."""
        task = await repo.reset_for_retry(task_id)
        if task is None:
            if await repo.get(task_id) is None:
                raise HTTPException(404, "tache introuvable")
            raise HTTPException(409, "seules les taches en dead_letter peuvent etre relancees")
        await queue.requeue_from_dead_letter(str(task_id))
        await queue.publish(task)
        return task

    @app.delete("/tasks/{task_id}", response_model=TaskOut, tags=["tasks"])
    async def cancel_task(
        task_id: UUID,
        repo: TaskRepository = Depends(get_repo),
        queue: TaskQueue = Depends(get_queue),
    ):
        """Annule une tache encore pending."""
        task = await repo.cancel(task_id)
        if task is None:
            if await repo.get(task_id) is None:
                raise HTTPException(404, "tache introuvable")
            raise HTTPException(409, "seules les taches pending peuvent etre annulees")
        await queue.remove(str(task_id))
        await queue.publish(task)
        return task

    @app.get("/stats", response_model=Stats, tags=["monitoring"])
    async def stats(repo: TaskRepository = Depends(get_repo), queue: TaskQueue = Depends(get_queue)):
        return Stats(by_status=await repo.count_by_status(), **await queue.depths())

    @app.get("/health", response_model=Health, tags=["monitoring"])
    async def health(
        response: Response,
        repo: TaskRepository = Depends(get_repo),
        queue: TaskQueue = Depends(get_queue),
    ):
        redis_ok, db_ok = await asyncio.gather(queue.ping(), repo.ping())
        ok = redis_ok and db_ok
        if not ok:
            response.status_code = 503
        return Health(status="ok" if ok else "degraded", api=True, redis=redis_ok, database=db_ok)

    @app.websocket("/ws/tasks")
    async def ws_tasks(ws: WebSocket):
        """Flux des changements de statut (un message JSON = l'etat complet d'une tache)."""
        hub: EventHub = ws.app.state.hub
        await hub.connect(ws)
        try:
            while True:
                await ws.receive_text()  # garde la connexion ouverte, detecte la deconnexion
        except WebSocketDisconnect:
            pass
        finally:
            hub.disconnect(ws)

    return app


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
app = create_app()  # aucune connexion n'est ouverte avant le demarrage (lifespan)

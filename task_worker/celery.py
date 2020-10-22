from celery import Celery
import globals


celery_app = Celery(
    "worker",
    backend=f'redis://:{globals.REDIS_PASSWORD}@{globals.REDIS_HOSTNAME}:{globals.REDIS_PORT}/{globals.REDIS_DB}',
    broker=f'redis://:{globals.REDIS_PASSWORD}@{globals.REDIS_HOSTNAME}:{globals.REDIS_PORT}/{globals.REDIS_DB}',
)

celery_app.conf.update(task_track_started=True)
celery_app.conf.imports = ['index_task']

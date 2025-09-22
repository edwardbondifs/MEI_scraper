from celery import Celery

# celery = Celery(
#     'mei_scraper',
#     broker='redis://redis:6379/0',   # 'redis' is the hostname for Redis in Docker Compose
#     backend='redis://redis:6379/0'
# )

celery = Celery(
    'mei_scraper',
    broker='memory://',
    backend='rpc://'
)
from celery_app import celery
from utils import process_cnpj_batch  # You will define this function

@celery.task
def process_cnpj_batch_task(cnpj_batch):
    return process_cnpj_batch(cnpj_batch)
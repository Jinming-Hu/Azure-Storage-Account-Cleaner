#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import json
from multiprocessing.dummy import Pool as ThreadPool

import azure.common
import azure.storage.common
from azure.storage.blob.baseblobservice import BaseBlobService
from azure.storage.file import FileService
from azure.storage.queue import QueueService
from azure.cosmosdb.table import TableService


def delete_container(blob_service, container_name):
    print("Delete container: {}/{}".format(blob_service.account_name, container_name))
    try:
        blob_service.delete_container(container_name)
    except azure.common.AzureHttpError as err:
        if "LeaseIdMissing" in err.message:
            blob_service.break_container_lease(container_name)
            blob_service.delete_container(container_name)


def delete_file_share(file_service, share_name):
    print("Delete share: {}/{}".format(file_service.account_name, share_name))
    file_service.delete_share(share_name)


def delete_queue(queue_service, queue_name):
    print("Delete queue: {}/{}".format(queue_service.account_name, queue_name))
    queue_service.delete_queue(queue_name)


def delete_table(table_service, table_name):
    print("Delete table: {}/{}".format(table_service.account_name, table_name))
    table_service.delete_table(table_name)


def clean_storage_account(connection_string):
    pool = ThreadPool(16)
    no_retry = azure.storage.common.retry.no_retry

    try:
        blob_service = BaseBlobService(connection_string=connection_string)
        blob_service.retry = no_retry
        pool.map(lambda container: delete_container(blob_service, container.name), blob_service.list_containers(timeout=3))
    except azure.common.AzureException:
        print("No blob service")

    try:
        file_service = FileService(connection_string=connection_string)
        file_service.retry = no_retry
        pool.map(lambda share: delete_file_share(file_service, share.name), file_service.list_shares(timeout=3))
    except azure.common.AzureException:
        print("No file service")

    try:
        queue_service = QueueService(connection_string=connection_string)
        queue_service.retry = no_retry
        pool.map(lambda queue: delete_queue(queue_service, queue.name), queue_service.list_queues(timeout=3))
    except azure.common.AzureException:
        print("No queue service")

    try:
        table_service = TableService(connection_string=connection_string)
        table_service.retry = no_retry
        pool.map(lambda table: delete_table(table_service, table.name), table_service.list_tables(timeout=3))
    except azure.common.AzureException:
        print("No table service")


if len(sys.argv) > 1 and sys.argv[1]:
    print("Cleaning...")
    clean_storage_account(sys.argv[1])

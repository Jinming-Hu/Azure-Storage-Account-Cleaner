#!/usr/bin/env python3

import os
import sys
import json
import socket
import urllib.parse
from multiprocessing.dummy import Pool as ThreadPool

import azure.core
from azure.storage.blob import BlobServiceClient, BlobLeaseClient
from azure.storage.fileshare import ShareServiceClient
from azure.storage.queue import QueueServiceClient
from azure.data.tables import TableServiceClient


def delete_container(blob_service, container_name):
    print("Delete container: {}/{}".format(blob_service.account_name, container_name))
    try:
        blob_service.delete_container(container_name)
    except azure.core.exceptions.HttpResponseError as err:
        if "LeaseIdMissing" in err.message:
            lease_client = BlobLeaseClient(
                blob_service.get_container_client(container_name))
            lease_client.break_lease()
            blob_service.delete_container(container_name)
    except azure.core.exceptions.ResourceExistsError as err:
        print(err)
        pass


def delete_file_share(file_service, share_name):
    print("Delete share: {}/{}".format(file_service.account_name, share_name))
    file_service.delete_share(share_name)


def delete_queue(queue_service, queue_name):
    print("Delete queue: {}/{}".format(queue_service.account_name, queue_name))
    queue_service.delete_queue(queue_name)


def delete_table(table_service, table_name):
    print("Delete table: {}/{}".format(table_service.account_name, table_name))
    table_service.delete_table(table_name)


def service_exists(service_client):
    try:
        socket.getaddrinfo(urllib.parse.urlparse(
            service_client.url).netloc, 443)
    except socket.gaierror:
        return False
    return True


def clean_storage_account(connection_string):
    pool = ThreadPool(16)

    blob_service = BlobServiceClient.from_connection_string(connection_string)
    if service_exists(blob_service):
        pool.map(lambda container: delete_container(blob_service,
                 container.name), blob_service.list_containers())

    file_service = ShareServiceClient.from_connection_string(connection_string)
    if service_exists(file_service):
        pool.map(lambda share: delete_file_share(
            file_service, share.name), file_service.list_shares())

    queue_service = QueueServiceClient.from_connection_string(
        connection_string)
    if service_exists(queue_service):
        pool.map(lambda queue: delete_queue(queue_service,
                 queue.name), queue_service.list_queues())

    table_service = TableServiceClient.from_connection_string(
        connection_string)
    if service_exists(table_service):
        try:
            pool.map(lambda table: delete_table(table_service,
                     table.name), table_service.list_tables())
        except azure.core.exceptions.HttpResponseError as e:
            pass


if len(sys.argv) > 1 and sys.argv[1]:
    print("Cleaning...")
    clean_storage_account(sys.argv[1])

# -*- coding: utf-8 -*-

import asyncio
import os
from typing import List, Dict

import boto3
import mcp.server.stdio
from botocore.exceptions import ClientError
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.types import Tool, Resource, TextContent

server = Server("mcp-s3-server")
client = None


async def load_config():
    """加载配置文件

    Returns:
        dict: 配置文件内容
    """
    config = {
        "endpoint": os.getenv("ENDPOINT"),
        "access_key_id": os.getenv("ACCESS_KEY_ID"),
        "access_key-secret": os.getenv("ACCESS_KEY_SECRET"),
        "region_name": os.getenv("REGION_NAME", "us-east-1"),
    }

    if not all([config["endpoint"], config["access_key_id"], config["access_key-secret"]]):
        raise ValueError("Missing required configuration")

    return config


async def init_client():
    global client
    config = await load_config()
    client = boto3.client(
        's3',
        endpoint_url=config.get('endpoint'),
        aws_access_key_id=config.get('access_key_id'),
        aws_secret_access_key=config.get('access_key-secret'),
        region_name=config.get('region_name')
    )


@server.list_resources()
async def list_resources() -> list[Resource]:
    """列出所有桶

    Returns:
        list[Resource]: 桶列表
    """
    try:
        buckets = client.list_buckets()
        resources = []
        for bucket in buckets['Buckets']:
            resources.append(
                Resource(
                    uri=f"s3://{bucket['Name']}",
                    name=f"Bucket: {bucket['Name']}",
                    mimeType="text/plain",
                    description=f"Data in bucket: {bucket['Name']}"
                )
            )

        return resources
    except RuntimeError:
        return []


@server.list_tools()
async def handle_list_tools() -> List[Tool]:
    return [
        Tool(
            name="list-buckets",
            description="List all buckets with permissions in the object storage",
            inputSchema={
                "type": "object",
                "properties": {
                }
            }
        ),
        Tool(
            name="exists-bucket",
            description="Check if a bucket exists",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "description": "The name of the bucket to check"
                    }
                },
                "required": ["bucket_name"]
            }
        ),
        Tool(
            name="create-bucket",
            description="Create a new bucket with permissions in the object storage",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "description": "The name of the bucket to create"
                    }
                },
                "required": ["bucket_name"]
            }
        ),
        Tool(
            name="delete-bucket",
            description="Delete a bucket with permissions in the object storage",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "description": "The name of the bucket to delete"
                    }
                },
                "required": ["bucket_name"]
            }
        ),
        Tool(
            name="list-objects",
            description="List all objects in a bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "description": "The name of the bucket to list objects from"
                    },
                    "prefix": {
                        "type": "string",
                        "description": "The prefix to filter objects by"
                    },
                    "delimiter": {
                        "type": "string",
                        "description": "The delimiter to use when listing objects"
                    },
                    "max_keys": {
                        "type": "integer",
                        "description": "The maximum number of keys to return"
                    },
                    "continuation_token": {
                        "type": "string",
                        "description": "The continuation token to use when listing objects"
                    },
                    "start_after": {
                        "type": "string",
                        "description": "The key to start listing objects after"
                    }
                },
                "required": ["bucket_name"]
            }
        ),
        Tool(
            name="get-object",
            description="Get an object from a bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "description": "The name of the bucket to get the object from"
                    },
                    "key": {
                        "type": "string",
                        "description": "The key of the object to get"
                    },
                    "version_id": {
                        "type": "string",
                        "description": "The version ID of the object to get"
                    },
                    "path": {
                        "type": "string",
                        "description": "The path to save the object to"
                    }
                },
                "required": ["bucket_name", "key", "path"]
            }
        ),
        Tool(
            name="put-object",
            description="Put an object into a bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "description": "The name of the bucket to put the object into"
                    },
                    "key": {
                        "type": "string",
                        "description": "The key of the object to put"
                    },
                    "path": {
                        "type": "string",
                        "description": "The path to the object to put"
                    }
                },
                "required": ["bucket_name", "key", "path"]
            }
        ),
        Tool(
            name="delete-object",
            description="Delete an object from a bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "description": "The name of the bucket to delete the object from"
                    },
                    "key": {
                        "type": "string",
                        "description": "The key of the object to delete"
                    },
                    "version_id": {
                        "type": "string",
                        "description": "The version ID of the object to delete"
                    }
                },
                "required": ["bucket_name", "key"]
            }
        ),
        Tool(
            name="get-object-metadata",
            description="Get object metadata from a bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "description": "The name of the bucket to get the object metadata from"
                    },
                    "key": {
                        "type": "string",
                        "description": "The key of the object to get metadata for"
                    },
                    "version_id": {
                        "type": "string",
                        "description": "The version ID of the object to get metadata for"
                    }
                },
                "required": ["bucket_name", "key"]
            }
        )
    ]


class ToolHandler:
    async def handle(self, name: str, args: Dict | None) -> List[TextContent]:
        raise NotImplementedError


class ListBucketsHandler(ToolHandler):
    """
    列出所有桶
    """

    async def handle(self, name: str, args: Dict | None) -> List[TextContent]:
        try:
            buckets = client.list_buckets()

            return [
                TextContent(
                    type="text",
                    text=f"Bucket: {bucket['Name']}"
                )
                for bucket in buckets['Buckets']
            ]
        except RuntimeError:
            return [TextContent(type="text", text="Failed to list buckets")]


class ExistsBucketHandler(ToolHandler):
    """
    检查桶是否存在
    """

    async def handle(self, name: str, args: Dict | None) -> List[TextContent]:
        bucket_name = args.get("bucket_name")
        try:
            client.head_bucket(Bucket=bucket_name)
            return [TextContent(type="text", text=f"Bucket {bucket_name} exists")]
        except ClientError:
            return [TextContent(type="text", text=f"Bucket {bucket_name} not exists")]


class CreateBucketHandler(ToolHandler):
    """
    创建桶
    """

    async def handle(self, name: str, args: Dict | None) -> List[TextContent]:
        try:
            bucket_name = args.get("bucket_name")
            client.create_bucket(Bucket=bucket_name)
            return [TextContent(type="text", text=f"Bucket {bucket_name} created successfully")]
        except RuntimeError:
            return [TextContent(type="text", text="Failed to create bucket")]


class DeleteBucketHandler(ToolHandler):
    """
    删除桶
    """

    async def handle(self, name: str, args: Dict | None) -> List[TextContent]:
        try:
            bucket_name = args.get("bucket_name")
            client.delete_bucket(Bucket=bucket_name)
            return [TextContent(type="text", text=f"Bucket {bucket_name} deleted successfully")]
        except RuntimeError:
            return [TextContent(type="text", text="Failed to delete bucket")]


class ListObjectsHandler(ToolHandler):
    """
    列出所有对象
    """

    async def handle(self, name: str, args: Dict | None) -> List[TextContent]:
        try:
            bucket_name = args.get("bucket_name")
            prefix = args.get("prefix")
            delimiter = args.get("delimiter")
            max_keys = args.get("max_keys", 1000)
            continuation_token = args.get("continuation_token")
            start_after = args.get("start_after")

            kwargs = {'Bucket': bucket_name}

            if prefix is not None:
                kwargs['Prefix'] = prefix
            if delimiter is not None:
                kwargs['Delimiter'] = delimiter
            if max_keys is not None:
                kwargs['MaxKeys'] = max_keys
            if continuation_token is not None:
                kwargs['ContinuationToken'] = continuation_token
            if start_after is not None:
                kwargs['StartAfter'] = start_after

            response = client.list_objects_v2(**kwargs)
            return [
                TextContent(
                    type="text",
                    text=f"Object[key={obj.get('Key', 'null')}, version_id={obj.get('VersionId', 'null')}]"
                )
                for obj in response['Contents']
            ]
        except ClientError:
            return [TextContent(type="text", text="Failed to list objects")]


class GetObjectHandler(ToolHandler):
    """
    获取对象, 并保存到本地
    """

    async def handle(self, name: str, args: Dict | None) -> List[TextContent]:
        try:
            bucket_name = args.get("bucket_name")
            key = args.get("key")
            version_id = args.get("version_id")
            path = args.get("path")
            kwargs = {'Bucket': bucket_name, 'Key': key}
            if version_id is not None:
                kwargs['VersionId'] = version_id
            response = client.get_object(**kwargs)
            with open(path, 'wb') as f:
                f.write(response['Body'].read())
                f.close()
                return [TextContent(type="text", text=f"Object {key} saved successfully")]
        except ClientError:
            return [TextContent(type="text", text="Failed to get object")]


class PutObjectHandler(ToolHandler):
    """
    上传对象
    """

    async def handle(self, name: str, args: Dict | None) -> List[TextContent]:
        try:
            bucket_name = args.get("bucket_name")
            key = args.get("key")
            path = args.get("path")
            kwargs = {'Bucket': bucket_name, 'Key': key}
            with open(path, 'rb') as f:
                client.put_object(**kwargs, Body=f)
                f.close()
                return [TextContent(type="text", text=f"Object {key} saved successfully")]
        except ClientError:
            return [TextContent(type="text", text="Failed to put object")]


class DeleteObjectHandler(ToolHandler):
    """
    删除对象
    """

    async def handle(self, name: str, args: Dict | None) -> List[TextContent]:
        try:
            bucket_name = args.get("bucket_name")
            key = args.get("key")
            version_id = args.get("version_id")
            kwargs = {'Bucket': bucket_name, 'Key': key}
            if version_id is not None:
                kwargs['VersionId'] = version_id
            client.delete_object(**kwargs)
            return [TextContent(type="text", text=f"Object {key} deleted successfully")]
        except ClientError:
            return [TextContent(type="text", text="Failed to delete object")]


class GetObjectMetadataHandler(ToolHandler):
    """
    获取对象元数据
    """

    async def handle(self, name: str, args: Dict | None) -> List[TextContent]:
        try:
            bucket_name = args.get("bucket_name")
            key = args.get("key")
            version_id = args.get("version_id")
            kwargs = {'Bucket': bucket_name, 'Key': key}
            if version_id is not None:
                kwargs['VersionId'] = version_id
            obj = client.head_object(**kwargs)
            return [
                TextContent(
                    type="text",
                    text=f"Metadata[content_type={obj.get('ContentType', '')}, "
                         f"content_length={obj.get('ContentLength', '')},"
                         f"last_modified={obj.get('LastModified', '')}]"
                )
            ]
        except ClientError:
            return [TextContent(type="text", text="Failed to get object metadata")]


tool_handlers = {
    "list-buckets": ListBucketsHandler(),
    "create-bucket": CreateBucketHandler(),
    "delete-bucket": DeleteBucketHandler(),
    "exists-bucket": ExistsBucketHandler(),
    "list-objects": ListObjectsHandler(),
    "get-object": GetObjectHandler(),
    "put-object": PutObjectHandler(),
    "delete-object": DeleteObjectHandler(),
    "get-object-metadata": GetObjectMetadataHandler(),
}


@server.call_tool()
async def handle_call_tool(name: str, args: Dict | None) -> List[TextContent]:
    if name in tool_handlers:
        return await tool_handlers[name].handle(name, args)
    else:
        raise ValueError(f"Unknown tool: {name}")


async def main():
    await init_client()
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="mcp-s3-server",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={}
                )
            )
        )


if __name__ == "__main__":
    asyncio.run(main())

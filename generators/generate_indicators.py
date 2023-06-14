#!/usr/bin/python
"""
Indicator generator to harvest information from endpoints and generate catalog

"""

from harvester.endpoint import create_stacapi_endpoint
from harvester.model import ResourceConfig, ResourceType, STACAPIConfig, QueryConfig, TimeConfig
from pystac_client import Client

from sh_endpoint import get_SH_token

from pystac import (
    Item,
    Asset,
    Catalog,
    # StacIO,
    CatalogType,
    Collection,
    Extent,
    SpatialExtent,
    TemporalExtent,
    MediaType,
)
from pystac.layout import TemplateLayoutStrategy

from vs_common.stac import (
    # create_item,
    # get_item,
    # get_or_create_catalog,
    get_or_create_collection,
    Item,
    # Asset,
)


import os
from pathlib import Path
from datetime import datetime
import yaml
from yaml.loader import SafeLoader

def process_catalog_file(file_path):
    print("Processing catalog:", file_path)
    with open(file_path) as f:
        data = yaml.load(f, Loader=SafeLoader)
        catalog = Catalog(
            id = data["id"],
            description = data["description"],
            title = data["title"],
            catalog_type=CatalogType.RELATIVE_PUBLISHED,
        )
        for collection in data["collections"]:
            process_collection_file("../collections/%s.yaml"%(collection), catalog)

        strategy = TemplateLayoutStrategy(item_template="${collection}/${year}")
        catalog.normalize_hrefs(data["endpoint"], strategy=strategy)
        catalog.save(dest_href="../build/%s"%data["id"])

def process_collection_file(file_path, catalog):
    print("Processing collection:", file_path)
    with open(file_path) as f:
        data = yaml.load(f, Loader=SafeLoader)
        for resource in data["Resources"]:
            if "EndPoint" in resource:
                if resource["Name"] == "Sentinel Hub":
                    handle_SH_endpoint(resource, data, catalog)
                elif resource["Name"] == "GeoDB":
                    handle_GeoDB_endpoint(resource, data, catalog)
                elif resource["Name"] == "VEDA":
                    handle_VEDA_endpoint(resource, data, catalog)
                else:
                    raise ValueError("Type of Resource is not supported")

def handle_SH_endpoint(endpoint, data, catalog):
    token = get_SH_token()
    headers = {"Authorization": "Bearer %s"%token}
    endpoint["EndPoint"] = "https://services.sentinel-hub.com/api/v1/catalog/1.0.0/"
    endpoint["CollectionId"] = endpoint["Type"]
    process_STACAPI_Endpoint(
        endpoint=endpoint,
        data=data,
        catalog=catalog,
        headers=headers,
    )

def handle_GeoDB_endpoint(endpoint, data, catalog):
    print(endpoint)

def handle_VEDA_endpoint(endpoint, data, catalog):
    process_STACAPI_Endpoint(
        endpoint=endpoint,
        data=data,
        catalog=catalog,
    )

def process_STACAPI_Endpoint(endpoint, data, catalog, headers={}):
    spatial_extent = SpatialExtent([
        [-180.0, -90.0, 180.0, 90.0],
    ])
    temporal_extent = TemporalExtent([[datetime.now()]])
    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)

    collection = Collection(
        id=endpoint["CollectionId"],
        description=data["Description"],
        extent=extent
    )
    if collection not in catalog.get_all_collections():
        catalog.add_child(collection)

    api = Client.open(endpoint["EndPoint"], headers=headers)
    bbox = "-180,-90,180,90"
    if "bbox" in endpoint:
        bbox = endpoint["bbox"]

    results = api.search(
        collections=[endpoint["CollectionId"]],
        bbox=bbox,
        datetime=['1970-01-01T00:00:00Z', '3000-01-01T00:00:00Z'],
    )
    for item in results.items_as_dicts():
        collection.add_item(Item.from_dict(item))
    
    collection.update_extent_from_items()
    # replace SH identifier with catalog identifier
    collection.id = data["Name"]
    # Add metadata information
    collection.license = data["License"]
    if "Story" in data:
        collection.add_asset(
            "metadata",
            Asset(
                href=data["Story"],
                media_type=MediaType.TEXT,
                roles=["metadata"],
            ),
        )

def process_catalogs(folder_path):
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            process_catalog_file(file_path)


folder_path = "../catalogs/"
process_catalogs(folder_path)
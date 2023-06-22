#!/usr/bin/python
"""
Indicator generator to harvest information from endpoints and generate catalog

"""

from pystac_client import Client

from sh_endpoint import get_SH_token

from pystac import (
    Item,
    Asset,
    Catalog,
    Link,
    # StacIO,
    CatalogType,
    Collection,
    Extent,
    SpatialExtent,
    TemporalExtent,
    MediaType,
)
from pystac.layout import TemplateLayoutStrategy

import os
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

def addVisualizationInfo(collection: Collection, data, endpoint):
    # add extension reference
    if endpoint["Name"] == "Sentinel Hub":
        instanceId = os.getenv("SH_INSTANCE_ID")
        if "InstanceId" in endpoint:
            instanceId = endpoint["InstanceId"]
        collection.add_link(
            Link(
                rel="wms",
                target="https://services.sentinel-hub.com/ogc/wms/%s"%(instanceId),
                media_type="text/xml",
                title=data["Name"],
                extra_fields={
                    "wms:layers": [endpoint["LayerId"]],
                },
            )
        )
    # elif resource["Name"] == "GeoDB":
    #     pass
    # elif resource["Name"] == "VEDA":
    #     pass
    else:
        pass

def process_STACAPI_Endpoint(endpoint, data, catalog, headers={}):
    spatial_extent = SpatialExtent([
        [-180.0, -90.0, 180.0, 90.0],
    ])
    temporal_extent = TemporalExtent([[datetime.now()]])
    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)

    collection = Collection(
        id=endpoint["CollectionId"],
        title=data["Title"],
        description=data["Description"],
        stac_extensions=[
            "https://stac-extensions.github.io/web-map-links/v1.1.0/schema.json",
        ],
        extent=extent
    )

    addVisualizationInfo(collection, data, endpoint)

    if collection not in catalog.get_all_collections():
        link = catalog.add_child(collection)
        # bubble fields we want to have up to collection link
        link.extra_fields["endpointtype"] = endpoint["Name"]
        link.extra_fields["description"] = collection.description
        link.extra_fields["title"] = collection.title
        link.extra_fields["code"] = data["EodashIdentifier"]
        link.extra_fields["themes"] = ",".join(data["Themes"])
        if "tags" in data:
            link.extra_fields["tags"] = ",".join(data["Tags"])
        if "satellite" in data:
            link.extra_fields["satellite"] = ",".join(data["Satellite"])
        if "sensor" in data:
            link.extra_fields["sensor"] = ",".join(data["Sensor"])
        if "agency" in data:
            link.extra_fields["agency"] = ",".join(data["Agency"])

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
        item = Item.from_dict(item)
        link = collection.add_item(item)
        # bubble up information we want to the link
        link.extra_fields["datetime"] = item.get_datetime().isoformat()[:-6] + 'Z'
        
    collection.update_extent_from_items()
    
    # replace SH identifier with catalog identifier
    collection.id = data["Name"]
    # Add metadata information
    # collection.license = data["License"]
    # TODO: need to review check against SPDX License identifier
    if "Story" in data:
        collection.add_asset(
            "metadata",
            Asset(
                href="../../%s"%data["Story"],
                media_type="text/markdown",
                roles=["metadata"],
            ),
        )
    if "Image" in data:
        collection.add_asset(
            "thumbnail",
            Asset(
                href="../../../assets/%s"%data["Image"],
                media_type="image/png",
                roles=["thumbnail"],
            ),
        )

    # validate collection after creation
    try:
        print(collection.validate())
    except Exception as e:
        print("Issue validationg collection: %s"%e)

def process_catalogs(folder_path):
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            process_catalog_file(file_path)


folder_path = "../catalogs/"
process_catalogs(folder_path)
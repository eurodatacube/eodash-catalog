#!/usr/bin/python
"""
Indicator generator to harvest information from endpoints and generate catalog

"""

import requests
import json
from pystac_client import Client
import os
from datetime import datetime
import yaml
from yaml.loader import SafeLoader
import urllib.parse
from itertools import groupby
from operator import itemgetter
from dateutil import parser
from sh_endpoint import get_SH_token
from utils import (
    create_geojson_point,
    retrieveExtentFromWMS,
)
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
    # MediaType,
    Summaries
)
from pystac.layout import TemplateLayoutStrategy


def process_catalog_file(file_path):
    print("Processing catalog:", file_path)
    with open(file_path) as f:
        config = yaml.load(f, Loader=SafeLoader)
        catalog = Catalog(
            id = config["id"],
            description = config["description"],
            title = config["title"],
            catalog_type=CatalogType.RELATIVE_PUBLISHED,
        )
        for collection in config["collections"]:
            process_collection_file(config, "../collections/%s.yaml"%(collection), catalog)

        strategy = TemplateLayoutStrategy(item_template="${collection}/${year}")
        catalog.normalize_hrefs(config["endpoint"], strategy=strategy)
        catalog.save(dest_href="../build/%s"%config["id"])

def process_collection_file(config, file_path, catalog):
    print("Processing collection:", file_path)
    with open(file_path) as f:
        data = yaml.load(f, Loader=SafeLoader)
        for resource in data["Resources"]:
            if "EndPoint" in resource:
                if resource["Name"] == "Sentinel Hub":
                    handle_SH_endpoint(config, resource, data, catalog)
                elif resource["Name"] == "GeoDB":
                    collection = handle_GeoDB_endpoint(config, resource, data)
                    add_to_catalog(collection, catalog, resource, data)
                elif resource["Name"] == "VEDA":
                    handle_VEDA_endpoint(config, resource, data, catalog)
                elif resource["Name"] == "WMS":
                    handle_WMS_endpoint(config, resource, data, catalog)
                else:
                    raise ValueError("Type of Resource is not supported")

def handle_WMS_endpoint(config, endpoint, data, catalog):
    if endpoint["Type"] == "Time" or endpoint["Type"] == "OverwriteTimes":

        times = []
        extent = retrieveExtentFromWMS(endpoint["EndPoint"], endpoint["LayerId"])
        if endpoint["Type"] == "OverwriteTimes":
            times = endpoint["Times"]
        else:
            times = extent["temporal"]
        if len(times) > 0:
            # Create an item per time to allow visualization in stac clients
            styles = None
            if hasattr(endpoint, "Styles"):
                styles = endpoint["Styles"]
            collection = create_collection(data["Name"], data)
            for t in times:
                item = Item(
                    id = t,
                    bbox=extent["spatial"],
                    properties={},
                    geometry = None,
                    datetime = parser.isoparse(t),
                )
                add_visualization_info(item, data, endpoint, time=t, styles=styles)
                link = collection.add_item(item)
                link.extra_fields["datetime"] = t
            collection.update_extent_from_items()
            add_visualization_info(collection, data, endpoint, styles=styles)
            add_to_catalog(collection, catalog, endpoint, data)
    else:
        # TODO: Implement
        print("Currently not supported")

def handle_SH_endpoint(config, endpoint, data, catalog):
    token = get_SH_token()
    headers = {"Authorization": "Bearer %s"%token}
    endpoint["EndPoint"] = "https://services.sentinel-hub.com/api/v1/catalog/1.0.0/"
    endpoint["CollectionId"] = endpoint["Type"] + "-" + endpoint["CollectionId"]
    # Check if we have Locations in config, if yes we divide the collection
    # into multiple collections based on their area
    if "Locations" in data:
        root_collection = create_collection(data["Name"], data)
        for location in data["Locations"]:
            collection = process_STACAPI_Endpoint(
                config=config,
                endpoint=endpoint,
                data=data,
                catalog=catalog,
                headers=headers,
                bbox=",".join(map(str,location["Bbox"])),
                root_collection=root_collection,
            )
            # Update identifier to use location as well as title
            collection.id = location["Identifier"]
            collection.title = location["Name"],
            # See if description should be overwritten
            if "Description" in location:
                collection.description = location["Description"]
            # TODO: should we remove all assets from sub collections?
            link = root_collection.add_child(collection)
            latlng = "%s,%s"%(location["Point"][1], location["Point"][0])
            # Add extra properties we need
            link.extra_fields["id"] = location["Identifier"]
            link.extra_fields["latlng"] = latlng
            link.extra_fields["name"] = location["Name"]
        root_collection.update_extent_from_items()
        # Add bbox extents from children
        for c_child in root_collection.get_children():
            root_collection.extent.spatial.bboxes.append(
                c_child.extent.spatial.bboxes[0]
            )
    else:
        root_collection = process_STACAPI_Endpoint(
            config=config,
            endpoint=endpoint,
            data=data,
            catalog=catalog,
            headers=headers,
        )
    add_to_catalog(root_collection, catalog, endpoint, data)

def create_collection(collection_id, data):
    spatial_extent = SpatialExtent([
        [-180.0, -90.0, 180.0, 90.0],
    ])
    temporal_extent = TemporalExtent([[datetime.now()]])
    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)

    collection = Collection(
        id=collection_id,
        title=data["Title"],
        description=data["Description"],
        stac_extensions=[
            "https://stac-extensions.github.io/web-map-links/v1.1.0/schema.json",
        ],
        extent=extent
    )
    return collection

def add_to_catalog(collection, catalog, endpoint, data):
    link = catalog.add_child(collection)
    # bubble fields we want to have up to collection link
    link.extra_fields["endpointtype"] = endpoint["Name"]
    link.extra_fields["description"] = collection.description
    link.extra_fields["title"] = collection.title
    link.extra_fields["code"] = data["EodashIdentifier"]
    link.extra_fields["themes"] = ",".join(data["Themes"])
    # Check for summaries and bubble up info
    if collection.summaries.lists:
        for sum in collection.summaries.lists:
            link.extra_fields[sum] = collection.summaries.lists[sum]
    if "Locations" in data:
        link.extra_fields["locations"] = True
    if "Tags" in data:
        link.extra_fields["tags"] = ",".join(data["Tags"])
    if "Satellite" in data:
        link.extra_fields["satellite"] = ",".join(data["Satellite"])
    if "Sensor" in data:
        link.extra_fields["sensor"] = ",".join(data["Sensor"])
    if "Agency" in data:
        link.extra_fields["agency"] = ",".join(data["Agency"])
    return link


def handle_GeoDB_endpoint(config, endpoint, data):
    collection = create_collection(endpoint["CollectionId"], data)
    select = "?select=aoi,aoi_id,country,city,time"
    url = endpoint["EndPoint"] + endpoint["Database"] + "_%s"%endpoint["CollectionId"] + select
    response = json.loads(requests.get(url).text)

    # Sort locations by key
    sorted_locations = sorted(response, key = itemgetter('aoi_id'))
    cities = []
    countries = []
    for key, value in groupby(sorted_locations, key = itemgetter('aoi_id')):
        # Finding min and max values for date
        values = [v for v in value]
        times = [datetime.fromisoformat(t["time"]) for t in values]
        unique_values = list({v["aoi_id"]:v for v in values}.values())[0]
        country = unique_values["country"]
        city = unique_values["city"]
        if country not in countries:
            countries.append(country)
        if city not in cities:
            cities.append(city)
        min_date = min(times)
        max_date = max(times)
        latlon = unique_values["aoi"]
        [lat, lon] = [float(x) for x in latlon.split(",")]
        # create item for unique locations
        buff = 0.01
        bbox = [lon-buff, lat-buff,lon+buff,lat+buff]
        item = Item(
            id = city,
            bbox=bbox,
            properties={},
            geometry = create_geojson_point(lon, lat),
            datetime = None,
            start_datetime = min_date,
            end_datetime = max_date
        )
        link = collection.add_item(item)
        # bubble up information we want to the link
        link.extra_fields["id"] = key 
        link.extra_fields["latlng"] = latlon
        link.extra_fields["country"] = country
        link.extra_fields["city"] = city
        
    add_collection_information(config, collection, data)

    collection.update_extent_from_items()    
    collection.summaries = Summaries({
        "cities": cities,
        "countries": countries,
    })
    return collection


def handle_VEDA_endpoint(config, endpoint, data, catalog):
    process_STACAPI_Endpoint(
        config=config,
        endpoint=endpoint,
        data=data,
        catalog=catalog,
    )

def add_visualization_info(stac_object, data, endpoint, file_url=None, time=None, styles=None):
    # add extension reference
    if endpoint["Name"] == "Sentinel Hub":
        instanceId = os.getenv("SH_INSTANCE_ID")
        if "InstanceId" in endpoint:
            instanceId = endpoint["InstanceId"]
        stac_object.add_link(
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
    elif endpoint["Name"] == "WMS":
        if endpoint["Type"] == "Time" or endpoint["Type"] == "OverwriteTimes":
            extra_fields={
                "wms:layers": [endpoint["LayerId"]]
            }
            if time != None:
                extra_fields["wms:dimensions"] = {
                    "TIME": time,
                }
            if styles != None:
                extra_fields["wms:styles"] = styles
            stac_object.add_link(
            Link(
                rel="wms",
                target=endpoint["EndPoint"],
                media_type="text/xml",
                title=data["Name"],
                extra_fields=extra_fields,
            )
        )
    elif endpoint["Name"] == "VEDA":
        if endpoint["Type"] == "cog":
            
            bidx = ""
            if "Bidx" in endpoint:
               bidx = "&bidx=%s"%(endpoint["Bidx"])
            
            colormap = ""
            if "Colormap" in endpoint:
               colormap = "&colormap=%s"%(urllib.parse.quote(str(endpoint["Colormap"])))

            colormap_name = ""
            if "ColormapName" in endpoint:
               colormap_name = "&colormap_name=%s"%(endpoint["ColormapName"])

            rescale = ""
            if "Rescale" in endpoint:
               rescale = "&rescale=%s,%s"%(endpoint["Rescale"][0], endpoint["Rescale"][1])
            
            if file_url:
                file_url = "url=%s&"%(file_url)
            else:
                file_url = ""

            target_url = "https://staging-raster.delta-backend.com/cog/tiles/WebMercatorQuad/{z}/{x}/{y}?%sresampling_method=nearest%s%s%s%s"%(
                file_url,
                bidx,
                colormap,
                colormap_name,
                rescale,
            )

            stac_object.add_link(
            Link(
                rel="xyz",
                target=target_url,
                media_type="image/png",
                title=data["Name"],
            )
        )
        pass
    else:
        print("Visualization endpoint not supported")

def process_STACAPI_Endpoint(config, endpoint, data, catalog, headers={}, bbox=None, root_collection=None):
    collection = create_collection(endpoint["CollectionId"], data)
    add_visualization_info(collection, data, endpoint)

    api = Client.open(endpoint["EndPoint"], headers=headers)
    if bbox == None:
        bbox = "-180,-90,180,90"
    results = api.search(
        collections=[endpoint["CollectionId"]],
        bbox=bbox,
        datetime=['1970-01-01T00:00:00Z', '3000-01-01T00:00:00Z'],
    )
    for item in results.items():
        link = collection.add_item(item)
        # Check if we can create visualization link
        if "cog_default" in item.assets:
            add_visualization_info(item, data, endpoint, item.assets["cog_default"].href)
            link.extra_fields["cog_href"] = item.assets["cog_default"].href
        # If a root collection exists we point back to it from the item
        if root_collection != None:
            item.set_collection(root_collection)

        # bubble up information we want to the link
        item_datetime = item.get_datetime()
        # it is possible for datetime to be null, if it is start and end datetime have to exist
        if item_datetime:
            link.extra_fields["datetime"] = item_datetime.isoformat()[:-6] + 'Z'
        else:
            link.extra_fields["start_datetime"] = item.properties["start_datetime"]
            link.extra_fields["end_datetime"] = item.properties["end_datetime"]
        
    collection.update_extent_from_items()
    
    # replace SH identifier with catalog identifier
    collection.id = data["Name"]
    add_collection_information(config, collection, data)

    # validate collection after creation
    '''
    try:
        print(collection.validate())
    except Exception as e:
        print("Issue validation collection: %s"%e)
    '''
    return collection

def add_collection_information(config, collection, data):
    # Add metadata information
    # collection.license = data["License"]
    # TODO: need to review check against SPDX License identifier

    if "Story" in data:
        collection.add_asset(
            "story",
            Asset(
                href="%s/%s"%(config["assets_endpoint"], data["Story"]),
                media_type="text/markdown",
                roles=["metadata"],
            ),
        )
    if "Image" in data:
        collection.add_asset(
            "thumbnail",
            Asset(
                href="%s/%s"%(config["assets_endpoint"], data["Image"]),
                media_type="image/png",
                roles=["thumbnail"],
            ),
        )
def process_catalogs(folder_path):
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            process_catalog_file(file_path)


folder_path = "../catalogs/"
process_catalogs(folder_path)
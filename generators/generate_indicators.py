#!/usr/bin/python
"""
Indicator generator to harvest information from endpoints and generate catalog

"""
import time
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
from pystac.validation import validate_all
import spdx_lookup as lookup
import argparse

argparser = argparse.ArgumentParser(
    prog='STAC generator and harvester',
    description='''
        This library goes over configured endpoints extracting as much information
        as possible and generating a STAC catalog with the information''',
)

argparser.add_argument("-vd", action="store_true", help="validation flag, if set, validation will be run on generated catalogs")
argparser.add_argument("-ni", action="store_true", help="no items flag, if set, items will not be saved")

def recursive_save(stac_object, no_items=False):
    stac_object.save_object()
    for child in stac_object.get_children():
        recursive_save(child, no_items)
    if not no_items:
        # try to save items if available
        for item in stac_object.get_items():
            item.save_object()

def process_catalog_file(file_path, options):
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
        catalog.normalize_hrefs("../build/%s"%config["id"], strategy=strategy)
        
        print("Started creation of collection files")
        start = time.time()
        if options.ni:
            recursive_save(catalog, options.ni)
        else:
            # For full catalog save with items this still seems to be faster
            catalog.save(dest_href="../build/%s"%config["id"])
        end = time.time()
        print("Time consumed in saving: ", end - start)

        if options.vd:
            # try to validate catalog if flag was set
            print("Running validation of catalog %s"%file_path)
            try:
                validate_all(catalog.to_dict(), href=config["endpoint"])
            except Exception as e:
                print("Issue validation collection: %s"%e)

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
                elif resource["Name"] == "GeoDB Vector Tiles":
                    handle_GeoDB_Tiles_endpoint(config, resource, data, catalog)
                else:
                    raise ValueError("Type of Resource is not supported")

def handle_GeoDB_Tiles_endpoint(config, endpoint, data, catalog):
    collection = create_collection(endpoint["CollectionId"], data, config)
    if "Dates" in data:
        pass
    else:
        select = "?select=%s"%endpoint["TimeKey"]
        url = endpoint["DBEndpoint"] + endpoint["Database"] + "_%s"%endpoint["Source"] + select
        response = json.loads(requests.get(url).text)
        times = set([entry[endpoint["TimeKey"]] for entry in response])
        if len(times) > 0:
            # Create an item per time to allow visualization in stac clients
            styles = None
            if hasattr(endpoint, "Styles"):
                styles = endpoint["Styles"]
            collection = create_collection(data["Name"], data, config)
            # TODO: For now we create global extent, we should be able to
            # fetch the extent of the layer
            for t in times:
                item = Item(
                    id = t,
                    bbox=[-180.0, -90.0, 180.0, 90.0],
                    properties={},
                    geometry = None,
                    datetime = parser.isoparse(t),
                )
                # add_visualization_info(item, data, endpoint, time=t, styles=styles)
                link = collection.add_item(item)
                link.extra_fields["datetime"] = t
            collection.update_extent_from_items()
            add_visualization_info(collection, data, endpoint, styles=styles)
            add_collection_information(config, collection, data)
            add_to_catalog(collection, catalog, endpoint, data)

    
def handle_WMS_endpoint(config, endpoint, data, catalog):
    if endpoint["Type"] == "Time" or endpoint["Type"] == "OverwriteTimes":

        times = []
        extent = retrieveExtentFromWMS(endpoint["EndPoint"], endpoint["LayerId"])
        if endpoint["Type"] == "OverwriteTimes":
            times = endpoint["Times"]
        else:
            times = extent["temporal"]
        if "OverwriteBBox" in endpoint:
            extent["spatial"] = endpoint["OverwriteBBox"]
        
        if len(times) > 0:
            # Create an item per time to allow visualization in stac clients
            styles = None
            if hasattr(endpoint, "Styles"):
                styles = endpoint["Styles"]
            collection = create_collection(data["Name"], data, config)
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
            add_collection_information(config, collection, data)
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
        root_collection = create_collection(data["Name"], data, config)
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

    add_example_info(root_collection, data, endpoint, config)
    add_to_catalog(root_collection, catalog, endpoint, data)

def create_collection(collection_id, data, config):
    spatial_extent = SpatialExtent([
        [-180.0, -90.0, 180.0, 90.0],
    ])
    temporal_extent = TemporalExtent([[datetime.now()]])
    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)

    # Check if description is link to markdown file
    if "Description" in data:
        description = data["Description"]
        if description.endswith((".md", ".MD")):
            if description.startswith(("http")):
                # if full absolut path is defined
                response = requests.get(description)
                if response.status_code == 200:
                    description = response.text
                elif "Subtitle" in data:
                    print("Warning: Markdown file could not be fetched")
                    description = data["Subtitle"]
            else:
                # relative path to assets was given
                response = requests.get(
                    "%s/%s"%(config["assets_endpoint"], description)
                )
                if response.status_code == 200:
                    description = response.text
                elif "Subtitle" in data:
                    print("Warning: Markdown file could not be fetched")
                    description = data["Subtitle"]
    elif "Subtitle" in data:
        # Try to use at least subtitle to fill some information
        description = data["Subtitle"]


    collection = Collection(
        id=collection_id,
        title=data["Title"],
        description=description,
        stac_extensions=[
            "https://stac-extensions.github.io/web-map-links/v1.1.0/schema.json",
            "https://stac-extensions.github.io/example-links/v0.0.1/schema.json"
        ],
        extent=extent
    )
    return collection

def add_to_catalog(collection, catalog, endpoint, data):
    link = catalog.add_child(collection)
    # bubble fields we want to have up to collection link
    link.extra_fields["endpointtype"] = endpoint["Name"]
    # Disabling bubbling up of description as now it is considered to be
    # used as markdown loading would increase the catalog size unnecessarily
    # link.extra_fields["description"] = collection.description
    link.extra_fields["subtitle"] = data["Subtitle"]
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
    collection = create_collection(endpoint["CollectionId"], data, config)
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
    collection = process_STACAPI_Endpoint(
        config=config,
        endpoint=endpoint,
        data=data,
        catalog=catalog,
    )
    add_example_info(collection, data, endpoint, config)
    add_to_catalog(collection, catalog, endpoint, data)

def add_example_info(stac_object, data, endpoint, config):
    if "Services" in data:
        for service in data["Services"]:
            if service["Name"] == "Statistical API":
                stac_object.add_link(
                    Link(
                        rel="example",
                        target="%s/%s"%(config["assets_endpoint"], service["Script"]),
                        title="evalscript",
                        media_type="application/javascript",
                        extra_fields={
                            "example:language": "JavaScript",
                            "dataId": "%s-%s"%(service["Type"], service["CollectionId"]),
                        },
                    )
                )
            if service["Name"] == "VEDA Statistics":
                stac_object.add_link(
                    Link(
                        rel="example",
                        target=service["Endpoint"],
                        title=service["Name"],
                        media_type="application/json",
                        extra_fields={
                            "example:language": "JSON",
                        },
                    )
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
    elif endpoint["Name"] == "GeoDB Vector Tiles":
        #`${geoserverUrl}${config.layerName}@EPSG%3A${projString}@pbf/{z}/{x}/{-y}.pbf`,
        # 'geodb_debd884d-92f9-4979-87b6-eadef1139394:GTIF_AT_Gemeinden_3857'
        target_url = "%s%s:%s_%s@EPSG:3857@pbf/{z}/{x}/{-y}.pbf"%(
            endpoint["EndPoint"],
            endpoint["Instance"],
            endpoint["Database"],
            endpoint["CollectionId"],
        )
        stac_object.add_link(
            Link(
                rel="xyz",
                target=target_url,
                media_type="application/pbf",
                title=data["Name"],
                extra_fields={
                    "description": data["Title"],
                    "parameters": endpoint["Parameters"],
                    "matchKey": endpoint["MatchKey"],
                    "timeKey": endpoint["TimeKey"],
                    "source" : endpoint["Source"],
                }
            )
        )
    else:
        print("Visualization endpoint not supported")

def process_STACAPI_Endpoint(config, endpoint, data, catalog, headers={}, bbox=None, root_collection=None):
    collection = create_collection(endpoint["CollectionId"], data, config)
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

    return collection

def add_collection_information(config, collection, data):
    # Add metadata information
    # Check license identifier
    if "License" in data:
        license = lookup.by_id(data["License"])
        if license is not None:
            collection.license = license.id
            if license.sources:
                # add links to licenses
                for source in license.sources:
                    collection.links.append(Link(
                        rel="license",
                        target=source,
                        media_type="text/html",
                    ))
        else:
            # fallback to proprietary
            print("Warning: License could not be parsed, falling back to proprietary")
            collection.license = "proprietary"
    else:
        print("Warning: No license was provided, falling back to proprietary")

    if "Subtitle" in data:
        collection.extra_fields["subtitle"] = data["Subtitle"]
    if "Legend" in data:
        collection.add_asset(
            "legend",
            Asset(
                href="%s/%s"%(config["assets_endpoint"], data["Legend"]),
                media_type="image/png",
                roles=["thumbnail"],
            ),
        )
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
    
    # Add extra fields to collection if available
    if "Themes" in data:
        collection.extra_fields["themes"] = ",".join(data["Themes"])
    if "Tags" in data:
        collection.extra_fields["tags"] = ",".join(data["Tags"])
    if "Satellite" in data:
        collection.extra_fields["satellite"] = ",".join(data["Satellite"])
    if "Sensor" in data:
        collection.extra_fields["sensor"] = ",".join(data["Sensor"])
    if "Agency" in data:
        collection.extra_fields["agency"] = ",".join(data["Agency"])


def process_catalogs(folder_path, options):
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            process_catalog_file(file_path, options)

options = argparser.parse_args()
folder_path = "../catalogs/"
process_catalogs(folder_path, options)

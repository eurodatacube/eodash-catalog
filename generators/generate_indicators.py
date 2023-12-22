#!/usr/bin/python
"""
Indicator generator to harvest information from endpoints and generate catalog

"""
import time
import requests
import json
from pystac_client import Client
import os
import re
from pathlib import Path
from datetime import datetime
import yaml
from yaml.loader import SafeLoader
from itertools import groupby
from operator import itemgetter
from dateutil import parser
from sh_endpoint import get_SH_token
from utils import (
    create_geojson_point,
    retrieveExtentFromWMS,
    generateDateIsostringsFromInterval,
    RaisingThread,
)
from pystac import (
    Item,
    Asset,
    Catalog,
    Link,
    CatalogType,
    Collection,
    Extent,
    SpatialExtent,
    TemporalExtent,
    Summaries,
    Provider
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
argparser.add_argument("-tn", action="store_true", help="generate additionally thumbnail image for supported collections")

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
        if "Resources" in data:
            for resource in data["Resources"]:
                if "EndPoint" in resource:
                    if resource["Name"] == "Sentinel Hub":
                        handle_SH_endpoint(config, resource, data, catalog)
                    elif resource["Name"] == "GeoDB":
                        collection = handle_GeoDB_endpoint(config, resource, data, catalog)
                        add_to_catalog(collection, catalog, resource, data)
                    elif resource["Name"] == "VEDA":
                        handle_VEDA_endpoint(config, resource, data, catalog)
                    elif resource["Name"] == "xcube":
                        handle_xcube_endpoint(config, resource, data, catalog)
                    elif resource["Name"] == "WMS":
                        handle_WMS_endpoint(config, resource, data, catalog)
                    elif resource["Name"] == "GeoDB Vector Tiles":
                        handle_GeoDB_Tiles_endpoint(config, resource, data, catalog)
                    elif resource["Name"] == "Collection-only":
                        handle_collection_only(config, resource, data, catalog)
                    else:
                        raise ValueError("Type of Resource is not supported")
        elif "Subcollections" in data:
            # TODO:
            #  - implement bbox filtering for sub collections
            #  - implement summary of locations on parent collection level
            #  - ...
            # if no endpoint is specified we check for combination of collections
            for sub_collection in data["Subcollections"]:
                parent_collection = get_or_create_collection(catalog, data["Name"], data, config)
                process_collection_file(config, "../collections/%s.yaml"%(sub_collection["Identifier"]), parent_collection)
                add_collection_information(config, parent_collection, data)
                parent_collection.update_extent_from_items()
                # find link in parent collection to update metadata
                for link in parent_collection.links:
                    if link.rel == "child" and link.extra_fields["code"] == sub_collection["Identifier"]:
                        latlng = "%s,%s"%(sub_collection["Point"][1], sub_collection["Point"][0])
                        link.extra_fields["id"] = sub_collection["Identifier"]
                        link.extra_fields["latlng"] = latlng
                        link.extra_fields["name"] = sub_collection["Name"]
                add_to_catalog(parent_collection, catalog, None, data)


def handle_collection_only(config, endpoint, data, catalog):
    times = []
    collection = get_or_create_collection(catalog, data["Name"], data, config, endpoint)
    if endpoint.get("Type") == "OverwriteTimes" and endpoint.get("Times"):
        times = endpoint["Times"]
    elif endpoint.get("Type") == "OverwriteTimes" and endpoint.get("DateTimeInterval"):
        start = endpoint["DateTimeInterval"].get("Start", "2020-09-01T00:00:00")
        end = endpoint["DateTimeInterval"].get("End", "2020-10-01T00:00:00")
        timedelta_config = endpoint["DateTimeInterval"].get("Timedelta", {'days': 1})
        times = generateDateIsostringsFromInterval(start, end, timedelta_config)
    for t in times:
        item = Item(
            id = t,
            bbox=endpoint.get("OverwriteBBox"),
            properties={},
            geometry = None,
            datetime = parser.isoparse(t),
        )
        link = collection.add_item(item)
        link.extra_fields["datetime"] = t
    if len(times) > 0:
        collection.update_extent_from_items()
    add_collection_information(config, collection, data)
    add_to_catalog(collection, catalog, None, data)


def handle_GeoDB_Tiles_endpoint(config, endpoint, data, catalog):
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
            collection = get_or_create_collection(catalog, data["Name"], data, config, endpoint)
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
    times = []
    extent = retrieveExtentFromWMS(endpoint["EndPoint"], endpoint["LayerId"])
    if endpoint.get("Type") == "OverwriteTimes" and endpoint.get("Times"):
        times = endpoint["Times"]
    elif endpoint.get("Type") == "OverwriteTimes" and endpoint.get("DateTimeInterval"):
        start = endpoint["DateTimeInterval"].get("Start", "2020-09-01T00:00:00")
        end = endpoint["DateTimeInterval"].get("End", "2020-10-01T00:00:00")
        timedelta_config = endpoint["DateTimeInterval"].get("Timedelta", {'days': 1})
        times = generateDateIsostringsFromInterval(start, end, timedelta_config)
    else:
        times = extent["temporal"]
    if "OverwriteBBox" in endpoint:
        extent["spatial"] = endpoint["OverwriteBBox"]
    
    collection = get_or_create_collection(catalog, data["Name"], data, config, endpoint)
    # Create an item per time to allow visualization in stac clients
    styles = None
    if hasattr(endpoint, "Styles"):
        styles = endpoint["Styles"]

    if len(times) > 0:
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


def handle_SH_endpoint(config, endpoint, data, catalog):
    token = get_SH_token()
    headers = {"Authorization": "Bearer %s"%token}
    endpoint["EndPoint"] = "https://services.sentinel-hub.com/api/v1/catalog/1.0.0/"
    endpoint["CollectionId"] = endpoint["Type"] + "-" + endpoint["CollectionId"]
    handle_STAC_based_endpoint(config, endpoint, data, catalog, headers)

def handle_VEDA_endpoint(config, endpoint, data, catalog):
    handle_STAC_based_endpoint(config, endpoint, data, catalog)

def handle_xcube_endpoint(config, endpoint, data, catalog):
    root_collection = process_STAC_Datacube_Endpoint(
        config=config,
        endpoint=endpoint,
        data=data,
        catalog=catalog,
    )

    add_example_info(root_collection, data, endpoint, config)
    add_to_catalog(root_collection, catalog, endpoint, data)


def get_or_create_collection(catalog, collection_id, data, config, endpoint=None):
    # Check if collection already in catalog
    for collection in catalog.get_collections():
        if collection.id == collection_id:
            return collection
    # If none found create a new one
    spatial_extent = [-180.0, -90.0, 180.0, 90.0]
    if endpoint and endpoint.get("OverwriteBBox"):
        spatial_extent = endpoint.get("OverwriteBBox")
    spatial_extent = SpatialExtent([
        spatial_extent,
    ])
    temporal_extent = TemporalExtent([[datetime.now(), None]])
    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)

    # Check if description is link to markdown file
    if "Description" in data:
        description = data["Description"]
        if description.endswith((".md", ".MD")):
            if description.startswith(("http")):
                # if full absolute path is defined
                response = requests.get(description)
                if response.status_code == 200:
                    description = response.text
                elif "Subtitle" in data:
                    print("WARNING: Markdown file could not be fetched")
                    description = data["Subtitle"]
            else:
                # relative path to assets was given
                response = requests.get(
                    "%s/%s"%(config["assets_endpoint"], description)
                )
                if response.status_code == 200:
                    description = response.text
                elif "Subtitle" in data:
                    print("WARNING: Markdown file could not be fetched")
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
            "https://stac-extensions.github.io/example-links/v0.0.1/schema.json",
            "https://stac-extensions.github.io/scientific/v1.0.0/schema.json"
        ],
        extent=extent
    )
    return collection

def add_to_catalog(collection, catalog, endpoint, data):
    # check if already in catalog, if it is do not re-add it
    # TODO: probably we should add to the catalog only when creating
    for cat_coll in catalog.get_collections():
        if cat_coll.id == collection.id:
            return

    link = catalog.add_child(collection)
    # bubble fields we want to have up to collection link
    if endpoint:
        link.extra_fields["endpointtype"] = endpoint["Name"]
    # Disabling bubbling up of description as now it is considered to be
    # used as markdown loading would increase the catalog size unnecessarily
    # link.extra_fields["description"] = collection.description
    if "Subtitle" in data:
        link.extra_fields["subtitle"] = data["Subtitle"]
    link.extra_fields["title"] = collection.title
    link.extra_fields["code"] = data["EodashIdentifier"]
    link.extra_fields["themes"] = data["Themes"]
    # Check for summaries and bubble up info
    if collection.summaries.lists:
        for sum in collection.summaries.lists:
            link.extra_fields[sum] = collection.summaries.lists[sum]
    if "Locations" in data or "Subcollections" in data:
        link.extra_fields["locations"] = True
    if "Tags" in data:
        link.extra_fields["tags"] = data["Tags"]
    if "Satellite" in data:
        link.extra_fields["satellite"] = data["Satellite"]
    if "Sensor" in data:
        link.extra_fields["sensor"] = data["Sensor"]
    if "Agency" in data:
        link.extra_fields["agency"] = data["Agency"]
    if "yAxis" in data:
        link.extra_fields["yAxis"] = data["yAxis"]
    return link


def handle_GeoDB_endpoint(config, endpoint, data, catalog):
    collection = get_or_create_collection(catalog, endpoint["CollectionId"], data, config, endpoint)
    select = "?select=aoi,aoi_id,country,city,time"
    where_parameter = endpoint.get("WhereParameter")
    url = endpoint["EndPoint"] + endpoint["Database"] + "_%s"%endpoint["CollectionId"] + select
    if where_parameter:
        url += f"&{where_parameter}"
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
        # sanitize city identifier to be sure it is filename save
        if city is not None:
            city = "".join([c for c in city if c.isalpha() or c.isdigit() or c==' ']).rstrip()
        # Additional check to see if city name is empty afterwards
        if city == "" or city is None:
            # use aoi_id as a fallback unique id instead of city
            city = key
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

    if "yAxis" not in data:
        # fetch yAxis and store it to data, preventing need to save it per dataset in yml
        select = "?select=y_axis&limit=1"
        url = endpoint["EndPoint"] + endpoint["Database"] + "_%s"%endpoint["CollectionId"] + select
        response = json.loads(requests.get(url).text)
        yAxis = response[0]['y_axis']
        data['yAxis'] = yAxis
    add_collection_information(config, collection, data)

    collection.update_extent_from_items()    
    collection.summaries = Summaries({
        "cities": cities,
        "countries": countries,
    })
    return collection


def handle_STAC_based_endpoint(config, endpoint, data, catalog, headers=None):
    if "Locations" in data:
        root_collection = get_or_create_collection(catalog, data["Name"], data, config, endpoint)
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
            else:
                collection.description = location["Name"]
            # TODO: should we remove all assets from sub collections?
            link = root_collection.add_child(collection)
            latlng = "%s,%s"%(location["Point"][1], location["Point"][0])
            # Add extra properties we need
            link.extra_fields["id"] = location["Identifier"]
            link.extra_fields["latlng"] = latlng
            link.extra_fields["name"] = location["Name"]
            add_example_info(collection, data, endpoint, config)
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
    elif "Resources" in data:
        for service in data["Resources"]:
            if service.get("Name") == "xcube":
                target_url = "%s/timeseries/%s/%s?aggMethods=median"%(
                    endpoint["EndPoint"],
                    endpoint["DatacubeId"],
                    endpoint["Variable"],
                )
                stac_object.add_link(
                    Link(
                        rel="example",
                        target=target_url,
                        title=service["Name"] + " analytics",
                        media_type="application/json",
                        extra_fields={
                            "example:language": "JSON",
                            "example:method": "POST"
                        },
                    )
                )
def generate_veda_link(endpoint, file_url):
    bidx = ""
    if "Bidx" in endpoint:
        # Check if an array was provided
        if hasattr(endpoint["Bidx"], "__len__"):
            for band in endpoint["Bidx"]:
                bidx = bidx + "&bidx=%s"%(band)
        else:
            bidx = "&bidx=%s"%(endpoint["Bidx"])
    
    colormap = ""
    if "Colormap" in endpoint:
        colormap = "&colormap=%s"%(endpoint["Colormap"])
        # TODO: For now we assume a already urlparsed colormap definition
        # it could be nice to allow a json and better convert it on the fly
        # colormap = "&colormap=%s"%(urllib.parse.quote(str(endpoint["Colormap"])))

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
    return target_url

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
    elif endpoint["Name"] == "xcube":
        if endpoint["Type"] == "zarr":
            # either preset ColormapName of left as a template
            cbar = endpoint.get("ColormapName", "{cbar}")
            # either preset Rescale of left as a template
            vmin = "{vmin}"
            vmax = "{vmax}"
            if "Rescale" in endpoint:
               vmin = endpoint["Rescale"][0]
               vmax = endpoint["Rescale"][1]
            crs = endpoint.get("Crs", "EPSG:3857")
            target_url = "%s/tiles/%s/%s/{z}/{y}/{x}?crs=%s&time={time}&vmin=%s&vmax=%s&cbar=%s"%(
                endpoint["EndPoint"],
                endpoint["DatacubeId"],
                endpoint["Variable"],
                crs,
                vmin,
                vmax,
                cbar,
            )
            stac_object.add_link(
            Link(
                rel="xyz",
                target=target_url,
                media_type="image/png",
                title="xcube tiles",
            )
        )
        pass
    elif endpoint["Name"] == "VEDA":
        if endpoint["Type"] == "cog":    
            target_url = generate_veda_link(endpoint, file_url)
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
    collection = get_or_create_collection(catalog, endpoint["CollectionId"], data, config, endpoint)
    add_visualization_info(collection, data, endpoint)

    api = Client.open(endpoint["EndPoint"], headers=headers)
    if bbox == None:
        bbox = "-180,-90,180,90"
    results = api.search(
        collections=[endpoint["CollectionId"]],
        bbox=bbox,
        datetime=['1900-01-01T00:00:00Z', '3000-01-01T00:00:00Z'],
    )
    for item in results.items():
        link = collection.add_item(item)
        if(options.tn):
            if "cog_default" in item.assets:
                generate_thumbnail(item, data, endpoint, item.assets["cog_default"].href)
            else:
                generate_thumbnail(item, data, endpoint)
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

def fetch_and_save_thumbnail(data, url):
    collection_path = "../thumbnails/%s_%s/"%(data["EodashIdentifier"], data["Name"])
    Path(collection_path).mkdir(parents=True, exist_ok=True)
    image_path = '%s/thumbnail.png'%(collection_path)
    if not os.path.exists(image_path):
        data = requests.get(url).content 
        f = open(image_path,'wb') 
        f.write(data) 
        f.close()

def generate_thumbnail(stac_object, data, endpoint, file_url=None, time=None, styles=None):
    if endpoint["Name"] == "Sentinel Hub" or endpoint["Name"] == "WMS":
        instanceId = os.getenv("SH_INSTANCE_ID")
        if "InstanceId" in endpoint:
            instanceId = endpoint["InstanceId"]
        # Build example url
        wms_config = "REQUEST=GetMap&SERVICE=WMS&VERSION=1.3.0&FORMAT=image/png&STYLES=&TRANSPARENT=true"
        bbox = "%s,%s,%s,%s"%(
            stac_object.bbox[1],
            stac_object.bbox[0],
            stac_object.bbox[3],
            stac_object.bbox[2],
        )
        output_format = "format=image/png&WIDTH=256&HEIGHT=128&CRS=EPSG:4326&BBOX=%s"%(bbox)
        item_datetime = stac_object.get_datetime()
        # it is possible for datetime to be null, if it is start and end datetime have to exist
        if item_datetime:
            time = item_datetime.isoformat()[:-6] + 'Z'
        url = "https://services.sentinel-hub.com/ogc/wms/%s?%s&layers=%s&time=%s&%s"%(
            instanceId,
            wms_config,
            endpoint["LayerId"],
            time,
            output_format,
        )
        fetch_and_save_thumbnail(data, url)
    elif endpoint["Name"] == "VEDA":
        target_url = generate_veda_link(endpoint, file_url)
        # set to get 0/0/0 tile
        url = re.sub(r"\{.\}", "0", target_url)
        fetch_and_save_thumbnail(data, url)
    

def process_STAC_Datacube_Endpoint(config, endpoint, data, catalog):
    collection = get_or_create_collection(catalog, data["Name"], data, config, endpoint)
    add_visualization_info(collection, data, endpoint)

    stac_endpoint_url = endpoint["EndPoint"]
    if endpoint.get('Name') == 'xcube':
        stac_endpoint_url = stac_endpoint_url + endpoint.get('StacEndpoint','')
    # assuming /search not implemented
    api = Client.open(stac_endpoint_url)
    coll = api.get_collection(endpoint.get('CollectionId', 'datacubes'))
    item = coll.get_item(endpoint.get('DatacubeId'))
    # slice a datacube along temporal axis to individual items, selectively adding properties
    dimensions = item.properties.get('cube:dimensions', {})
    variables = item.properties.get('cube:variables')
    if not endpoint.get("Variable") in variables.keys():
        raise Exception(f'Variable {endpoint.get("Variable")} not found in datacube {variables}')
    time_dimension = 'time'
    for k, v in dimensions.items():
        if v.get('type') == 'temporal':
            time_dimension = k
            break
    time_entries = dimensions.get(time_dimension).get('values')
    for t in time_entries:
        item = Item(
            id = t,
            bbox=item.bbox,
            properties={},
            geometry = item.geometry,
            datetime = parser.isoparse(t),
        )
        link = collection.add_item(item)
        link.extra_fields["datetime"] = t
        # bubble up information we want to the link
        item_datetime = item.get_datetime()
        # it is possible for datetime to be null, if it is start and end datetime have to exist
        if item_datetime:
            link.extra_fields["datetime"] = item_datetime.isoformat()[:-6] + 'Z'
        else:
            link.extra_fields["start_datetime"] = item.properties["start_datetime"]
            link.extra_fields["end_datetime"] = item.properties["end_datetime"]
    unit = variables.get(endpoint.get("Variable")).get('unit')
    if unit and "yAxis" not in data:
        data["yAxis"] = unit
    collection.update_extent_from_items()

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
            print("WARNING: License could not be parsed, falling back to proprietary")
            collection.license = "proprietary"
    else:
        print("WARNING: No license was provided, falling back to proprietary")

    if "Provider" in data:
        try:
            collection.providers = [
                Provider(
                    # convert information to lower case
                    **dict((k.lower(), v) for k,v in provider.items())
                ) for provider in data["Provider"]
            ]
        except:
            print("WARNING: Issue creating provider information for collection: %s"%collection.id)

    if "Citation" in data:
        if "DOI" in data["Citation"]:
            collection.extra_fields["sci:doi"] = data["Citation"]["DOI"]
        if "Citation" in data["Citation"]:
            collection.extra_fields["sci:citation"] = data["Citation"]["Citation"]
        if "Publication" in data["Citation"]:
            collection.extra_fields["sci:publications"] = [
                # convert keys to lower case
                dict((k.lower(), v) for k,v in publication.items())
                for publication in data["Citation"]["Publication"]
            ]


    if "Subtitle" in data:
        collection.extra_fields["subtitle"] = data["Subtitle"]
    if "Legend" in data:
        collection.add_asset(
            "legend",
            Asset(
                href="%s/%s"%(config["assets_endpoint"], data["Legend"]),
                media_type="image/png",
                roles=["metadata"],
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
    if "yAxis" in data:
        collection.extra_fields["yAxis"] = data["yAxis"]
    if "Themes" in data:
        collection.extra_fields["themes"] = data["Themes"]
    if "Tags" in data:
        collection.extra_fields["keywords"] = data["Tags"]
    if "Satellite" in data:
        collection.extra_fields["satellite"] = data["Satellite"]
    if "Sensor" in data:
        collection.extra_fields["sensor"] = data["Sensor"]
    if "Agency" in data:
        collection.extra_fields["agency"] = data["Agency"]


def process_catalogs(folder_path, options):
    tasks = []
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            tasks.append(RaisingThread(target=process_catalog_file, args=(file_path, options)))
            tasks[-1].start()
    for task in tasks:
        task.join()

options = argparser.parse_args()
folder_path = "../catalogs/"
process_catalogs(folder_path, options)

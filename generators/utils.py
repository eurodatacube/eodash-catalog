
import json

def create_geojson_point(lon, lat):
    point = {
        "type": "Point",
        "coordinates": [lon, lat]
    }

    feature = {
        "type": "Feature",
        "geometry": point,
        "properties": {}
    }

    feature_collection = {
        "type": "FeatureCollection",
        "features": [feature]
    }

    return feature_collection
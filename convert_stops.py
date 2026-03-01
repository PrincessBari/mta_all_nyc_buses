import csv
import json
import os
import re

GTFS_FOLDERS = [
    'bus_routes/gtfs_bronx',
    'bus_routes/gtfs_brooklyn',
    'bus_routes/gtfs_manhattan',
    'bus_routes/gtfs_queens',
    'bus_routes/gtfs_staten_island',
    'bus_routes/gtfs_express'
]

def read_csv(filepath):
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))

def convert():
    # route_id -> set of (stop_id, stop_name, lat, lng)
    route_stops = {}
    stop_info = {}

    for folder in GTFS_FOLDERS:
        if not os.path.exists(folder):
            print(f"Warning: folder '{folder}' not found, skipping")
            continue

        trips_path    = os.path.join(folder, 'trips.txt')
        stop_times_path = os.path.join(folder, 'stop_times.txt')
        stops_path    = os.path.join(folder, 'stops.txt')

        if not all(os.path.exists(p) for p in [trips_path, stop_times_path, stops_path]):
            print(f"Warning: missing required files in '{folder}', skipping")
            continue

        print(f"Processing {folder}...")

        # Build trip_id -> route_id mapping
        trips = read_csv(trips_path)
        trip_to_route = {}
        for trip in trips:
            trip_id  = trip.get('trip_id', '').strip()
            route_id = trip.get('route_id', '').strip()
            if not trip_id or not route_id:
                continue
            # Same normalization as routes script
            route_id = route_id.split('_')[-1] if '_' in route_id else route_id
            if route_id == 'route_id':
                continue
            if route_id.endswith('+'):
                route_id = route_id[:-1] + '-SBS'
            if route_id.startswith('BX'):
                route_id = 'Bx' + route_id[2:]
            route_id = re.sub(r'^([A-Za-z]+)0+(\d)', r'\1\2', route_id)
            trip_to_route[trip_id] = route_id

        # Build stop_id -> (stop_name, lat, lng) mapping
        stops = read_csv(stops_path)
        
        for row in stops:
            stop_id = row.get('stop_id', '').strip()
            try:
                lat = float(row['stop_lat'])
                lng = float(row['stop_lon'])
            except (ValueError, KeyError):
                continue
            stop_name = row.get('stop_name', '').strip()
            stop_info[stop_id] = (stop_name, lat, lng)

        # Link stops to routes via stop_times.txt (streaming to handle large files)
        with open(stop_times_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                trip_id = row.get('trip_id', '').strip()
                stop_id = row.get('stop_id', '').strip()
                route_id = trip_to_route.get(trip_id)
                if not route_id or stop_id not in stop_info:
                    continue
                if route_id not in route_stops:
                    route_stops[route_id] = set()
                route_stops[route_id].add(stop_id)

    # DEBUG
    m50_stops = route_stops.get('M50', set())
    print(f"M50 stops found: {len(m50_stops)}")
    if m50_stops:
        print("Sample stop IDs:", list(m50_stops)[:5]) 

    # Build GeoJSON
    features = []
    for route_id, stop_ids in route_stops.items():
        for stop_id in stop_ids:
            stop_name, lat, lng = stop_info.get(stop_id, (None, None, None))
            if lat is None:
                continue
            features.append({
                "type": "Feature",
                "properties": {
                    "route_id": route_id,
                    "stop_id":  stop_id,
                    "stop_name": stop_name
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat]
                }
            })

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    output_path = 'stops.geojson'
    with open(output_path, 'w') as f:
        json.dump(geojson, f)

    print(f"\nDone! {len(features)} stop features written to {output_path}")

if __name__ == '__main__':
    convert()
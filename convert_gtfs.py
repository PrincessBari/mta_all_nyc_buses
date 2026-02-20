import csv
import json
import os
import re

# Folder names to process
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
    # route_id -> list of coordinates per shape
    # We'll collect all shapes per route, then pick the longest one
    route_shapes = {}  # route_id -> { shape_id -> [(lng, lat), ...] }

    for folder in GTFS_FOLDERS:
        if not os.path.exists(folder):
            print(f"Warning: folder '{folder}' not found, skipping")
            continue

        trips_path = os.path.join(folder, 'trips.txt')
        shapes_path = os.path.join(folder, 'shapes.txt')

        if not os.path.exists(trips_path) or not os.path.exists(shapes_path):
            print(f"Warning: missing trips.txt or shapes.txt in '{folder}', skipping")
            continue

        print(f"Processing {folder}...")

        # Build shape_id -> route_id mapping from trips.txt
        trips = read_csv(trips_path)
        shape_to_route = {}
        for trip in trips:
            shape_id = trip.get('shape_id', '').strip()
            route_id = trip.get('route_id', '').strip()
            if shape_id and route_id:
                # Clean route_id — MTA sometimes prefixes with agency e.g. "MTA NYCT_M15"
                route_id = route_id.split('_')[-1] if '_' in route_id else route_id
                # Skip header row if it bleeds through
                if route_id == 'route_id':
                    continue
                # Normalize SBS routes: M23+ -> M23-SBS (must happen before Bx fix)
                if route_id.endswith('+'):
                    route_id = route_id[:-1] + '-SBS'
                # Normalize Bx routes: BX26 -> Bx26, BXM1 -> BxM1
                if route_id.startswith('BX'):
                    route_id = 'Bx' + route_id[2:]
                # Normalize leading zeros: Q06 -> Q6
                route_id = re.sub(r'^([A-Za-z]+)0+(\d)', r'\1\2', route_id)
                shape_to_route[shape_id] = route_id

        # Read shapes.txt and group points by shape_id
        shapes = read_csv(shapes_path)
        shape_points = {}  # shape_id -> [(seq, lng, lat)]
        for row in shapes:
            shape_id = row.get('shape_id', '').strip()
            try:
                lat = float(row['shape_pt_lat'])
                lng = float(row['shape_pt_lon'])
                seq = int(row['shape_pt_sequence'])
            except (ValueError, KeyError):
                continue
            if shape_id not in shape_points:
                shape_points[shape_id] = []
            shape_points[shape_id].append((seq, lng, lat))

        # Map shapes to routes
        for shape_id, points in shape_points.items():
            route_id = shape_to_route.get(shape_id)
            if not route_id:
                continue

            # Sort points by sequence
            points.sort(key=lambda x: x[0])
            coords = [(lng, lat) for _, lng, lat in points]

            if route_id not in route_shapes:
                route_shapes[route_id] = {}
            route_shapes[route_id][shape_id] = coords

    # Build GeoJSON — for each route, pick the longest shape
    features = []
    for route_id, shapes in route_shapes.items():
        longest = max(shapes.values(), key=len)
        features.append({
            "type": "Feature",
            "properties": {
                "route_id": route_id
            },
            "geometry": {
                "type": "LineString",
                "coordinates": longest
            }
        })

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    output_path = 'routes.geojson'
    with open(output_path, 'w') as f:
        json.dump(geojson, f)

    print(f"\nDone! {len(features)} routes written to {output_path}")
    print("Route IDs sample:", sorted([f['properties']['route_id'] for f in features])[:20])

if __name__ == '__main__':
    convert()

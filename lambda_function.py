import json
import os
import requests
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Borough color mapping (blue gradient + purple for express)
BOROUGH_COLORS = {
    'Manhattan': '#1e40af',
    'Bronx': '#2563eb',
    'Queens': '#3b82f6',
    'Brooklyn': '#60a5fa',
    'Staten Island': '#93c5fd',
    'Express': '#8b5cf6'
}

def get_borough_from_route(route):
    if not route or len(route) == 0:
        return {'name': 'Unknown', 'color': '#6b7280'}
    
    route_upper = route.upper()
    
    # Express routes (all go to their own category)
    if route_upper.startswith('BXM'):
        return {'name': 'Express', 'color': '#8b5cf6'}
    if route_upper.startswith('QM'):
        return {'name': 'Express', 'color': '#8b5cf6'}
    if route_upper.startswith('BM'):
        return {'name': 'Express', 'color': '#8b5cf6'}
    if route_upper.startswith('SIM'):
        return {'name': 'Express', 'color': '#8b5cf6'}
    if route_upper.startswith('X'):
        return {'name': 'Express', 'color': '#8b5cf6'}
    
    # Check Bx (Bronx local) before single B (Brooklyn)
    if route_upper.startswith('BX'):
        return {'name': 'Bronx', 'color': '#2563eb'}
    
    # Single letter local routes
    prefix = route_upper[0]
    borough_map = {
        'M': {'name': 'Manhattan', 'color': '#1e40af'},
        'B': {'name': 'Brooklyn', 'color': '#60a5fa'},
        'Q': {'name': 'Queens', 'color': '#3b82f6'},
        'S': {'name': 'Staten Island', 'color': '#93c5fd'}
    }
    return borough_map.get(prefix, {'name': 'Unknown', 'color': '#6b7280'})

def lambda_handler(event, context):
    """AWS Lambda handler for all NYC buses"""
    
    api_key = os.environ.get('MTA_API_KEY')
    
    if not api_key:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({'error': 'API key not configured'})
        }
    
    try:
        logger.info("Starting MTA API request for all NYC buses...")
        url = "https://bustime.mta.info/api/siri/vehicle-monitoring.json"
        params = {
            'key': api_key,
            'version': '2',
            'VehicleMonitoringDetailLevel': 'calls'
        }
        
        logger.info(f"Calling {url}")
        response = requests.get(url, params=params, timeout=90)
        logger.info(f"Got response with status {response.status_code}")
        response.raise_for_status()
        data = response.json()
        
        buses = []
        borough_counts = {}
        route_counts = {}
        
        vehicle_activities = data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0].get('VehicleActivity', [])
        
        logger.info(f"Processing {len(vehicle_activities)} vehicles...")
        
        for activity in vehicle_activities:
            journey = activity.get('MonitoredVehicleJourney', {})
            location = journey.get('VehicleLocation', {})
            lat = location.get('Latitude')
            lon = location.get('Longitude')
            
            if not lat or not lon:
                continue
            
            vehicle_ref = journey.get('VehicleRef', 'Unknown')
            published_line_raw = journey.get('PublishedLineName', '')
            
            if isinstance(published_line_raw, list) and len(published_line_raw) > 0:
                published_line = published_line_raw[0]
            elif isinstance(published_line_raw, str):
                published_line = published_line_raw
            else:
                published_line = 'Unknown'
            
            destination_name = journey.get('DestinationName', 'Unknown')

            borough_info = get_borough_from_route(published_line)
            
            # Track counts
            borough_name = borough_info['name']
            if borough_name not in borough_counts:
                borough_counts[borough_name] = 0
            borough_counts[borough_name] += 1
            
            route_key = f"{borough_name}:{published_line}"
            if route_key not in route_counts:
                route_counts[route_key] = 0
            route_counts[route_key] += 1
            
            # Extract next stops
            next_stops = []
            onward_calls = journey.get('OnwardCalls', {})
            if onward_calls:
                calls = onward_calls.get('OnwardCall', [])
                for call in calls[:3]:
                    stop_name = call.get('StopPointName', 'Unknown')
                    next_stops.append(stop_name)
            
            bus_info = {
                'vehicle_id': vehicle_ref.replace('MTA NYCT_', '').replace('MTABC_', '').replace('MTA QVC_', '').replace('MTA BRKLM_', '').replace('MTA SI_', ''),
                'route': published_line,
                'latitude': lat,
                'longitude': lon,
                'destination': destination_name,
                'next_stops': next_stops,
                'borough': borough_name,
                'color': borough_info['color']
            }
            
            buses.append(bus_info)
        
        logger.info(f"Returning {len(buses)} buses")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'buses': buses,
                'total_count': len(buses),
                'borough_counts': borough_counts,
                'route_counts': route_counts
            })
        }
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({'error': str(e)})
        }
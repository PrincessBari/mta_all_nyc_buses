# NYC Live MTA Bus Tracker
An interactive map that visualizes every MTA bus currently in service across New York City, updating every 45 seconds with live data.

# Overview
As an NYC resident and avid MTA bus rider, I wanted a way to actually see bus options on a map rather than scrolling through a list.
This project pulls live data from the MTA Bus Time API and renders every active bus across all five boroughs on an interactive Mapbox
map. Hover or tap any bus to see its route, destination, and next stops — along with the full route path and bus stops drawn on the map.

# How It Works
The frontend is a single HTML file hosted on GitHub Pages. Every 45 seconds, the browser makes a call to an AWS Lambda function, which
fetches the latest data from the MTA Bus Time API and returns it to the map. Bus markers are color-coded by borough and update in place
without clearing the map, so buses appear to move smoothly over time.

Route naming: Considerable time was spent reverse engineering the MTA Bus Time API's route naming convention. The API returns a PublishedLineName
field that is sometimes a string and sometimes a list, requiring normalization before parsing. NYC bus routes follow a prefix-based system encoding
both borough and service type — M for Manhattan, B for Brooklyn, Q for Queens, BX for the Bronx, and S for Staten Island. Express routes get their
own category with prefixes like BXM, QM, BM, SIM, and X.

Route shapes: GTFS (General Transit Feed Specification) data was downloaded from the MTA, which includes a shapes.txt file for each borough and a
separate one for express buses. These coordinate sequences were converted to GeoJSON format and hosted on GitHub as a source layer in Mapbox, allowing
route paths to be drawn instantly when a user interacts with a bus or selects a route from the dropdown.

Bus stops: Stop location data was extracted from the MTA's GTFS stops.txt and stop_times.txt files across all six borough folders and converted to GeoJSON. 
Each stop is linked to its route by chaining stop_times.txt (which maps trip IDs to stop IDs) with trips.txt (which maps trip IDs to route IDs). When a user 
hovers a bus or selects a route, the stop circles appear along the route path alongside the route line.

# Technologies Used
- JavaScript / HTML / CSS
- Mapbox GL JS
- AWS Lambda
- MTA Bus Time API
- GTFS Shape Data (converted to GeoJSON)
- Python (for GTFS processing)
- GitHub Pages

# Running Locally
No installation required. Simply open index.html in a browser. The map will load and begin fetching live bus data automatically. Note that the Lambda
function must be active for data to appear.

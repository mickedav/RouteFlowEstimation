import time
import sys
from django.contrib.gis.geos import Point #Librart for creating GEO points
from googlemaps import Client #googlemaps API Library
from datetime import datetime

#Add your API-KEY
mapService = Client('API-KEY')

#Loop through locations and fetch routes between all OD-pairs
def getGoogleRoutes(vmidList, locationList, time_period, time_periods):

    test_time = datetime(2016,9,6,time_periods[1][1].hour, time_periods[1][1].minute, time_periods[1][1].second)
    routes = []
    print('Collecting routes between', len(vmidList), 'OD-paris')
    start_time = time.time()
    counter = 0
    for odPair in vmidList:

        elapsed_time = time.time() - start_time
        if elapsed_time >= 1:
            sys.stdout.write("\r%d%%" % ((counter/len(vmidList)*100)))
            sys.stdout.flush()

        for zone in locationList:
            if odPair[1] == zone[0]:
                x = [zone[1], zone[2]]
            if odPair[2] == zone[0]:
                y = [zone[1], zone[2]]

        directions = mapService.directions(x, y,
                                            'driving',
                                            None,
                                            True,
                                            None,
                                            'Swedish',
                                            'metric',
                                            None,
                                            test_time,
                                            None,
                                            False,
                                            None,
                                            None)

    #Add Each step of each route of each OD-pair
        for routeIndex, route in enumerate(directions):
            for stepIndex, step in enumerate(route['legs'][0]['steps']):
                data = {'od_id': odPair[0],
                        'route_id': str(odPair[0]) + ':' + str(routeIndex + 1) + ':' + str(time_period),
                        'start_point': Point(step['start_location']['lng'], step['start_location']['lat']).wkt,
                        'end_point': Point(step['end_location']['lng'], step['end_location']['lat']).wkt,
                        'route_index': routeIndex + 1,
                        'step_index': stepIndex + 1,
                        'travel_time': step['duration']['value'],
                        'distance': step['distance']['value'],
                        'time_period': time_period,
                        'start_time': test_time,
                        'date_inserted': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }

                routes.append(data)
        counter = counter + 1
    sys.stdout.write("\r%d%%" % ((100)))
    sys.stdout.flush()
    return routes

#Code for collecting pollyline, can be added to function above
def getGooglePolyline(x,y):
    directions = mapService.directions(x, y, 'driving', None, True)
    polyline = directions[0]['overview_polyline']['points']
    return polyline

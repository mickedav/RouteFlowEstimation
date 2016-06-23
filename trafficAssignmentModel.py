import math

def createRouteShares(routes, beta):
    counter = 0
    currentRoute = 0
    last_od = routes[len(routes) - 1][2]
    routes_to_calc = []
    routes_to_calc.append(routes[0])
    route_shares = []
    
    for index,route in enumerate(routes):
        od_id = route[2]
        route_index = route[3]
        travel_time = route[4]

        if currentRoute == od_id:
            routes_to_calc.append((route))
        else:
            if len(routes_to_calc) != 0:
                shares = getRouteShares(routes_to_calc, beta)
                for index, share in enumerate(shares):
                    route_data = [routes_to_calc[index][1], share, 'dynamic', beta]
                    route_shares.append(route_data)
            currentRoute = od_id
            routes_to_calc = []
            routes_to_calc.append((route))



    return route_shares


def getRouteShares(routes, beta):
    nmbrRoutes = len(routes)

    utilities = getUtilityFunctions(routes, beta)
    shares = calculateRouteShares(utilities)
    return shares

def getUtilityFunctions(routes, beta):
    utilities = []
    beta
    for route in routes:
        travel_time = route[4]/3600

        utilities.append(-1 * beta * travel_time)

    return(utilities)

def calculateRouteShares(utilities):
    exp = []
    shares = []
    den = 0
    for i in utilities:
        exp.append(math.exp(i))
        den = den + math.exp(i)

    for e in exp:
         shares.append(e/den)

    return shares

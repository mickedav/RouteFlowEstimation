import psycopg2

#Initialise connection to DB
def dbConnect(name, usr, pas):
    try:
        conn = psycopg2.connect("dbname=" + name + "user=" + usr + "host='localhost' password=" + pas + "")
        print('Connection to db established')
    except:
        print ("I am unable to connect to the database")

    cur = conn.cursor()
    return [cur, conn]

#Extract all beta values
def getBetaValues(cur):
    beta_values = None
    if cur != None:
        try:
            cur.execute("""SELECT *
                            FROM vm.logit_parameter_values""")

        except:
            print('I cant SELECT from database')

        beta_values = cur.fetchall()
        return beta_values

    else:
        print('No db connection established, try running dbConnect() first')
        return None

#USED
def getVmRoutes(cur):
    routes = None
    if cur != None:
        try:
            cur.execute("""SELECT *
                            FROM vm.vm_routes
                            ORDER BY time_period, od_id, route_index""")

        except:
            print('I cant SELECT from database')

        routes = cur.fetchall()
        return routes

    else:
        print('No db connection established, try running dbConnect() first')
        return None

def storeRouteShares(cur, conn, dataToStore):
    print(dataToStore)
    if cur != None:
        try:
        	cur.executemany("""INSERT INTO vm.route_shares (route_id, share, description, beta)
            VALUES (%s, %s, %s, %s)""", dataToStore)
        	conn.commit()
        except:
            print ("Can't write to database...")
    else:
        print('Not db connection established, try running dbConnect() first')

def networkLoadingRegionalCut(cur, conn, data):
    if cur != None:
        try:
            cur.execute("""INSERT INTO vm.geh_calibration(ref_lid_link, time_period, beta , od_matrix,  geh,model_flow, real_flow, road_type)
                            with route_flows AS(SELECT *
                                FROM
                                (
                                	SELECT q1.od_id, routeID AS route_id, q1.time_period AS time_period, demand*share AS flow
                                	FROM
                                	(
                                		SELECT vm.vm_routes.route_id AS routeID, vm.vm_routes.od_id AS od_id, vm.vm_routes.time_period AS time_period, share
                                		FROM vm.route_shares
                                		INNER JOIN vm.vm_routes
                                		ON vm.route_shares.route_id = vm.vm_routes.route_id
                                		WHERE beta = %s
                                	) AS q1
                                	INNER JOIN vm.vm_od_time_regional_cut
                                	ON vm.vm_od_time_regional_cut.od_id =  q1.od_id AND vm.vm_od_time_regional_cut.time_period = q1.time_period
                                ) AS q2),
                                linkflows AS( with test AS (SELECT time_period, routeID, ref_lid_link
                                FROM (SELECT *, vm.vm_routes.route_id AS routeID
                                FROM vm.vm_routes
                                INNER JOIN vm.route_step_links
                                ON vm.route_step_links.route_id = vm.vm_routes.route_id
                                WHERE step_usage IS NOT FALSE
                                ) AS FIRST
                                INNER JOIN vm.step_links
                                ON vm.step_links.step_id = FIRST.step_id
                                WHERE link_usage IS NOT FALSE
                                )
                                SELECT ref_lid_link, SUM(flow) AS flow, time_period
                                FROM (SELECT DISTINCT(ref_lid_link), route_id, flow, test.time_period
                                FROM test
                                INNER JOIN route_flows
                                ON route_flows.route_id = test.routeID
                                GROUP BY route_id,ref_lid_link, flow, test.time_period) AS q1
                                GROUP BY ref_lid_link,time_period),
                                test2 AS (SELECT sensor_id,vm.calibration_link_flows.ref_lid_link AS ref_lid_link,
                                vm.calibration_link_flows.time_period AS time_period,
                                linkflows.flow AS model_flow,
                                vm.calibration_link_flows.flow_private_cars AS real_flow,
                                sqrt(2*(linkflows.flow-vm.calibration_link_flows.flow_private_cars)^2/(linkflows.flow+vm.calibration_link_flows.flow_private_cars)) AS GEH

                                FROM vm.calibration_link_flows
                                INNER JOIN linkflows
                                ON vm.calibration_link_flows.ref_lid_link = linkflows.ref_lid_link AND vm.calibration_link_flows.time_period = linkflows .time_period
                                )

                                SELECT test2.ref_lid_link, time_period, %s as beta , 'regionalCut' AS od_matrix, geh,model_flow, real_flow, road_type
                                FROM  test2
                                INNER JOIN vm.sensor_locations
                                ON vm.sensor_locations.sensor_id = test2.sensor_id
                                ORDER BY time_period,  GEH""", data)
            conn.commit()

        except:
            print('Cant performe network loading')
    else:
        print('No db connection established, try running dbConnect() first')


def networkLoadingBrunnsvikenCut(cur, conn, data):
    if cur != None:
        try:
            cur.execute("""INSERT INTO vm.geh_calibration(ref_lid_link, time_period, beta , od_matrix,  geh,model_flow, real_flow, road_type)
                            with route_flows AS(SELECT *
                                FROM
                                (
                                	SELECT q1.od_id, routeID AS route_id, q1.time_period AS time_period, demand*share AS flow
                                	FROM
                                	(
                                		SELECT vm.vm_routes.route_id AS routeID, vm.vm_routes.od_id AS od_id, vm.vm_routes.time_period AS time_period, share
                                		FROM vm.route_shares
                                		INNER JOIN vm.vm_routes
                                		ON vm.route_shares.route_id = vm.vm_routes.route_id
                                		WHERE beta = %s
                                	) AS q1
                                	INNER JOIN vm.vm_od_time_brunnsviken_cut
                                	ON vm.vm_od_time_brunnsviken_cut.od_id =  q1.od_id AND vm.vm_od_time_brunnsviken_cut.time_period = q1.time_period
                                ) AS q2),
                                linkflows AS( with test AS (SELECT time_period, routeID, ref_lid_link
                                FROM (SELECT *, vm.vm_routes.route_id AS routeID
                                FROM vm.vm_routes
                                INNER JOIN vm.route_step_links
                                ON vm.route_step_links.route_id = vm.vm_routes.route_id
                                WHERE step_usage IS NOT FALSE
                                ) AS FIRST
                                INNER JOIN vm.step_links
                                ON vm.step_links.step_id = FIRST.step_id
                                WHERE link_usage IS NOT FALSE
                                )
                                SELECT ref_lid_link, SUM(flow) AS flow, time_period
                                FROM (SELECT DISTINCT(ref_lid_link), route_id, flow, test.time_period
                                FROM test
                                INNER JOIN route_flows
                                ON route_flows.route_id = test.routeID
                                GROUP BY route_id,ref_lid_link, flow, test.time_period) AS q1
                                GROUP BY ref_lid_link,time_period),
                                test2 AS (SELECT sensor_id,vm.calibration_link_flows.ref_lid_link AS ref_lid_link,
                                vm.calibration_link_flows.time_period AS time_period,
                                linkflows.flow AS model_flow,
                                vm.calibration_link_flows.flow_private_cars AS real_flow,
                                sqrt(2*(linkflows.flow-vm.calibration_link_flows.flow_private_cars)^2/(linkflows.flow+vm.calibration_link_flows.flow_private_cars)) AS GEH

                                FROM vm.calibration_link_flows
                                INNER JOIN linkflows
                                ON vm.calibration_link_flows.ref_lid_link = linkflows.ref_lid_link AND vm.calibration_link_flows.time_period = linkflows .time_period
                                )

                                SELECT test2.ref_lid_link, time_period, %s as beta , 'brunnsvikenCut' AS od_matrix, geh,model_flow, real_flow, road_type
                                FROM  test2
                                INNER JOIN vm.sensor_locations
                                ON vm.sensor_locations.sensor_id = test2.sensor_id
                                ORDER BY time_period,  GEH""", data)
            conn.commit()

        except:
            print('I cant SELECT from database')

        return 'Success!'

    else:
        print('No db connection established, try running dbConnect() first')
        return 'Fail!'

def storeGehTotalCalibrationRegionalCut(cur, conn, data):
    if cur != None:
        try:
            cur.execute(""" INSERT INTO vm.geh_total_calibration
                            with gehshare5 AS (with mainline_under5 AS (SELECT beta, od_matrix, road_type,COUNT(geh) AS mainline_under5
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'regionalCut' AND geh <5 AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type),

                            mainline_all5 AS (SELECT  beta, od_matrix, road_type,COUNT(geh) AS mainline_all
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'regionalCut' AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type
                            ),

                            mainline_over5 AS (SELECT beta, od_matrix, road_type,COUNT(geh) AS mainline_under5
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'regionalCut' AND geh >=5 AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type)

                            SELECT mainline_all5.beta,mainline_all5.od_matrix, CAST(mainline_under5 AS float)/CAST(mainline_all AS float) AS share_under_5
                            FROM mainline_all5
                            INNER JOIN mainline_under5
                            ON mainline_under5.beta = mainline_all5.beta),

                            gehshare10 AS (with mainline_under10 AS (SELECT beta, od_matrix, road_type,COUNT(geh) AS mainline_under10
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'regionalCut' AND geh <10 AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type),

                            mainline_all10 AS (SELECT  beta, od_matrix, road_type,COUNT(geh) AS mainline_all
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'regionalCut' AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type
                            ),

                            mainline_over10 AS (SELECT beta, od_matrix, road_type,COUNT(geh) AS mainline_under10
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'regionalCut' AND geh >=10 AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type)

                            SELECT mainline_all10.beta,mainline_all10.od_matrix, CAST(mainline_under10 AS float)/CAST(mainline_all AS float) AS share_under_10
                            FROM mainline_all10
                            INNER JOIN mainline_under10
                            ON mainline_under10.beta = mainline_all10.beta),


                            geh AS (SELECT sqrt((2*(sum_model_flow-sum_real_flow)^2)/(sum_model_flow+sum_real_flow)) AS GEH, %s  AS beta, 'regionalCut' AS od_matrix
                             FROM( SELECT SUM(model_flow) AS sum_model_flow, SUM(real_flow) AS sum_real_flow
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'regionalCut' AND road_type = 'mainline') AS q1),

                            gehny AS (SELECT geh,gehshare5.beta,gehshare5.od_matrix::text, share_under_5
                            FROM geh
                            INNER JOIN gehshare5
                            ON gehshare5.beta = geh.beta)

                            SELECT geh,gehny.beta,gehny.od_matrix,share_under_5,share_under_10
                            FROM gehny
                            INNER JOIN gehshare10
                            ON gehshare10.beta = gehny.beta AND gehshare10.od_matrix = gehny.od_matrix """, data)
            conn.commit()

        except:
            print('I cant SELECT from database')

        return 'Success!'

    else:
        print('No db connection established, try running dbConnect() first')
        return 'Fail!'

def storeGehTotalCalibrationBrunnsvikenCut(cur, conn, data):
    if cur != None:
        try:
            cur.execute(""" INSERT INTO vm.geh_total_calibration
                            with gehshare5 AS (with mainline_under5 AS (SELECT beta, od_matrix, road_type,COUNT(geh) AS mainline_under5
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'brunnsvikenCut' AND geh <5 AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type),

                            mainline_all5 AS (SELECT  beta, od_matrix, road_type,COUNT(geh) AS mainline_all
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'brunnsvikenCut' AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type
                            ),

                            mainline_over5 AS (SELECT beta, od_matrix, road_type,COUNT(geh) AS mainline_under5
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'brunnsvikenCut' AND geh >=5 AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type)

                            SELECT mainline_all5.beta,mainline_all5.od_matrix, CAST(mainline_under5 AS float)/CAST(mainline_all AS float) AS share_under_5
                            FROM mainline_all5
                            INNER JOIN mainline_under5
                            ON mainline_under5.beta = mainline_all5.beta),

                            gehshare10 AS (with mainline_under10 AS (SELECT beta, od_matrix, road_type,COUNT(geh) AS mainline_under10
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'brunnsvikenCut' AND geh <10 AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type),

                            mainline_all10 AS (SELECT  beta, od_matrix, road_type,COUNT(geh) AS mainline_all
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'brunnsvikenCut' AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type
                            ),

                            mainline_over10 AS (SELECT beta, od_matrix, road_type,COUNT(geh) AS mainline_under10
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'brunnsvikenCut' AND geh >=10 AND road_type = 'mainline'
                            GROUP BY beta,od_matrix,road_type)

                            SELECT mainline_all10.beta,mainline_all10.od_matrix, CAST(mainline_under10 AS float)/CAST(mainline_all AS float) AS share_under_10
                            FROM mainline_all10
                            INNER JOIN mainline_under10
                            ON mainline_under10.beta = mainline_all10.beta),

                            geh AS (SELECT sqrt((2*(sum_model_flow-sum_real_flow)^2)/(sum_model_flow+sum_real_flow)) AS GEH, %s  AS beta, 'brunnsvikenCut' AS od_matrix
                            FROM( SELECT SUM(model_flow) AS sum_model_flow, SUM(real_flow) AS sum_real_flow
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'brunnsvikenCut' AND road_type = 'mainline') AS q1),

                            gehny AS (SELECT geh,gehshare5.beta,gehshare5.od_matrix::text, share_under_5
                            FROM geh
                            INNER JOIN gehshare5
                            ON gehshare5.beta = geh.beta)

                            SELECT geh,gehny.beta,gehny.od_matrix,share_under_5,share_under_10
                            FROM gehny
                            INNER JOIN gehshare10
                            ON gehshare10.beta = gehny.beta AND gehshare10.od_matrix = gehny.od_matrix
                            """, data)
            conn.commit()

        except:
            print('I cant SELECT from database')

        return 'Success!'

    else:
        print('No db connection established, try running dbConnect() first')
        return 'Fail!'

def storeGehAcceptanceCalibrationBrunnsvikenCut(cur, conn, data):
    if cur != None:
        try:
            cur.execute(""" INSERT INTO vm.geh_acceptance_calibration
                            with mainline_under5 AS (SELECT time_period, beta,od_matrix, road_type,COUNT(geh) AS mainline_under5
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'brunnsvikenCut' AND geh <5 AND road_type = 'mainline'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            mainline_under10 AS (SELECT time_period, beta,od_matrix, road_type,COUNT(geh) AS mainline_under10
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'brunnsvikenCut' AND geh <10 AND road_type = 'mainline'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            mainline_all AS (SELECT time_period, beta, od_matrix, road_type,COUNT(geh) AS mainline_all
                            FROM vm.geh_calibration
                            WHERE beta = %s   AND od_matrix =  'brunnsvikenCut' AND road_type = 'mainline'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            mainline_over5 AS (SELECT time_period,beta,od_matrix,road_type, 0 AS mainline_under5
                            FROM mainline_all
                            WHERE time_period NOT IN (SELECT time_period FROM mainline_under5)),

                            mainline_over10 AS (SELECT time_period,beta,od_matrix,road_type, 0 AS mainline_under10
                            FROM mainline_all
                            WHERE time_period NOT IN (SELECT time_period FROM mainline_under10)),

                            mainline_under_all_5 AS (SELECT *
                            FROM mainline_under5
                            UNION
                            SELECT *
                            FROM mainline_over5),

                            mainline_under_all_10 AS (SELECT *
                            FROM mainline_under10
                            UNION
                            SELECT *
                            FROM mainline_over10),

                            mainline5 AS (SELECT mainline_under_all_5.time_period,mainline_under_all_5.beta,mainline_under_all_5.od_matrix, CAST(mainline_under5 AS float)/CAST(mainline_all AS float)AS  mainline_share
                            FROM mainline_under_all_5
                            INNER JOIN mainline_all
                            ON mainline_under_all_5.time_period = mainline_all.time_period),

                            mainline10 AS (SELECT mainline_under_all_10.time_period,mainline_under_all_10.beta,mainline_under_all_10.od_matrix, CAST(mainline_under10 AS float)/CAST(mainline_all AS float)AS  mainline_share
                            FROM mainline_under_all_10
                            INNER JOIN mainline_all
                            ON mainline_under_all_10.time_period = mainline_all.time_period),

                            onoff_under5 AS (SELECT time_period, beta, od_matrix, road_type,COUNT(geh) AS onoff_under_5
                            FROM vm.geh_calibration
                            WHERE beta = %s    AND od_matrix =  'brunnsvikenCut' AND geh <5 AND road_type = 'on/off-ramp'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            onoff_under10 AS (SELECT time_period, beta, od_matrix, road_type,COUNT(geh) AS onoff_under_10
                            FROM vm.geh_calibration
                            WHERE beta =%s    AND od_matrix =  'brunnsvikenCut' AND geh <10 AND road_type = 'on/off-ramp'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            onoff_all AS (SELECT time_period, beta, od_matrix, road_type,COUNT(geh) AS onoff_all
                            FROM vm.geh_calibration
                            WHERE beta = %s   AND od_matrix = 'brunnsvikenCut' AND road_type = 'on/off-ramp'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            onoff_over5 AS (SELECT time_period,beta,od_matrix,road_type, 0 AS onoff_under_5
                            FROM onoff_all
                            WHERE time_period NOT IN (SELECT time_period FROM onoff_under5)),

                            onoff_over10 AS (SELECT time_period,beta,od_matrix,road_type, 0 AS onoff_under_10
                            FROM onoff_all
                            WHERE time_period NOT IN (SELECT time_period FROM onoff_under10)),

                            onoff_under_all5 AS (SELECT *
                            FROM onoff_under5
                            UNION
                            SELECT *
                            FROM onoff_over5),

                            onoff_under_all10 AS (SELECT *
                            FROM onoff_under10
                            UNION
                            SELECT *
                            FROM onoff_over10),

                            totalGeh AS (SELECT time_period, beta, od_matrix, road_type,sqrt((2*(SUM(model_flow)-SUM(real_flow))^2)/(SUM(model_flow)+SUM(real_flow))) AS total_geh
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix =  'brunnsvikenCut' AND road_type = 'mainline'
                            GROUP BY time_period, beta,od_matrix,road_type
                            ORDER BY time_period),

                            shares5 AS (SELECT timeperiod, beta,od_matrix,onoff_share,mainline_share
                            FROM (SELECT CAST(onoff_under_5 AS float)/CAST(onoff_all AS float)AS onoff_share, onoff_under_all5.time_period AS timeperiod
                            FROM onoff_under_all5
                            INNER JOIN onoff_all
                            ON onoff_under_all5.time_period = onoff_all.time_period) AS q1
                            INNER JOIN mainline5
                            ON mainline5.time_period = q1.timeperiod),

                            shares10 AS (SELECT timeperiod, beta,od_matrix,onoff_share,mainline_share
                            FROM (SELECT CAST(onoff_under_10 AS float)/CAST(onoff_all AS float)AS onoff_share, onoff_under_all10.time_period AS timeperiod
                            FROM onoff_under_all10
                            INNER JOIN onoff_all
                            ON onoff_under_all10.time_period = onoff_all.time_period) AS q1
                            INNER JOIN mainline10
                            ON mainline10.time_period = q1.timeperiod)

                            SELECT time_period, shares10.beta, shares10.od_matrix,  onoff_under_5, onoff_share AS onoff_under_10, mainline_under_5,mainline_share AS mainline_under_10,total_geh
                            FROM (SELECT time_period, totalGeh.beta, totalGeh.od_matrix,  onoff_share AS onoff_under_5, mainline_share AS mainline_under_5, total_geh FROM shares5
                            INNER JOIN totalGeh
                            ON shares5.timeperiod = totalGeh.time_period AND shares5.beta = totalGEH.beta AND shares5.od_matrix = totalGeh.od_matrix) AS q1
                            INNER JOIN shares10
                            ON shares10.timeperiod = q1.time_period
                            """, data)
            conn.commit()

        except:
            print('I cant SELECT from database')

        return 'Success!'

    else:
        print('No db connection established, try running dbConnect() first')
        return 'Fail!'

def storeGehAcceptanceCalibrationRegionalCut(cur, conn, data):
    if cur != None:
        try:
            cur.execute(""" INSERT INTO vm.geh_acceptance_calibration
                            with mainline_under5 AS (SELECT time_period, beta,od_matrix, road_type,COUNT(geh) AS mainline_under5
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix = 'regionalCut' AND geh <5 AND road_type = 'mainline'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            mainline_under10 AS (SELECT time_period, beta,od_matrix, road_type,COUNT(geh) AS mainline_under10
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix =  'regionalCut' AND geh <10 AND road_type = 'mainline'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            mainline_all AS (SELECT time_period, beta, od_matrix, road_type,COUNT(geh) AS mainline_all
                            FROM vm.geh_calibration
                            WHERE beta = %s   AND od_matrix =   'regionalCut' AND road_type = 'mainline'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            mainline_over5 AS (SELECT time_period,beta,od_matrix,road_type, 0 AS mainline_under5
                            FROM mainline_all
                            WHERE time_period NOT IN (SELECT time_period FROM mainline_under5)),

                            mainline_over10 AS (SELECT time_period,beta,od_matrix,road_type, 0 AS mainline_under10
                            FROM mainline_all
                            WHERE time_period NOT IN (SELECT time_period FROM mainline_under10)),

                            mainline_under_all_5 AS (SELECT *
                            FROM mainline_under5
                            UNION
                            SELECT *
                            FROM mainline_over5),

                            mainline_under_all_10 AS (SELECT *
                            FROM mainline_under10
                            UNION
                            SELECT *
                            FROM mainline_over10),

                            mainline5 AS (SELECT mainline_under_all_5.time_period,mainline_under_all_5.beta,mainline_under_all_5.od_matrix, CAST(mainline_under5 AS float)/CAST(mainline_all AS float)AS  mainline_share
                            FROM mainline_under_all_5
                            INNER JOIN mainline_all
                            ON mainline_under_all_5.time_period = mainline_all.time_period),

                            mainline10 AS (SELECT mainline_under_all_10.time_period,mainline_under_all_10.beta,mainline_under_all_10.od_matrix, CAST(mainline_under10 AS float)/CAST(mainline_all AS float)AS  mainline_share
                            FROM mainline_under_all_10
                            INNER JOIN mainline_all
                            ON mainline_under_all_10.time_period = mainline_all.time_period),

                            onoff_under5 AS (SELECT time_period, beta, od_matrix, road_type,COUNT(geh) AS onoff_under_5
                            FROM vm.geh_calibration
                            WHERE beta = %s    AND od_matrix =   'regionalCut' AND geh <5 AND road_type = 'on/off-ramp'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            onoff_under10 AS (SELECT time_period, beta, od_matrix, road_type,COUNT(geh) AS onoff_under_10
                            FROM vm.geh_calibration
                            WHERE beta =%s    AND od_matrix =   'regionalCut' AND geh <10 AND road_type = 'on/off-ramp'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            onoff_all AS (SELECT time_period, beta, od_matrix, road_type,COUNT(geh) AS onoff_all
                            FROM vm.geh_calibration
                            WHERE beta = %s   AND od_matrix =  'regionalCut' AND road_type = 'on/off-ramp'
                            GROUP BY time_period,beta,od_matrix,road_type
                            ORDER BY time_period),

                            onoff_over5 AS (SELECT time_period,beta,od_matrix,road_type, 0 AS onoff_under_5
                            FROM onoff_all
                            WHERE time_period NOT IN (SELECT time_period FROM onoff_under5)),

                            onoff_over10 AS (SELECT time_period,beta,od_matrix,road_type, 0 AS onoff_under_10
                            FROM onoff_all
                            WHERE time_period NOT IN (SELECT time_period FROM onoff_under10)),

                            onoff_under_all5 AS (SELECT *
                            FROM onoff_under5
                            UNION
                            SELECT *
                            FROM onoff_over5),

                            onoff_under_all10 AS (SELECT *
                            FROM onoff_under10
                            UNION
                            SELECT *
                            FROM onoff_over10),

                            totalGeh AS (SELECT time_period, beta, od_matrix, road_type,sqrt((2*(SUM(model_flow)-SUM(real_flow))^2)/(SUM(model_flow)+SUM(real_flow))) AS total_geh
                            FROM vm.geh_calibration
                            WHERE beta = %s  AND od_matrix =   'regionalCut' AND road_type = 'mainline'
                            GROUP BY time_period, beta,od_matrix,road_type
                            ORDER BY time_period),

                            shares5 AS (SELECT timeperiod, beta,od_matrix,onoff_share,mainline_share
                            FROM (SELECT CAST(onoff_under_5 AS float)/CAST(onoff_all AS float)AS onoff_share, onoff_under_all5.time_period AS timeperiod
                            FROM onoff_under_all5
                            INNER JOIN onoff_all
                            ON onoff_under_all5.time_period = onoff_all.time_period) AS q1
                            INNER JOIN mainline5
                            ON mainline5.time_period = q1.timeperiod),

                            shares10 AS (SELECT timeperiod, beta,od_matrix,onoff_share,mainline_share
                            FROM (SELECT CAST(onoff_under_10 AS float)/CAST(onoff_all AS float)AS onoff_share, onoff_under_all10.time_period AS timeperiod
                            FROM onoff_under_all10
                            INNER JOIN onoff_all
                            ON onoff_under_all10.time_period = onoff_all.time_period) AS q1
                            INNER JOIN mainline10
                            ON mainline10.time_period = q1.timeperiod)

                            SELECT time_period, shares10.beta, shares10.od_matrix,  onoff_under_5, onoff_share AS onoff_under_10, mainline_under_5,mainline_share AS mainline_under_10,total_geh
                            FROM (SELECT time_period, totalGeh.beta, totalGeh.od_matrix,  onoff_share AS onoff_under_5, mainline_share AS mainline_under_5, total_geh FROM shares5
                            INNER JOIN totalGeh
                            ON shares5.timeperiod = totalGeh.time_period AND shares5.beta = totalGEH.beta AND shares5.od_matrix = totalGeh.od_matrix) AS q1
                            INNER JOIN shares10
                            ON shares10.timeperiod = q1.time_period  """, data)
            conn.commit()

        except:
            print('I cant SELECT from database')

        return 'Success!'

    else:
        print('No db connection established, try running dbConnect() first')
        return 'Fail!'

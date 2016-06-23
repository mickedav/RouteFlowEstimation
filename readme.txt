#Route Flow Estimation

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Scriptfiles to run %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  routeCollector.py
Used to collect routes using the Google directions API. Stores data in a postgreSQL DB.

  routeShareCalc.py
Uses the data from the routeCollector to estimate route shares.  Stores data in a postgreSQL DB.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%    Utility files   %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

  dbUtil.py
Contains functions with SQL quaries used to collect and store data in the choosen database.
Needs a lot of changes in order to switch database.

  dbUtil_networkLoading.py
Contains functions with SQL quaries tied to the network loading used to collect and store
data in the chosen database. Needs a lot of changes in order to switch database.

  googleRoutes.py
Scriptfile containing the functionality to fetch data from Google and convert it into the
format that can be stored in the postgreSQL db. Needs an API-key.

  trafficAssignmentModel.py
The model used to calculate the route shares.

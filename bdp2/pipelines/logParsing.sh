#!/usr/bin/env bash
echo "Running logParsing.sh"

# ---- APACHE ----
cd ./../data/apacheLog || exit

# -- Q1 --
echo "-- Q1 --"
# Write a pipeline that gets all POST requests from the `access_log` file and displays time, clientIP, path, statusCode and size. These values should be comma-separated.
# Example output: 10/Mar/2004:12:02:59,10.0.0.153,/cgi-bin/mailgraph2.cgi,200,2987
accessData=$()
# Print the accessData
echo "Access data:"
echo "$accessData"


echo "--------"


# -- Q2 --
echo "-- Q2 --"
# Write a pipeline that returns the IP address and the path of the largest-sized response to a POST request.
# Example output: 192.168.0.1,/actions/logout
# Hint: you could re-use the `accessData` variable to make it easier.
largestResponse=$()
echo "The largest Response was to:"
echo "$largestResponse"


echo "--------"


# -- Q3--
echo "-- Q3 --"
# Write a pipeline that returns the amount and the IP address of the client that sent the most POST requests.
# Example output: 20 192.168.0.1
# Hint: you could re-use the `accessData` variable to make it easier.
mostRequests=$()
echo "The most requests where done by:"
echo "$mostRequests"

echo "--------"

# End on start path.
cd ../../pipelines/ || exit

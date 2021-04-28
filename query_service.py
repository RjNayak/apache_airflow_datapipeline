from flask import Flask, request, Response
import psycopg2
import json

app = Flask(__name__)
con = psycopg2.connect(database="crosslend", user="postgres",
                       password="Omsairam", host="127.0.0.1", port="5432")
cursor = con.cursor()


@app.route('/tlc/trendanalysis//v1.0/pickuptodrop', methods=['POST'])
def get_montly_trends():
    """ Gets the top 5 drop location for a specific pick up location for a specific month
    :param json: a json object with 2 properties month and pickup location
    :type : str
    :returns json: A list of json object
    :rtype : json
    """
    query = request.get_json()
    month_value = query['month']
    pickup_value = query['pick_up_location']
    query_string = "Select * from history_data where month = " + \
        "'" + month_value + "' AND pick_up = '" + pickup_value + "'"
    cursor.execute(query_string)
    result = cursor.fetchall()
    response_object = json.dumps(result)
    response_code = 200
    return Response(response_object, response_code, mimetype='application/json')


if __name__ == "__main__":
    app.run(host="0.0.0.0")

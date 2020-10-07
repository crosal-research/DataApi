# import from system
import os, json

# imports of packages
from bottle import Bottle, run, request, response, static_file #, template, TEMPLATE
from truckpad.bottle.cors import CorsPlugin, enable_cors

import pandas as pd

# apps imports
from DB.transactions import fetch_multi_series
from Search.transactions import query_docs


# App definition
root=os.path.join(os.path.dirname(__file__), 'static')
app = Bottle()


######## Webpage
@app.get("/")
def home():
    return env.get_template("index.html").render()


@app.get('/<filepath:re:.*\.html>')
def index_view(filepath):
    return env.get_template(filepath).render()

@app.get("/static/css/<filepath:re:.*\.css>")
def css(filepath):
    return static_file(filepath, root=root + "/css")


########### API
@enable_cors
@app.get('/api')
def fetch_data():
    srs = request.query.getall("series")
    fort = request.query.get("format")
    headers = request.headers
    if fort is None:
        fort = "csv"
        
    if len(srs) == 0:
        return "Please, add a series to your request"
    else:
        try:
            if fort == "csv":
                return fetch_multi_series(srs).to_csv()
            else:
                return fetch_multi_series(srs).to_json(date_unit="s", 
                                                       date_format="iso")
        except:
            return "not valid series"


@enable_cors
@app.get('/search')
def fetch_data():
    words = request.query.getall("words")
    fort = request.query.get("format")
    
    if len(words) == 0:
        return "Please, add words for you search"
    else:
        sentence = words[0].encode("latin1").decode("utf-8")

        if fort == "csv":
            return pd.DataFrame(query_docs(sentence)).to_csv()
        else:
            dj = query_docs(sentence)
            response.content_type = 'application/json'
            return json.dumps(dj)

app.install(CorsPlugin(origins=['http://localhost:5000']))
run(app, host='localhost', port=8090, debbug=True)

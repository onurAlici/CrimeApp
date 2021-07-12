from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine, MetaData, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy.schema import Table
from geoalchemy2 import Geometry
from sqlalchemy.sql import select
import json

app = Flask(__name__)


metadata = MetaData()
engine = create_engine("Database URL", echo = True)

search = Table("search", metadata, autoload_with=engine)
outcomes = Table("outcomes", metadata, autoload_with=engine)
street = Table("street", metadata, autoload_with=engine)
local = Table("local", metadata, autoload_with=engine)

Session = scoped_session(sessionmaker(bind=engine))




@app.route('/')
def hello_world():
    s = select(local.c.objectid.label("id"),
               local.c.lad20nm.label("name"),
               func.ST_AsGeoJSON(local.c.geometry).label("geojson"))
    rows = Session.execute(s).all()

    cevap = {   }
    for row in rows:
        cevap[str(row.id)] = {"geo":row.geojson,"name":row.name }
    cevap = jsonify(cevap)
    cevap.headers.add('Access-Control-Allow-Origin', '*')

    return cevap

@app.route('/crimes')
def crimes():
    id = request.args.get('id')
    startDate = request.args.get('start')
    endDate = request.args.get('end')

    poligon = select(local).where(local.c.objectid == id)

    searchs = select(search, func.ST_AsGeoJSON(search.c.geometry).label("geo")).where(
        search.c.geometry.ST_Within(poligon.columns.geometry),
        search.c.Date < endDate,
        search.c.Date > startDate
    )
    outcomess = select(outcomes, func.ST_AsGeoJSON(outcomes.c.geometry).label("geo")).where(
        outcomes.c.geometry.ST_Within(poligon.columns.geometry),
        outcomes.c.Month < endDate,
        outcomes.c.Month > startDate
    )
    streets = select(street, func.ST_AsGeoJSON(street.c.geometry).label("geo")).where(
        street.c.geometry.ST_Within(poligon.columns.geometry),
        street.c.Month < endDate,
        street.c.Month > startDate
    )
    #collection = {"type": "FeatureCollection", "features":[]}
    """cevap = {"searchs":{"type": "FeatureCollection", "features":[]},
             "outcomes":{"type": "FeatureCollection", "features":[]},
             "street":{"type": "FeatureCollection", "features":[]}}"""

    cevap = {"type": "FeatureCollection", "features":[]}
    idNumber = 0
    rows = Session.execute(searchs).all()

    for row in rows:
        row = dict(row)
        row.pop("geometry", None)
        feature = {"type": "Feature"}
        feature["geometry"] = json.loads(row["geo"])
        feature["properties"] = {**row, "id":str(idNumber), "tag":"search"}
        cevap["features"].append(feature)
        idNumber += 1
    #doing geojson with properties 28 haziran 23:46
    rows = Session.execute(outcomess).all()
    for row in rows:
        row = dict(row)
        row.pop("geometry", None)
        feature = {"type": "Feature"}
        feature["geometry"] = json.loads(row["geo"])
        feature["properties"] = {**row, "id": str(idNumber), "tag":"outcome"}
        cevap["features"].append(feature)
        idNumber += 1
    rows = Session.execute(streets).all()
    for row in rows:
        row = dict(row)
        row.pop("geometry", None)
        feature = {"type": "Feature"}
        feature["geometry"] = json.loads(row["geo"])
        feature["properties"] = {**row, "id": str(idNumber), "tag":"street"}
        cevap["features"].append(feature)
        idNumber += 1
    cevap = jsonify(cevap)
    cevap.headers.add('Access-Control-Allow-Origin', '*')

    return cevap

@app.teardown_appcontext
def cleanup(resp_or_exc):
    Session.remove()

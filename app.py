from flask import Flask,render_template
import libgrid
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def home():
#    return render_template('index.html')
    return "OK", 200

@app.route("/grid/gid/<gid>")
def from_gid(gid):
    # GID Parameter Check
    if len(gid)==14:
        pass
    else:
        return {"error":"GID must be level 14"}
    
    # Process
    try:
        return {"value":libgrid.get_value_from_gid(str(gid)),"gid":gid}
    except:
        return {"error":"Error generating interpolated data"}

@app.route("/grid/coordinate/<lon>/<lat>")
def from_lonlat(lon,lat):
    # Coordinate validation
    if -180<=float(lon)<=180:
        pass
    else:
        return {"error":"Longitude must be -180 to 180"}
    
    if -90<=float(lat)<=90:
        pass
    else:
        return {"error":"Longitude must be -90 to 90"}
    
    lon = float(lon)
    lat = float(lat)

    # Process
    try:
        return {"value":libgrid.get_value_from_gid(libgrid.lonlat_to_gid(lon,lat,14)),"gid":libgrid.lonlat_to_gid(lon,lat,14)}
    except:
        return {"error":"Error generating interpolated data"}

if __name__ == "__main__":
    app.run()

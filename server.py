from flask import Flask
app = Flask(__name__, static_url_path="/static")

@app.route('/', defaults={'path': 'index.html'})
@app.route('/<path:path>')
def catch_all(path):
    return app.send_static_file(path) # better to do on nginx

if __name__ == "__main__":
    #app.run(host='0.0.0.0') # run publicly
    app.run(debug=True)
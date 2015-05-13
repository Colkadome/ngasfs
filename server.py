from flask import Flask
app = Flask(__name__)

@app.route('/', defaults={'path': 'index.html'})
@app.route('/<path:path>')
def catch_all(path):
	return app.send_static_file(path)

if __name__ == '__main__':
	app.run(debug=True)
	#app.run(host='0.0.0.0')
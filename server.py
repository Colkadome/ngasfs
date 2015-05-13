
# other imports here

######

from flask import Flask
from flask import request
app = Flask(__name__)


@app.route('/create_fs', methods=['POST'])
def create_fs():
	fsName = request.form['fsName']
	return "Successfully created " + fsName

@app.route('/', defaults={'path': 'index.html'})
@app.route('/<path:path>')
def catch_all(path):
	return app.send_static_file(path)

if __name__ == '__main__':
	app.run(debug=True)
	#app.run(host='0.0.0.0')
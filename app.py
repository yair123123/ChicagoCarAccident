from flask import Flask
from controllers.accidents_controller import accident_blueprint

app = Flask(__name__)
app.register_blueprint(accident_blueprint,url_prefix= "/api/db/")
if __name__ == '__main__':
    app.run(debug=True)

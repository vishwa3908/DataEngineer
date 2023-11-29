from flask import Flask

app = Flask(__name__)

@app.route('/',methods=['GET'])
def hello():
    return "Welcome to Flask App"

if __name__ == "__main__":
    app.run(debug=True)
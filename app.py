
from flask import Flask, render_template

app = Flask(__name__)
name = "Finn"
@app.route('/')
def user():
    return render_template('index.html', name=name)

@app.route('/script/', methods=['POST'])
def check():
    return render_template('script.html')

if __name__ == '__main__':
    app.run(debug=True)
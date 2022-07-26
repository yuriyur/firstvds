# app.py
import redis
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.get("/tasks")
def get_tasks():
    with redis.Redis() as client:
        tasks = client.llen('tasks')
        print(tasks, type(tasks))
    return jsonify({'Number of tasks':tasks})

@app.get("/answer")
def get_answer():
    with redis.Redis() as client:
        id = request.args.get('id')
        answers = client.lrange('answers', id, id)[0].decode('UTF-8')
    return jsonify({'Answer':answers})

@app.post("/addfilename")
def add_file_name():
    if request.is_json:
        file_name = request.get_json()
        with redis.Redis() as client:
            client.lpush('tasks', file_name['file_path'])
        return file_name, 201
    return {"error": "Request must be JSON"}, 415

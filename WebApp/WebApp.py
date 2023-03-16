import os
from flask import Flask, render_template

app = Flask(__name__)
dir_path = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(dir_path, 'chapters.json')

@app.route('/')
def start():

    import json

    with open(file_path, 'r') as f:
        data = json.load(f)

    chapters = data['chapters']

    return render_template('start.html', chapters=chapters, chapter_id=None)

@app.route('/chapter/<int:id>')
def chapter(id):

    import json

    with open(file_path, 'r') as f:
        data = json.load(f)

    chapters = data['chapters']

    for chapter in chapters:
        if chapter['id'] == id:
            return render_template('chapter.html', chapter=chapter, chapter_id=id)
    return 'Chapter not found'

if __name__ == '__main__':
    app.run()  

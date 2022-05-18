# Transform list of json files into postgresql models using python multiprocessing

- python3 -m venv venv
- source venv/bin/activate
- pip install -r requirements.pip
- docker compose up
- Add urls to parse in settings
- python3 transform_messages

If you are running osx, stop your local postgres: `brew services stop postgresql`

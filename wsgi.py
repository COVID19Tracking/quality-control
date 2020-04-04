import os
import logging
from loguru import logger
from flask import Flask

from flaskapp import create_app
from app.webhooks import webhook


if __name__ == "__main__":

    app = create_app()

    # propogate logging
    gunicorn_logger = logging.getLogger("gunicorn.error")
    if gunicorn_logger != None:
        app.logger.handlers = gunicorn_logger.handlers
        app.logger.setLevel(gunicorn_logger.level)
    
    # for auto-update
    app.config['GITHUB_SECRET'] = os.environ.get('GITHUB_SECRET')
    app.config['REPO_PATH'] = os.environ.get('REPO_PATH')
    app.register_blueprint(webhook)

    app.run()

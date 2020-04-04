# from https://fedoramagazine.org/continuous-deployment-github-python/

import hmac
from loguru import logger
from flask import request, Blueprint, jsonify, current_app 
from git import Repo

g_last_commit = ""

@property
def last_deployed_commit() -> str:
    return g_last_commit

webhook = Blueprint('webhook', __name__, url_prefix='')

@webhook.route('/github', methods=['POST']) 
def handle_github_hook():  
    """ Entry point for github webhook """
    global g_last_commit

    try:      
        signature = request.headers.get('X-Hub-Signature') 
        sha, signature = signature.split('=')

        secret = str.encode(current_app.config.get('GITHUB_SECRET'))

        hashhex = hmac.new(secret, request.data, digestmod='sha1').hexdigest()
        if hmac.compare_digest(hashhex, signature): 
            repo = Repo(current_app.config.get('REPO_PATH')) 
            origin = repo.remotes.origin 
            origin.pull('--rebase')

        commit = request.json['after'][0:6]
        g_last_commit = commit
        logger.info(f'Repository updated with commit {commit}')

        return jsonify({"status": "okay", "message": ""}), 200
    except Exception as ex:
        logger.exception(ex)
        logger.error("deploy failed")
        g_last_commit = "[deploy failed]"
        return jsonify({"status": "failed", "message": f"{ex}"}), 500
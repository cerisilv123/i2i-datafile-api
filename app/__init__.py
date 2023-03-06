import secrets
from flask import Flask, request, jsonify, send_file
import os

from dotenv import load_dotenv

from app.run import run, update_table_tblInvoicesSentToDatafile, delete_file_from_directory

def create_app():
    # 1) Loading envars from .env
    load_dotenv()
    
    # Creating Flask app
    app = Flask(__name__)
    
    # Configure app settings here
    app.config['DEBUG'] = True
    session_key = secrets.token_urlsafe(16)
    app.config["SECRET_KEY"] = f"{session_key}" 
    app.secret_key = session_key 

    # Register API routes and blueprints
    from app.api.api import api

    app.register_blueprint(api, url_prefix='/')
    
    return app
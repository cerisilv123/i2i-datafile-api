import secrets
from flask import Flask, request, jsonify, send_file
import os

from datetime import datetime
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

    # Define API routes here
    @app.route('/api/v1/cyclops/invoices/file')
    def file():
        
        # Getting header data to check for API Key
        headers = request.headers
        auth = headers.get('Api-Key')
        
        # Checking if user making GET request has provided API key 
        if auth == os.getenv('API_KEY'):
            
            # Running function to generate CSV file
            csv_file, session, filename = run()
            
            # Checking if CSV file exists and there was Data
            if csv_file is not None: 
                try: 
                    file_path = os.path.join(os.getcwd(), 'files', f'{filename}.csv')
                    update_table_tblInvoicesSentToDatafile(csv_file, session)
                    return send_file(file_path, as_attachment=True)
                except: 
                    return jsonify({"message": "ERROR: Unable to create file "}), 401
            else: 
                return jsonify({"message": "NO DATA: No new file data to retreive"})
        else:
            return jsonify({"message": "ERROR: Unauthorized"}), 401
    
    return app
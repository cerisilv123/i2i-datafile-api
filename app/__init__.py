import secrets
from flask import Flask, request, jsonify, send_file
import os

from app.run import run, update_table_tblInvoicesSentToDatafile, delete_file_from_directory

def create_app():
    app = Flask(__name__)
    
    # Configure app settings here
    app.config['DEBUG'] = True
    session_key = secrets.token_urlsafe(16)
    app.config["SECRET_KEY"] = f"{session_key}" 
    app.secret_key = session_key 

    # Define API routes here
    @app.route('/api/v1/cyclops/invoices/file')
    def file():
        
        headers = request.headers
        auth = headers.get("X-Api-Key")
                
        if auth == os.getenv('API_KEY'):
            
            csv_file, session, filename = run()
    
            if csv_file is not None: 
                file_path = os.path.join(os.getcwd(), 'files', f'{filename}.csv')
                update_table_tblInvoicesSentToDatafile(csv_file, session)
                return send_file(file_path, as_attachment=True)
            else: 
                return jsonify({"message": "ERROR: No new file data to retreive"}), 401
        else:
            return jsonify({"message": "ERROR: Unauthorized"}), 401
    
    return app
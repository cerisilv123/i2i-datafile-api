from app import create_app
from app.run import run, update_table_tblInvoicesSentToDatafile, delete_file_from_directory

app = create_app()
    
if __name__ == "__main__":
    app.run(debug=True)
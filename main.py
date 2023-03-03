from app.run import run, update_table_tblInvoicesSentToDatafile, delete_file_from_directory

def main():
    csv_file, session, filename = run()
    
    if csv_file is not None: 
        update_table_tblInvoicesSentToDatafile(csv_file, session)
        delete_file_from_directory(filename)
    else: 
        "No data to update in file. - return this in GET request."
    
if __name__ == "__main__":
    main()
import os 
import logging
import pandas as pd
import time
from datetime import datetime

from app.services.connection_services import create_connection, create_connection_cursor
from app.services.i2i_services import insert_invoices_paid_transactions_from_datafile, get_transaction_data_new_ids, insert_payment_transaction, insert_invoices_sent_to_datafile_row, get_transaction_data_by_type, get_tax_code, database_table_exists, create_database_table, get_so_inv_masters_approved, get_so_inv_details, get_customer_by_so_num, has_invoice_already_been_sent_to_datafile
from app.services.helper_services import append_to_csv
from app.models.i2i_daos import QIPM, Payments, Currencies, InvoicesSentToDatafile, InvoicesPaidTransactionsFromDatafile, SOInvMaster, SOInvDetail, SoMaster, CCCust, CYCSLA, CYCSLT

def update_table_tblInvoicesSentToDatafile(csv_file, session):
    # 7.1) Updating tblInvoicesSentToDatafile with invoices that have been posted
    for index, row in csv_file.iterrows():
        if row['Type'] == 1:
            invoice_number_string = str(row['InvoiceNumber'])
            invoice_number_string = f"{invoice_number_string:>08}"
            insert_invoices_sent_to_datafile_row(session, InvoicesSentToDatafile, invoice_number_string)
            
def delete_file_from_directory(filename):
    # 8) Deleting file from directory now it is posted
    working_directory = os.getcwd()
    path_csv = f"{working_directory}/files" 
    os.remove(f"{path_csv}/{filename}.csv")

def run(): 
    
        logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
        logger = logging.getLogger()
        
        # 2) Creating connection to Cyclops i2i DB (SQL Alchemy ORM) & i2i DB via pyodbc. SQL allchemy is used for reading. pyodbc is used for writing
        engine, session = create_connection()
        conn, cursor = create_connection_cursor()
        print(conn)
        logger.info("2) Connection to i2i DB created Successful via pyodbc")
        
        # 3) Creating connection to i2i Cyclops-Datfile mirrored DB (SQL Alchemy ORM). SQL alchemy is used for reading data 
        datafile_engine, datafile_session = create_connection(True) # isDatafile boolean value
        logger.info("3) Connection to Cyclops-Datafile DB created Successful via pyodbc")
        
        # 4) Checking if cyclops tblinvoicesSentToDatafile is created -> creating in i2i DB if not
        invoices_sent_to_datafile_table_exists = database_table_exists(engine, 'tblInvoicesSentToDatafile')
        invoices_paid_transactions_from_datafile_table_exists = database_table_exists(engine, 'tblInvoicesPaidTransactionsFromDatafile')
        
        if not invoices_sent_to_datafile_table_exists:
            create_database_table(session, InvoicesSentToDatafile)
            logger.info("4) tblInvoicesSentToDatafile DB Table did NOT exist. table created successfully")
        else: 
            logger.info("4) tblInvoicesSentToDatafile DB Table DOES exist. No need to create new one. ")
            
        if not invoices_paid_transactions_from_datafile_table_exists:
            create_database_table(session, InvoicesPaidTransactionsFromDatafile)
            logger.info("4) tblInvoicesPaidTransactionsFromDatafilee DB Table did NOT exist. table created successfully")
        else: 
            logger.info("4) tblInvoicesPaidTransactionsFromDatafile DB Table DOES exist. No need to create new one. ")        
        
        # 5) Calling function to retrieve invoice masters (THIS WILL NEED TO BE CHANGED TO ONLY GET APPROVED invoices by Cyclops (Lee))
        is_new_file = True
        now = datetime.now()
        datetime_string = now.strftime("%d-%m-%Y-%H.%M.%S")
        filename = f"{datetime_string}i2i-cyclops-datafile-invoices"
        
        invoices = get_so_inv_masters_approved(session, SOInvMaster)
        logger.info("5) Retreived invoices from i2i ")
        
        if invoices is not None: 
            for invoice_master in invoices: 
                # 5.1) Checking if invoice is new 
                invoice_already_sent = has_invoice_already_been_sent_to_datafile(session, InvoicesSentToDatafile, invoice_master.InvMasNum)
                logger.info("5.1) Checked if invoice is new ")
                
                if not invoice_already_sent: 
                    # 5.2) Getting Customer details
                    customer = get_customer_by_so_num(session, invoice_master.InvMasSONum, SoMaster, CCCust)
                    if customer is not None: 
                        logger.info("5.2) Invoice is new -> retreived customer using SO Number ")
                        # 5.3) Gettting Invoice details
                        invoice_details = get_so_inv_details(session, SOInvDetail, invoice_master.InvMasNum)
                        if invoice_details is not None: 
                            logger.info("5.3) Retrieving invoice details from i2i")
                            # 5.4) Appending to CSV File
                            tax_code = get_tax_code(datafile_session, customer.CustAcctNo, CYCSLA)
                            append_to_csv(invoice_master, invoice_details, f"{filename}.csv", is_new_file, customer, tax_code, QIPM, session, logger)
                            is_new_file = False
                            logger.info("5.4) Calling function to append invoice details to CSV")
                        else: 
                            logger.info("5.3) Could not retrieve invoice details")
                    else: 
                        logger.info("5.2) Could not get customer details ")

            # 6) Converting CSV to excel to have both formats
            try: 
                csv_file = pd.read_csv(f"files/{filename}.csv")
                #excel_file = pd.ExcelWriter(f"files/{filename}.xlsx")
                #csv_file.to_excel(excel_file, index=False)
                #excel_file.save()
                #excel_file.close()
                logger.info("6) Converted CSV to excel to have both formats")
                
                # 7) Calling Function to post file to FTP server
                #post_file_to_ftp_server(filename, os.getenv('FTP_IP'), os.getenv('FTP_USERNAME'), os.getenv('FTP_PASSWORD'))
                #logger.info("7) Posted CSV file to FTP Server")
                        # 11) Clearing Database connection
                conn.close()
                logger.info("11) Successfully cleared database connections.")
                return csv_file, session, filename

            except FileNotFoundError:
                print("File not found, no data to sync.")
                conn.close()
                logger.info("11) Successfully cleared database connections.")
                return None, session, filename
            except pd.errors.ParserError:
                print("File is not a valid CSV format.")
                conn.close()
                logger.info("11) Successfully cleared database connections.")
                return None, session, filename
            except: 
                print("No file to post.")
                conn.close()
                logger.info("11) Successfully cleared database connections.")
                return None, session, filename
                
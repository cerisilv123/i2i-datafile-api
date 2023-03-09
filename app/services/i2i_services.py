import os
import logging
import sys
import uuid
import numpy as np
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import or_
from datetime import datetime

logger = logging.getLogger()

def generate_uuid():
    random_uuid = uuid.uuid4()
    return random_uuid

def database_table_exists(engine, name):
    try: 
        inspector = Inspector.from_engine(engine)
        result = inspector.has_table(name, schema='dbo')
        return result
    except SQLAlchemyError as e:
        logger.exception("SQLAlchemy Exception Occured: (function = database_table_exists): " + str(e))
        sys.exit(1)
    except Exception as e:
        logger.exception("General Exception Occurred (function = database_table_exists): " + str(e))
        sys.exit(1)
    
def create_database_table(session, db_model):
    try:
        db_model.__table__.create(session.bind) 
    except SQLAlchemyError as e: 
        logger.exception("SQLAlchemy Exception Occured: (function = create_database_table): ")
        sys.exit(1)
    except Exception as e:
        logger.exception("General Exception Occurred (function = create_database_table): " + str(e))
        sys.exit(1)
        
def get_so_inv_masters_approved(session, SOInvMaster):
    try:
        rows = session.query(SOInvMaster).filter(SOInvMaster.InvApprove == -1).all()
        if rows is None: 
            return None
        else:
            return rows
    except SQLAlchemyError as e: 
        logger.exception("SQLAlchemy Exception Occured: (function = get_so_inv_masters_approved): ")
        sys.exit(1)
    except Exception as e:
        logger.exception("General Exception Occurred (function = get_so_inv_masters_approved): " + str(e))
        sys.exit(1)
        
def get_so_inv_details(session, SOInvDetail, inv_mas_num):
    try:
        rows = session.query(SOInvDetail).filter(SOInvDetail.InvDetNum == inv_mas_num).all()
        if rows is None: 
            return None
        else: 
            return rows
    except SQLAlchemyError as e: 
        logger.exception("SQLAlchemy Exception Occured: (function = get_so_inv_details): ")
        sys.exit(1)
    except Exception as e:
        logger.exception("General Exception Occurred (function = get_so_inv_details): " + str(e))
        sys.exit(1)
        
def get_customer_by_so_num(session, so_num, SoMaster, CCCust):
    try:
        customer_id = session.query(SoMaster.SOCustID, SoMaster.SONum ).filter(SoMaster.SONum == so_num).first()
        if customer_id is None: 
            return None
        else: 
            id = customer_id[0]
            customer = session.query(CCCust.CustName, CCCust.CustAcctNo).filter(CCCust.ID == id).first()
            return customer
    except SQLAlchemyError as e: 
        logger.exception("SQLAlchemy Exception Occured: (function = get_customer_by_so_num): ")
        sys.exit(1)
    except Exception as e:
        logger.exception("General Exception Occurred (function = get_customer_by_so_num): " + str(e))
        sys.exit(1)
        
def has_invoice_already_been_sent_to_datafile(session, InvoicesSentToDatafile , inv_mas_num):
    try:
        rows = session.query(InvoicesSentToDatafile).filter(InvoicesSentToDatafile.InvMasNum == inv_mas_num).first()
        if rows is None: 
            return False
        else: 
            return True
    except SQLAlchemyError as e: 
        logger.exception("SQLAlchemy Exception Occured: (function = has_invoice_already_been_sent_to_datafile): ")
        sys.exit(1)
    except Exception as e:
        logger.exception("General Exception Occurred (function = has_invoice_already_been_sent_to_datafile): " + str(e))
        sys.exit(1)
        
def get_tax_code(datafile_session, cust_acct_no, CYCSLA):
    try:
        result = datafile_session.query(CYCSLA).filter(CYCSLA.ACCOUNT == cust_acct_no).first()
        if result is None: 
            return None
        else: 
            return result.TAX
     
    except SQLAlchemyError as e: 
        logger.exception("SQLAlchemy Exception Occured: (function = get_tax_code): ")
        sys.exit(1)
    except Exception as e:
        logger.exception("General Exception Occurred (function = get_tax_code): " + str(e))
        sys.exit(1)
        
def get_transaction_data_by_type(datafile_session, CYCSTL, type):
    try:
        after_date = '2023-02-20 00:00:00.000'
        
        results = datafile_session.query(CYCSTL).filter(CYCSTL.TYPE == type, CYCSTL.DATE > after_date).all()
        if results is None: 
            return None
        else: 
            return results
     
    except SQLAlchemyError as e: 
        logger.exception("SQLAlchemy Exception Occured: (function = get_transaction_data_by_type): ")
        sys.exit(1)
    except Exception as e:
        logger.exception("General Exception Occurred (function = get_transaction_data_by_type): " + str(e))
        sys.exit(1)
        
def insert_invoices_sent_to_datafile_row(session, InvoicesSentToDatafile, inv_number):
    try:
        row = InvoicesSentToDatafile(InvMasNum = inv_number)
        session.add(row)
        session.commit()
     
    except SQLAlchemyError as e: 
        logger.exception("SQLAlchemy Exception Occured: (function = insert_invoices_sent_to_datafile_row): ")
        sys.exit(1)
    except Exception as e:
        logger.exception("General Exception Occurred (function = insert_invoices_sent_to_datafile_row): " + str(e))
        sys.exit(1)
        
def insert_payment_transaction(cursor, conn, session, paid_transaction, so_master, Payments, invoice_num):
    try:
        # Getting new ID 
        cursor.execute('''
                INSERT INTO tblPayments (SONum, InvoiceNum, PayCustID, PaymentTypeID, PaymentAmount, PaymentRef, PaymentUserName, PaymentDateTime, PaymentExRate, PaymentTransID, QBAcctExportStatus)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', so_master.SONum, invoice_num, so_master.SOCustID, "D1", paid_transaction.ALLOCATED, paid_transaction.REF, "Datafile", paid_transaction.DATE_ALLOC, 1, "SI", 0)

        conn.commit()
     
    except Exception as e:
        logger.exception("General Exception Occurred (function = insert_invoices_sent_to_datafile_row): " + str(e))
        sys.exit(1)
        
def get_transaction_data_new_ids(session, InvoicesPaidTransactionsFromDatafile, paid_transactions_datafile, customers):
    try:
        paid_transactions_i2i = session.query(InvoicesPaidTransactionsFromDatafile).all()
        
        # Appending to new_transactions if transaction has not already been added to i2i
        new_transactions = []
        
        for x in paid_transactions_datafile:
            exists = False
            for y in paid_transactions_i2i:
                if x._ID == y._ID:
                    exists = True
                    break
                
            if not exists:
                new_transactions.append(x)
        
        # Keeping in new_transaction if customer of that transaction exists in i2i    
        new_transactions_final = []
         
        for x in new_transactions:
            for y in customers:
                if x.ACCOUNT == y.CustAcctNo:
                    new_transactions_final.append(x)
                       
        return new_transactions_final                 
                
    except Exception as e:
        logger.exception("General Exception Occurred (function = get_transaction_data_new_ids): " + str(e))
        sys.exit(1)
    
def insert_invoices_paid_transactions_from_datafile(session, InvoicesPaidTransactionsFromDatafile, id):
    try:
        row = InvoicesPaidTransactionsFromDatafile(_ID = id)
        session.add(row)
        session.commit()
     
    except SQLAlchemyError as e: 
        logger.exception("SQLAlchemy Exception Occured: (function = insert_invoices_sent_to_datafile_row): ")
        sys.exit(1)
    except Exception as e:
        logger.exception("General Exception Occurred (function = insert_invoices_sent_to_datafile_row): " + str(e))
        sys.exit(1)
    
    
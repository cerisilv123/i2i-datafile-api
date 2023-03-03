import sys 
import logging
import csv
import ftplib
import os
import socket

from collections import Counter

logger = logging.getLogger()

def append_to_csv(invoice_master, invoice_details, filename, is_new_file, customer, tax_code, QIPM, session):
    try: 
        # Creating CSV fields
        fields = ['Type', 'InvoiceDate', 'CustAccount', 'InvoiceNumber', 'CustomerName', 'CyclopsRef', 
                  'CustOrderNumber', 'Amount', 'VAT', 'NominalCode', 'CommodityCode', 'NetMass', 
                  'DeliveryTerms', 'SupplementaryUnits', 'SuppUnits', 'VAT Code', 'Czech-Goods', 'Czech-VAT', 'Belgium-Goods', 'Belgium-VAT']
        
        # Creating SO Inv Master
        invoice_master_row = ['1', str(invoice_master.InvMasDate), customer.CustAcctNo, invoice_master.InvMasNum, 
                              customer.CustName, '0000', str(invoice_master.InvMasSONum), str(invoice_master.InvMasterTot), 
                              str(invoice_master.InvMasterTax), '', '', '', '', '', '', '', '', '', '', '']
        
        invoice_detail_rows = []
        invoice_detail_rows.append(invoice_master_row)
        
        # Array to store tax codes that appear in SO In Detail
        invoice_master_tax_code_qtys = []
        
        # Calcualting for InvMaster based on InvDetails
        PMIntraStatKgTotal = 0 
        
        # Creating SO Inv Details
        for invoice_detail in invoice_details:
            row = ['9', str(invoice_master.InvMasDate), customer.CustAcctNo, invoice_master.InvMasNum, 
                   customer.CustName, '0000', str(invoice_master.InvMasSONum), 
                   str(invoice_detail.InvDetSOLineQty * invoice_detail.InvDetSOLineSalPrice), 
                   str(invoice_detail.InvDetSOLineTax), '0000', '0000', '', '', '', '', invoice_detail.InvDetSOLineTaxCode, '', '', '', '']
            
            # Getting inventory details
            qipm = session.query(QIPM).filter(QIPM.PMPartNo == invoice_detail.InvDetPartNo).first()
            if qipm is not None: 
                if qipm.PMDefCOGSNomLedg is None: 
                    row[9] = ''
                else: 
                    row[9] = str(qipm.PMDefCOGSNomLedg)
                    
                row[10] = str(qipm.PMIntraStatCode)
                row[11] = str(qipm.PMIntraStatKg * invoice_detail.InvDetSOLineQty)
                PMIntraStatKgTotal += float(qipm.PMIntraStatKg * invoice_detail.InvDetSOLineQty)
            
            # If Czech -> Czech
            if invoice_detail.InvDetSOLineTaxCode == 'K':
                row[16] = str(invoice_detail.InvDetCZKVal)
                row[17] = str(invoice_detail.InvDetCZKVAT)
            
            # If Belgium -> Belgium
            if invoice_detail.InvDetSOLineTaxCode == 'B':
                row[18] = str(invoice_detail.InvDetSOLineQty * invoice_detail.InvDetSOLineSalPrice)
                row[19] = str(invoice_detail.InvDetSOLineTax)
            
            invoice_detail_rows.append(row)
            
            # Calculating SO Inv Master tax code -> the tax code that shows up in the most SO Inv Details will be the tax code for SO Inv Master
            invoice_master_tax_code_qtys.append(invoice_detail.InvDetSOLineTaxCode)
            
        # Calculating tax code to use for invoice master
        tax_code_count = Counter(invoice_master_tax_code_qtys)
        most_common_tax_code = tax_code_count.most_common(1)[0][0]
        invoice_master_row[15] = most_common_tax_code
        
        # Updating PMIntraStatKgTotal for InvMaster
        invoice_master_row[11] = str(PMIntraStatKgTotal)
        
        try: 
            # If Czech -> Czech
            if most_common_tax_code == 'K':
                invoice_master_row[16] = str(sum(invoice_detail.InvDetCZKVal for invoice_detail in invoice_details)) # Summing czk value in invoice details
                invoice_master_row[17] = str(sum(invoice_detail.InvDetCZKVAT for invoice_detail in invoice_details)) # Summing czk value in invoice details
            
            # If Belgium -> Belgium
            if most_common_tax_code == 'B':
                invoice_master_row[18] = str(sum((invoice_detail.InvDetSOLineQty * invoice_detail.InvDetSOLineSalPrice) for invoice_detail in invoice_details)) # Summing czk value in invoice details
                invoice_master_row[19] = str(sum(invoice_detail.InvDetSOLineTax for invoice_detail in invoice_details)) # Summing czk value in invoice details
        except: 
            print("No values in column - nonetype")
            
        # Opening file and writing to file
        print(os.getcwd())
        
        with open(f"files/{filename}", 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            
            if is_new_file:
                csvwriter.writerow(fields)

            csvwriter.writerows(invoice_detail_rows)
              
    except Exception as e:
        logger.exception("General Exception Occurred (function = append_to_csv): " + str(e))
        sys.exit(1)
        
    
    
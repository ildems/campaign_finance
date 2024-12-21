MERGE INTO `demsilsp.boe_stream.expenditures` e
USING `demsilsp.boe_stream._expenditures` e_staging
ON e_staging.id = e.id
WHEN NOT MATCHED THEN
INSERT 
(id,committee_id,filed_doc_id,e_trans_id,last_only_name,first_name,expended_date,amount,aggregate_amount,address1,address2,city,state,zip,d2_part,purpose,candidate_name,office,supporting,opposing,archived,country,redaction_requested)
VALUES
(id,committee_id,filed_doc_id,e_trans_id,last_only_name,first_name,expended_date,amount,aggregate_amount,address1,address2,city,state,zip,d2_part,purpose,candidate_name,office,supporting,opposing,archived,country,redaction_requested);
MERGE INTO `demsilsp.boe_stream.receipts` r
USING `demsilsp.boe_stream._receipts` r_staging
ON r_staging.id = r.id
WHEN NOT MATCHED THEN
INSERT
(id,committee_id,filed_doc_id,e_trans_id,last_only_name,first_name,rcv_date,amount,aggregate_amount,loan_amount,occupation,employer,address1,address2,city,state,zip,d2_part,description,vendor_last_only_name,vendor_first_name,vendor_address1,vendor_address2,vendor_city,vendor_zip,archived,country,redaction_requested)
VALUES
(id,committee_id,filed_doc_id,e_trans_id,last_only_name,first_name,rcv_date,amount,aggregate_amount,loan_amount,occupation,employer,address1,address2,city,state,zip,d2_part,description,vendor_last_only_name,vendor_first_name,vendor_address1,vendor_address2,vendor_city,vendor_zip,archived,country,redaction_requested)

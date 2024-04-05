with
inst1 as 
(select loanAccountNumber from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)),
inst1def as 
(select loanAccountNumber from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 and isDelinquent = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)),
inst1currdef as 
(select loanAccountNumber from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 and isDelinquent = 1 and isCurrentDelinquent = 1
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)),
inst2plus as 
(select loanAccountNumber from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)),
inst2plusdef as 
(select loanAccountNumber from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)),
inst2pluscurrdef as 
(select loanAccountNumber from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and isCurrentDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)),
activeloans as 
(select loanAccountNumber from `risk_credit_mis.loan_master_table` where flagDisbursement = 1 and coalesce(loanPaidStatus, 'NA') not in ('Written Off',	
'Completed',	
'Settled')),
fraud as 
(select * from `dap_ds_poweruser_playground.fraudlist`),
i1 as 
(
select count(distinct loanAccountNumber) cntlid, 
count(distinct case when loanAccountNumber in (select loanAccountNumber from inst1def) then loanAccountNumber end) cntfirstinstallmentdefault 
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst1)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i2 as 
(select count(distinct loanAccountNumber) cntlid, count(distinct case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then loanAccountNumber end) cntsecondinstallmentdefault 
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst2plus)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i90total as 
(select count(distinct loanAccountNumber) cntlid, count(distinct case when coalesce(Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault 
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i902022total as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault 
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from `risk_credit_mis.loan_master_table` where format_date('%Y', disbursementDateTime) = '2022'and flagDisbursement = 1)
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i90fraudtotal as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusfrauddefault 
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
and cast(loanAccountNumber as integer) in (select ACCOUNTNUMBER from fraud)
)
select 
DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) Balance_Date,
-- (select count(loanAccountNumber) from activeloans) activeloans,
-- (select count(loanAccountNumber) from inst1) installmentone,
-- (select count(loanAccountNumber) from inst1def) installmentonedefault,
(select cntfirstinstallmentdefault from i1) firstpaymentdefaultcnt,
(select cntlid from i1) firstpaymentoverallobservation,
(select cntfirstinstallmentdefault from i1)/(select cntlid from i1) firstpaymentdefault,
(select cntsecondinstallmentdefault from i2) cntsecondinstallmentdefault,
(select cntlid from i2) secondplusoverallobservation,
(select cntsecondinstallmentdefault from i2)/(select cntlid from i2)secondpluspaymentdefault,
(select cnt90plusoveralldefault from i90total) cnt90plusoveralldefault,
(select cntlid from i90total) totalobservedportfolio,
(select cnt90plusoveralldefault from i90total) / (select cntlid from i90total) overall90plusdefaultrate,
(select cnt90plusfrauddefault from i90fraudtotal)cnt90plusfrauddefault,
(select cntlid from i90fraudtotal)totalobservationfraud90plusportfolio,
(select cnt90plusfrauddefault from i90fraudtotal)/(select cntlid from i90fraudtotal) fraud90plusdefaultrate,
(select cnt90plusoveralldefault from i902022total) total90plustdefault2022portfolio,
(select cntlid from i902022total) totalobservation2022portfolio,
(select cnt90plusoveralldefault from i902022total)/(select cntlid from i902022total) total90plus2022defaultrate
;


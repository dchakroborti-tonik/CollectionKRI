-- 06-12-2022 -- Insert query only for the latest month to be used every month

insert into `dap_ds_poweruser_playground.collectionkri` 
with
inst1 as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(
  select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid
from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1def as 
( select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
  (select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table`
 where installmentNumber = 1 and isDelinquent = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1currdef as
(select   loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` 
where installmentNumber = 1 and isDelinquent = 1 and isCurrentDelinquent = 1
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plus as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid  from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plusdef as 
(select  loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber
)
),
inst2pluscurrdef as 
(select loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and isCurrentDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber)
),
activeloans as 
(select loanAccountNumber from `risk_credit_mis.loan_master_table` where flagDisbursement = 1 and coalesce(loanPaidStatus, 'NA') not in ('Written Off',	
'Completed',	
'Settled')),
fraud as 
(select * from `dap_ds_poweruser_playground.fraudlist`),
i1 as 
(
select count(distinct loanAccountNumber) cntlid, 
count(distinct case when loanAccountNumber in (select loanAccountNumber from inst1def) then loanAccountNumber end) cntfirstinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst1def) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst1)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i2 as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then loanAccountNumber end) cntsecondinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst2plus)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i90total as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when coalesce(Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when coalesce(Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i902022total as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from `risk_credit_mis.loan_master_table` where format_date('%Y', disbursementDateTime) = '2022'and flagDisbursement = 1)
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i90fraudtotal as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusfrauddefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
and cast(loanAccountNumber as integer) not in (select loanAccountNumber from fraud)
)
select 
DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) Balance_Date,
-- (select count(loanAccountNumber) from activeloans) activeloans,
-- (select count(loanAccountNumber) from inst1) installmentone,
-- (select count(loanAccountNumber) from inst1def) installmentonedefault,
(select cntfirstinstallmentdefault from i1) firstpaymentdefaultcnt,
(select cntlid from i1) firstpaymentoverallobservation,
(select cntfirstinstallmentdefault from i1)/(select cntlid from i1) firstpaymentdefault,
(select Total_Outstanding_Amount from i1) firstpaymentTotal_Outstanding_Amount,
(select totalamount from i1) firstpaymenttotalamount,
(select Total_Outstanding_Amount from i1)/(select totalamount from i1) firstpaymentdefaultv,

(select cntsecondinstallmentdefault from i2) cntsecondinstallmentdefault,
(select cntlid from i2) secondplusoverallobservation,
(select cntsecondinstallmentdefault from i2)/(select cntlid from i2)secondpluspaymentdefault,

(select Total_Outstanding_Amount from i2) cntsecondTotal_Outstanding_Amount,
(select totalamount from i2) secondplustotalamount,
(select Total_Outstanding_Amount from i2)/(select totalamount from i2)secondpluspaymentdefaultv,

(select cnt90plusoveralldefault from i90total) cnt90plusoveralldefault,
(select cntlid from i90total) totalobservedportfolio,
(select cnt90plusoveralldefault from i90total) / (select cntlid from i90total) overall90plusdefaultrate,

(select Total_Outstanding_Amount from i90total) cnt90plusoverallTotal_Outstanding_Amount,
(select totalamount from i90total) totalobservedportfoliototalamount,
(select Total_Outstanding_Amount from i90total) / (select totalamount from i90total) overall90plusdefaultratev,

(select cnt90plusfrauddefault from i90fraudtotal)cnt90plusfrauddefault,
(select cntlid from i90fraudtotal)totalobservationfraud90plusportfolio,
(select cnt90plusfrauddefault from i90fraudtotal)/(select cntlid from i90fraudtotal) fraud90plusdefaultrate,

(select Total_Outstanding_Amount from i90fraudtotal)cnt90plusfrauddefaultTotal_Outstanding_Amount,
(select totalamount from i90fraudtotal)totalobservationfraud90plusportfoliototalamount,
(select Total_Outstanding_Amount from i90fraudtotal)/(select totalamount from i90fraudtotal) fraud90plusdefaultratev,

(select cnt90plusoveralldefault from i902022total) total90plustdefault2022portfolio,
(select cntlid from i902022total) totalobservation2022portfolio,
(select cnt90plusoveralldefault from i902022total)/(select cntlid from i902022total) total90plus2022defaultrate,

(select Total_Outstanding_Amount from i902022total) total90plustdefault2022portfolioTotal_Outstanding_Amount,
(select totalamount from i902022total) totalobservation2022portfoliototalamount,
(select Total_Outstanding_Amount from i902022total)/(select totalamount from i902022total) total90plus2022defaultratev
;





-- 29-11-2022 -- 16:17

drop table if exists `dap_ds_poweruser_playground.collectionkri`;

create table `dap_ds_poweruser_playground.collectionkri` as 
with
inst1 as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(
  select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid
from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1def as 
( select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
  (select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table`
 where installmentNumber = 1 and isDelinquent = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1currdef as
(select   loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` 
where installmentNumber = 1 and isDelinquent = 1 and isCurrentDelinquent = 1
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plus as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid  from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plusdef as 
(select  loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber
)
),
inst2pluscurrdef as 
(select loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and isCurrentDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber)
),
activeloans as 
(select loanAccountNumber from `risk_credit_mis.loan_master_table` where flagDisbursement = 1 and coalesce(loanPaidStatus, 'NA') not in ('Written Off',	
'Completed',	
'Settled')),
fraud as 
(select * from `dap_ds_poweruser_playground.fraudlist`),
i1 as 
(
select count(distinct loanAccountNumber) cntlid, 
count(distinct case when loanAccountNumber in (select loanAccountNumber from inst1def) then loanAccountNumber end) cntfirstinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst1def) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst1)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i2 as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then loanAccountNumber end) cntsecondinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst2plus)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i90total as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when coalesce(Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when coalesce(Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i902022total as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from `risk_credit_mis.loan_master_table` where format_date('%Y', disbursementDateTime) = '2022'and flagDisbursement = 1)
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i90fraudtotal as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusfrauddefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
and cast(loanAccountNumber as integer) not in (select ACCOUNTNUMBER from fraud)
)
select 
DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) Balance_Date,
-- (select count(loanAccountNumber) from activeloans) activeloans,
-- (select count(loanAccountNumber) from inst1) installmentone,
-- (select count(loanAccountNumber) from inst1def) installmentonedefault,
(select cntfirstinstallmentdefault from i1) firstpaymentdefaultcnt,
(select cntlid from i1) firstpaymentoverallobservation,
(select cntfirstinstallmentdefault from i1)/(select cntlid from i1) firstpaymentdefault,
(select Total_Outstanding_Amount from i1) firstpaymentTotal_Outstanding_Amount,
(select totalamount from i1) firstpaymenttotalamount,
(select Total_Outstanding_Amount from i1)/(select totalamount from i1) firstpaymentdefaultv,

(select cntsecondinstallmentdefault from i2) cntsecondinstallmentdefault,
(select cntlid from i2) secondplusoverallobservation,
(select cntsecondinstallmentdefault from i2)/(select cntlid from i2)secondpluspaymentdefault,

(select Total_Outstanding_Amount from i2) cntsecondTotal_Outstanding_Amount,
(select totalamount from i2) secondplustotalamount,
(select Total_Outstanding_Amount from i2)/(select totalamount from i2)secondpluspaymentdefaultv,

(select cnt90plusoveralldefault from i90total) cnt90plusoveralldefault,
(select cntlid from i90total) totalobservedportfolio,
(select cnt90plusoveralldefault from i90total) / (select cntlid from i90total) overall90plusdefaultrate,

(select Total_Outstanding_Amount from i90total) cnt90plusoverallTotal_Outstanding_Amount,
(select totalamount from i90total) totalobservedportfoliototalamount,
(select Total_Outstanding_Amount from i90total) / (select totalamount from i90total) overall90plusdefaultratev,

(select cnt90plusfrauddefault from i90fraudtotal)cnt90plusfrauddefault,
(select cntlid from i90fraudtotal)totalobservationfraud90plusportfolio,
(select cnt90plusfrauddefault from i90fraudtotal)/(select cntlid from i90fraudtotal) fraud90plusdefaultrate,

(select Total_Outstanding_Amount from i90fraudtotal)cnt90plusfrauddefaultTotal_Outstanding_Amount,
(select totalamount from i90fraudtotal)totalobservationfraud90plusportfoliototalamount,
(select Total_Outstanding_Amount from i90fraudtotal)/(select totalamount from i90fraudtotal) fraud90plusdefaultratev,

(select cnt90plusoveralldefault from i902022total) total90plustdefault2022portfolio,
(select cntlid from i902022total) totalobservation2022portfolio,
(select cnt90plusoveralldefault from i902022total)/(select cntlid from i902022total) total90plus2022defaultrate,

(select Total_Outstanding_Amount from i902022total) total90plustdefault2022portfolioTotal_Outstanding_Amount,
(select totalamount from i902022total) totalobservation2022portfoliototalamount,
(select Total_Outstanding_Amount from i902022total)/(select totalamount from i902022total) total90plus2022defaultratev
;


-- select DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY);

insert into `dap_ds_poweruser_playground.collectionkri` 
with
inst1 as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(
  select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid
from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1def as 
( select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
  (select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table`
 where installmentNumber = 1 and isDelinquent = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1currdef as
(select   loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` 
where installmentNumber = 1 and isDelinquent = 1 and isCurrentDelinquent = 1
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plus as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid  from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plusdef as 
(select  loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber
)
),
inst2pluscurrdef as 
(select loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and isCurrentDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber)
),
activeloans as 
(select loanAccountNumber from `risk_credit_mis.loan_master_table` where flagDisbursement = 1 and coalesce(loanPaidStatus, 'NA') not in ('Written Off',	
'Completed',	
'Settled')),
fraud as 
(select * from `dap_ds_poweruser_playground.fraudlist`),
i1 as 
(
select count(distinct loanAccountNumber) cntlid, 
count(distinct case when loanAccountNumber in (select loanAccountNumber from inst1def) then loanAccountNumber end) cntfirstinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst1def) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst1)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i2 as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then loanAccountNumber end) cntsecondinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst2plus)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i90total as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when coalesce(Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when coalesce(Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i902022total as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from `risk_credit_mis.loan_master_table` where format_date('%Y', disbursementDateTime) = '2022'and flagDisbursement = 1)
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i90fraudtotal as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusfrauddefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
and cast(loanAccountNumber as integer) not in (select ACCOUNTNUMBER from fraud)
)
select 
DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) Balance_Date,
-- (select count(loanAccountNumber) from activeloans) activeloans,
-- (select count(loanAccountNumber) from inst1) installmentone,
-- (select count(loanAccountNumber) from inst1def) installmentonedefault,
(select cntfirstinstallmentdefault from i1) firstpaymentdefaultcnt,
(select cntlid from i1) firstpaymentoverallobservation,
(select cntfirstinstallmentdefault from i1)/(select cntlid from i1) firstpaymentdefault,
(select Total_Outstanding_Amount from i1) firstpaymentTotal_Outstanding_Amount,
(select totalamount from i1) firstpaymenttotalamount,
(select Total_Outstanding_Amount from i1)/(select totalamount from i1) firstpaymentdefaultv,

(select cntsecondinstallmentdefault from i2) cntsecondinstallmentdefault,
(select cntlid from i2) secondplusoverallobservation,
(select cntsecondinstallmentdefault from i2)/(select cntlid from i2)secondpluspaymentdefault,

(select Total_Outstanding_Amount from i2) cntsecondTotal_Outstanding_Amount,
(select totalamount from i2) secondplustotalamount,
(select Total_Outstanding_Amount from i2)/(select totalamount from i2)secondpluspaymentdefaultv,

(select cnt90plusoveralldefault from i90total) cnt90plusoveralldefault,
(select cntlid from i90total) totalobservedportfolio,
(select cnt90plusoveralldefault from i90total) / (select cntlid from i90total) overall90plusdefaultrate,

(select Total_Outstanding_Amount from i90total) cnt90plusoverallTotal_Outstanding_Amount,
(select totalamount from i90total) totalobservedportfoliototalamount,
(select Total_Outstanding_Amount from i90total) / (select totalamount from i90total) overall90plusdefaultratev,

(select cnt90plusfrauddefault from i90fraudtotal)cnt90plusfrauddefault,
(select cntlid from i90fraudtotal)totalobservationfraud90plusportfolio,
(select cnt90plusfrauddefault from i90fraudtotal)/(select cntlid from i90fraudtotal) fraud90plusdefaultrate,

(select Total_Outstanding_Amount from i90fraudtotal)cnt90plusfrauddefaultTotal_Outstanding_Amount,
(select totalamount from i90fraudtotal)totalobservationfraud90plusportfoliototalamount,
(select Total_Outstanding_Amount from i90fraudtotal)/(select totalamount from i90fraudtotal) fraud90plusdefaultratev,

(select cnt90plusoveralldefault from i902022total) total90plustdefault2022portfolio,
(select cntlid from i902022total) totalobservation2022portfolio,
(select cnt90plusoveralldefault from i902022total)/(select cntlid from i902022total) total90plus2022defaultrate,

(select Total_Outstanding_Amount from i902022total) total90plustdefault2022portfolioTotal_Outstanding_Amount,
(select totalamount from i902022total) totalobservation2022portfoliototalamount,
(select Total_Outstanding_Amount from i902022total)/(select totalamount from i902022total) total90plus2022defaultratev
;


-- select DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY);

insert into `dap_ds_poweruser_playground.collectionkri` 
with
inst1 as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(
  select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid
from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1def as 
( select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
  (select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table`
 where installmentNumber = 1 and isDelinquent = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1currdef as
(select   loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` 
where installmentNumber = 1 and isDelinquent = 1 and isCurrentDelinquent = 1
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plus as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid  from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plusdef as 
(select  loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber
)
),
inst2pluscurrdef as 
(select loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and isCurrentDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber)
),
activeloans as 
(select loanAccountNumber from `risk_credit_mis.loan_master_table` where flagDisbursement = 1 and coalesce(loanPaidStatus, 'NA') not in ('Written Off',	
'Completed',	
'Settled')),
fraud as 
(select * from `dap_ds_poweruser_playground.fraudlist`),
i1 as 
(
select count(distinct loanAccountNumber) cntlid, 
count(distinct case when loanAccountNumber in (select loanAccountNumber from inst1def) then loanAccountNumber end) cntfirstinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst1def) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst1)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i2 as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then loanAccountNumber end) cntsecondinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst2plus)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i90total as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when coalesce(Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when coalesce(Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i902022total as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from `risk_credit_mis.loan_master_table` where format_date('%Y', disbursementDateTime) = '2022'and flagDisbursement = 1)
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i90fraudtotal as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusfrauddefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
and cast(loanAccountNumber as integer) not in (select ACCOUNTNUMBER from fraud)
)
select 
DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) Balance_Date,
-- (select count(loanAccountNumber) from activeloans) activeloans,
-- (select count(loanAccountNumber) from inst1) installmentone,
-- (select count(loanAccountNumber) from inst1def) installmentonedefault,
(select cntfirstinstallmentdefault from i1) firstpaymentdefaultcnt,
(select cntlid from i1) firstpaymentoverallobservation,
(select cntfirstinstallmentdefault from i1)/(select cntlid from i1) firstpaymentdefault,
(select Total_Outstanding_Amount from i1) firstpaymentTotal_Outstanding_Amount,
(select totalamount from i1) firstpaymenttotalamount,
(select Total_Outstanding_Amount from i1)/(select totalamount from i1) firstpaymentdefaultv,

(select cntsecondinstallmentdefault from i2) cntsecondinstallmentdefault,
(select cntlid from i2) secondplusoverallobservation,
(select cntsecondinstallmentdefault from i2)/(select cntlid from i2)secondpluspaymentdefault,

(select Total_Outstanding_Amount from i2) cntsecondTotal_Outstanding_Amount,
(select totalamount from i2) secondplustotalamount,
(select Total_Outstanding_Amount from i2)/(select totalamount from i2)secondpluspaymentdefaultv,

(select cnt90plusoveralldefault from i90total) cnt90plusoveralldefault,
(select cntlid from i90total) totalobservedportfolio,
(select cnt90plusoveralldefault from i90total) / (select cntlid from i90total) overall90plusdefaultrate,

(select Total_Outstanding_Amount from i90total) cnt90plusoverallTotal_Outstanding_Amount,
(select totalamount from i90total) totalobservedportfoliototalamount,
(select Total_Outstanding_Amount from i90total) / (select totalamount from i90total) overall90plusdefaultratev,

(select cnt90plusfrauddefault from i90fraudtotal)cnt90plusfrauddefault,
(select cntlid from i90fraudtotal)totalobservationfraud90plusportfolio,
(select cnt90plusfrauddefault from i90fraudtotal)/(select cntlid from i90fraudtotal) fraud90plusdefaultrate,

(select Total_Outstanding_Amount from i90fraudtotal)cnt90plusfrauddefaultTotal_Outstanding_Amount,
(select totalamount from i90fraudtotal)totalobservationfraud90plusportfoliototalamount,
(select Total_Outstanding_Amount from i90fraudtotal)/(select totalamount from i90fraudtotal) fraud90plusdefaultratev,

(select cnt90plusoveralldefault from i902022total) total90plustdefault2022portfolio,
(select cntlid from i902022total) totalobservation2022portfolio,
(select cnt90plusoveralldefault from i902022total)/(select cntlid from i902022total) total90plus2022defaultrate,

(select Total_Outstanding_Amount from i902022total) total90plustdefault2022portfolioTotal_Outstanding_Amount,
(select totalamount from i902022total) totalobservation2022portfoliototalamount,
(select Total_Outstanding_Amount from i902022total)/(select totalamount from i902022total) total90plus2022defaultratev
;


-- select DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY);

insert into `dap_ds_poweruser_playground.collectionkri` 
with
inst1 as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(
  select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid
from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1def as 
( select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
  (select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table`
 where installmentNumber = 1 and isDelinquent = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1currdef as
(select   loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` 
where installmentNumber = 1 and isDelinquent = 1 and isCurrentDelinquent = 1
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plus as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid  from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plusdef as 
(select  loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber
)
),
inst2pluscurrdef as 
(select loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and isCurrentDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber)
),
activeloans as 
(select loanAccountNumber from `risk_credit_mis.loan_master_table` where flagDisbursement = 1 and coalesce(loanPaidStatus, 'NA') not in ('Written Off',	
'Completed',	
'Settled')),
fraud as 
(select * from `dap_ds_poweruser_playground.fraudlist`),
i1 as 
(
select count(distinct loanAccountNumber) cntlid, 
count(distinct case when loanAccountNumber in (select loanAccountNumber from inst1def) then loanAccountNumber end) cntfirstinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst1def) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst1)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i2 as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then loanAccountNumber end) cntsecondinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst2plus)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i90total as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when coalesce(Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when coalesce(Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i902022total as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from `risk_credit_mis.loan_master_table` where format_date('%Y', disbursementDateTime) = '2022'and flagDisbursement = 1)
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i90fraudtotal as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusfrauddefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
and cast(loanAccountNumber as integer) not in (select ACCOUNTNUMBER from fraud)
)
select 
DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) Balance_Date,
-- (select count(loanAccountNumber) from activeloans) activeloans,
-- (select count(loanAccountNumber) from inst1) installmentone,
-- (select count(loanAccountNumber) from inst1def) installmentonedefault,
(select cntfirstinstallmentdefault from i1) firstpaymentdefaultcnt,
(select cntlid from i1) firstpaymentoverallobservation,
(select cntfirstinstallmentdefault from i1)/(select cntlid from i1) firstpaymentdefault,
(select Total_Outstanding_Amount from i1) firstpaymentTotal_Outstanding_Amount,
(select totalamount from i1) firstpaymenttotalamount,
(select Total_Outstanding_Amount from i1)/(select totalamount from i1) firstpaymentdefaultv,

(select cntsecondinstallmentdefault from i2) cntsecondinstallmentdefault,
(select cntlid from i2) secondplusoverallobservation,
(select cntsecondinstallmentdefault from i2)/(select cntlid from i2)secondpluspaymentdefault,

(select Total_Outstanding_Amount from i2) cntsecondTotal_Outstanding_Amount,
(select totalamount from i2) secondplustotalamount,
(select Total_Outstanding_Amount from i2)/(select totalamount from i2)secondpluspaymentdefaultv,

(select cnt90plusoveralldefault from i90total) cnt90plusoveralldefault,
(select cntlid from i90total) totalobservedportfolio,
(select cnt90plusoveralldefault from i90total) / (select cntlid from i90total) overall90plusdefaultrate,

(select Total_Outstanding_Amount from i90total) cnt90plusoverallTotal_Outstanding_Amount,
(select totalamount from i90total) totalobservedportfoliototalamount,
(select Total_Outstanding_Amount from i90total) / (select totalamount from i90total) overall90plusdefaultratev,

(select cnt90plusfrauddefault from i90fraudtotal)cnt90plusfrauddefault,
(select cntlid from i90fraudtotal)totalobservationfraud90plusportfolio,
(select cnt90plusfrauddefault from i90fraudtotal)/(select cntlid from i90fraudtotal) fraud90plusdefaultrate,

(select Total_Outstanding_Amount from i90fraudtotal)cnt90plusfrauddefaultTotal_Outstanding_Amount,
(select totalamount from i90fraudtotal)totalobservationfraud90plusportfoliototalamount,
(select Total_Outstanding_Amount from i90fraudtotal)/(select totalamount from i90fraudtotal) fraud90plusdefaultratev,

(select cnt90plusoveralldefault from i902022total) total90plustdefault2022portfolio,
(select cntlid from i902022total) totalobservation2022portfolio,
(select cnt90plusoveralldefault from i902022total)/(select cntlid from i902022total) total90plus2022defaultrate,

(select Total_Outstanding_Amount from i902022total) total90plustdefault2022portfolioTotal_Outstanding_Amount,
(select totalamount from i902022total) totalobservation2022portfoliototalamount,
(select Total_Outstanding_Amount from i902022total)/(select totalamount from i902022total) total90plus2022defaultratev
;





-- 28-11-2022

drop table if exists `dap_ds_poweruser_playground.collectionkri`;

create table `dap_ds_poweruser_playground.collectionkri` as 
with
inst1 as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(
  select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid
from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1def as 
( select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
  (select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table`
 where installmentNumber = 1 and isDelinquent = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1currdef as
(select   loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` 
where installmentNumber = 1 and isDelinquent = 1 and isCurrentDelinquent = 1
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plus as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid  from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plusdef as 
(select  loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber
)
),
inst2pluscurrdef as 
(select loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and isCurrentDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber)
),
activeloans as 
(select loanAccountNumber from `risk_credit_mis.loan_master_table` where flagDisbursement = 1 and coalesce(loanPaidStatus, 'NA') not in ('Written Off',	
'Completed',	
'Settled')),
fraud as 
(select * from `dap_ds_poweruser_playground.fraudlist`),
i1 as 
(
select count(distinct loanAccountNumber) cntlid, 
count(distinct case when loanAccountNumber in (select loanAccountNumber from inst1def) then loanAccountNumber end) cntfirstinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst1def) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst1)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i2 as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then loanAccountNumber end) cntsecondinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst2plus)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i90total as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when coalesce(Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when coalesce(Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i902022total as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from `risk_credit_mis.loan_master_table` where format_date('%Y', disbursementDateTime) = '2022'and flagDisbursement = 1)
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i90fraudtotal as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusfrauddefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
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
(select Total_Outstanding_Amount from i1) firstpaymentTotal_Outstanding_Amount,
(select totalamount from i1) firstpaymenttotalamount,
(select Total_Outstanding_Amount from i1)/(select totalamount from i1) firstpaymentdefaultv,

(select cntsecondinstallmentdefault from i2) cntsecondinstallmentdefault,
(select cntlid from i2) secondplusoverallobservation,
(select cntsecondinstallmentdefault from i2)/(select cntlid from i2)secondpluspaymentdefault,

(select Total_Outstanding_Amount from i2) cntsecondTotal_Outstanding_Amount,
(select totalamount from i2) secondplustotalamount,
(select Total_Outstanding_Amount from i2)/(select totalamount from i2)secondpluspaymentdefaultv,

(select cnt90plusoveralldefault from i90total) cnt90plusoveralldefault,
(select cntlid from i90total) totalobservedportfolio,
(select cnt90plusoveralldefault from i90total) / (select cntlid from i90total) overall90plusdefaultrate,

(select Total_Outstanding_Amount from i90total) cnt90plusoverallTotal_Outstanding_Amount,
(select totalamount from i90total) totalobservedportfoliototalamount,
(select Total_Outstanding_Amount from i90total) / (select totalamount from i90total) overall90plusdefaultratev,

(select cnt90plusfrauddefault from i90fraudtotal)cnt90plusfrauddefault,
(select cntlid from i90fraudtotal)totalobservationfraud90plusportfolio,
(select cnt90plusfrauddefault from i90fraudtotal)/(select cntlid from i90fraudtotal) fraud90plusdefaultrate,

(select Total_Outstanding_Amount from i90fraudtotal)cnt90plusfrauddefaultTotal_Outstanding_Amount,
(select totalamount from i90fraudtotal)totalobservationfraud90plusportfoliototalamount,
(select Total_Outstanding_Amount from i90fraudtotal)/(select totalamount from i90fraudtotal) fraud90plusdefaultratev,

(select cnt90plusoveralldefault from i902022total) total90plustdefault2022portfolio,
(select cntlid from i902022total) totalobservation2022portfolio,
(select cnt90plusoveralldefault from i902022total)/(select cntlid from i902022total) total90plus2022defaultrate,

(select Total_Outstanding_Amount from i902022total) total90plustdefault2022portfolioTotal_Outstanding_Amount,
(select totalamount from i902022total) totalobservation2022portfoliototalamount,
(select Total_Outstanding_Amount from i902022total)/(select totalamount from i902022total) total90plus2022defaultratev
;


-- select DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY);

insert into `dap_ds_poweruser_playground.collectionkri` 
with
inst1 as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(
  select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid
from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1def as 
( select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
  (select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table`
 where installmentNumber = 1 and isDelinquent = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1currdef as
(select   loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` 
where installmentNumber = 1 and isDelinquent = 1 and isCurrentDelinquent = 1
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plus as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid  from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plusdef as 
(select  loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber
)
),
inst2pluscurrdef as 
(select loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and isCurrentDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber)
),
activeloans as 
(select loanAccountNumber from `risk_credit_mis.loan_master_table` where flagDisbursement = 1 and coalesce(loanPaidStatus, 'NA') not in ('Written Off',	
'Completed',	
'Settled')),
fraud as 
(select * from `dap_ds_poweruser_playground.fraudlist`),
i1 as 
(
select count(distinct loanAccountNumber) cntlid, 
count(distinct case when loanAccountNumber in (select loanAccountNumber from inst1def) then loanAccountNumber end) cntfirstinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst1def) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst1)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i2 as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then loanAccountNumber end) cntsecondinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst2plus)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i90total as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when coalesce(Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when coalesce(Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i902022total as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from `risk_credit_mis.loan_master_table` where format_date('%Y', disbursementDateTime) = '2022'and flagDisbursement = 1)
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i90fraudtotal as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusfrauddefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
and cast(loanAccountNumber as integer) in (select ACCOUNTNUMBER from fraud)
)
select 
DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 1 month), MONTH), INTERVAL 1 DAY) Balance_Date,
-- (select count(loanAccountNumber) from activeloans) activeloans,
-- (select count(loanAccountNumber) from inst1) installmentone,
-- (select count(loanAccountNumber) from inst1def) installmentonedefault,
(select cntfirstinstallmentdefault from i1) firstpaymentdefaultcnt,
(select cntlid from i1) firstpaymentoverallobservation,
(select cntfirstinstallmentdefault from i1)/(select cntlid from i1) firstpaymentdefault,
(select Total_Outstanding_Amount from i1) firstpaymentTotal_Outstanding_Amount,
(select totalamount from i1) firstpaymenttotalamount,
(select Total_Outstanding_Amount from i1)/(select totalamount from i1) firstpaymentdefaultv,

(select cntsecondinstallmentdefault from i2) cntsecondinstallmentdefault,
(select cntlid from i2) secondplusoverallobservation,
(select cntsecondinstallmentdefault from i2)/(select cntlid from i2)secondpluspaymentdefault,

(select Total_Outstanding_Amount from i2) cntsecondTotal_Outstanding_Amount,
(select totalamount from i2) secondplustotalamount,
(select Total_Outstanding_Amount from i2)/(select totalamount from i2)secondpluspaymentdefaultv,

(select cnt90plusoveralldefault from i90total) cnt90plusoveralldefault,
(select cntlid from i90total) totalobservedportfolio,
(select cnt90plusoveralldefault from i90total) / (select cntlid from i90total) overall90plusdefaultrate,

(select Total_Outstanding_Amount from i90total) cnt90plusoverallTotal_Outstanding_Amount,
(select totalamount from i90total) totalobservedportfoliototalamount,
(select Total_Outstanding_Amount from i90total) / (select totalamount from i90total) overall90plusdefaultratev,

(select cnt90plusfrauddefault from i90fraudtotal)cnt90plusfrauddefault,
(select cntlid from i90fraudtotal)totalobservationfraud90plusportfolio,
(select cnt90plusfrauddefault from i90fraudtotal)/(select cntlid from i90fraudtotal) fraud90plusdefaultrate,

(select Total_Outstanding_Amount from i90fraudtotal)cnt90plusfrauddefaultTotal_Outstanding_Amount,
(select totalamount from i90fraudtotal)totalobservationfraud90plusportfoliototalamount,
(select Total_Outstanding_Amount from i90fraudtotal)/(select totalamount from i90fraudtotal) fraud90plusdefaultratev,

(select cnt90plusoveralldefault from i902022total) total90plustdefault2022portfolio,
(select cntlid from i902022total) totalobservation2022portfolio,
(select cnt90plusoveralldefault from i902022total)/(select cntlid from i902022total) total90plus2022defaultrate,

(select Total_Outstanding_Amount from i902022total) total90plustdefault2022portfolioTotal_Outstanding_Amount,
(select totalamount from i902022total) totalobservation2022portfoliototalamount,
(select Total_Outstanding_Amount from i902022total)/(select totalamount from i902022total) total90plus2022defaultratev
;


-- select DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY);

insert into `dap_ds_poweruser_playground.collectionkri` 
with
inst1 as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(
  select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid
from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1def as 
( select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
  (select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table`
 where installmentNumber = 1 and isDelinquent = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1currdef as
(select   loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` 
where installmentNumber = 1 and isDelinquent = 1 and isCurrentDelinquent = 1
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plus as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid  from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plusdef as 
(select  loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber
)
),
inst2pluscurrdef as 
(select loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and isCurrentDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber)
),
activeloans as 
(select loanAccountNumber from `risk_credit_mis.loan_master_table` where flagDisbursement = 1 and coalesce(loanPaidStatus, 'NA') not in ('Written Off',	
'Completed',	
'Settled')),
fraud as 
(select * from `dap_ds_poweruser_playground.fraudlist`),
i1 as 
(
select count(distinct loanAccountNumber) cntlid, 
count(distinct case when loanAccountNumber in (select loanAccountNumber from inst1def) then loanAccountNumber end) cntfirstinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst1def) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst1)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i2 as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then loanAccountNumber end) cntsecondinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst2plus)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i90total as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when coalesce(Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when coalesce(Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i902022total as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from `risk_credit_mis.loan_master_table` where format_date('%Y', disbursementDateTime) = '2022'and flagDisbursement = 1)
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i90fraudtotal as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusfrauddefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
and cast(loanAccountNumber as integer) in (select ACCOUNTNUMBER from fraud)
)
select 
DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 2 month), MONTH), INTERVAL 1 DAY) Balance_Date,
-- (select count(loanAccountNumber) from activeloans) activeloans,
-- (select count(loanAccountNumber) from inst1) installmentone,
-- (select count(loanAccountNumber) from inst1def) installmentonedefault,
(select cntfirstinstallmentdefault from i1) firstpaymentdefaultcnt,
(select cntlid from i1) firstpaymentoverallobservation,
(select cntfirstinstallmentdefault from i1)/(select cntlid from i1) firstpaymentdefault,
(select Total_Outstanding_Amount from i1) firstpaymentTotal_Outstanding_Amount,
(select totalamount from i1) firstpaymenttotalamount,
(select Total_Outstanding_Amount from i1)/(select totalamount from i1) firstpaymentdefaultv,

(select cntsecondinstallmentdefault from i2) cntsecondinstallmentdefault,
(select cntlid from i2) secondplusoverallobservation,
(select cntsecondinstallmentdefault from i2)/(select cntlid from i2)secondpluspaymentdefault,

(select Total_Outstanding_Amount from i2) cntsecondTotal_Outstanding_Amount,
(select totalamount from i2) secondplustotalamount,
(select Total_Outstanding_Amount from i2)/(select totalamount from i2)secondpluspaymentdefaultv,

(select cnt90plusoveralldefault from i90total) cnt90plusoveralldefault,
(select cntlid from i90total) totalobservedportfolio,
(select cnt90plusoveralldefault from i90total) / (select cntlid from i90total) overall90plusdefaultrate,

(select Total_Outstanding_Amount from i90total) cnt90plusoverallTotal_Outstanding_Amount,
(select totalamount from i90total) totalobservedportfoliototalamount,
(select Total_Outstanding_Amount from i90total) / (select totalamount from i90total) overall90plusdefaultratev,

(select cnt90plusfrauddefault from i90fraudtotal)cnt90plusfrauddefault,
(select cntlid from i90fraudtotal)totalobservationfraud90plusportfolio,
(select cnt90plusfrauddefault from i90fraudtotal)/(select cntlid from i90fraudtotal) fraud90plusdefaultrate,

(select Total_Outstanding_Amount from i90fraudtotal)cnt90plusfrauddefaultTotal_Outstanding_Amount,
(select totalamount from i90fraudtotal)totalobservationfraud90plusportfoliototalamount,
(select Total_Outstanding_Amount from i90fraudtotal)/(select totalamount from i90fraudtotal) fraud90plusdefaultratev,

(select cnt90plusoveralldefault from i902022total) total90plustdefault2022portfolio,
(select cntlid from i902022total) totalobservation2022portfolio,
(select cnt90plusoveralldefault from i902022total)/(select cntlid from i902022total) total90plus2022defaultrate,

(select Total_Outstanding_Amount from i902022total) total90plustdefault2022portfolioTotal_Outstanding_Amount,
(select totalamount from i902022total) totalobservation2022portfoliototalamount,
(select Total_Outstanding_Amount from i902022total)/(select totalamount from i902022total) total90plus2022defaultratev
;


-- select DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY);

insert into `dap_ds_poweruser_playground.collectionkri` 
with
inst1 as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(
  select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid
from `risk_credit_mis.loan_installments_table` where installmentNumber = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1def as 
( select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
  (select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table`
 where installmentNumber = 1 and isDelinquent = 1 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst1currdef as
(select   loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` 
where installmentNumber = 1 and isDelinquent = 1 and isCurrentDelinquent = 1
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plus as 
(select loanAccountNumber, (principal+interest+penalty)totalamount,(principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid  from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
group by loanAccountNumber
)
),
inst2plusdef as 
(select  loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber
)
),
inst2pluscurrdef as 
(select loanAccountNumber,(principal+interest+penalty)totalamount, (principal+interest+penalty) - (principalpaid+interestpaid+penaltypaid) outstandingamount from 
(select loanAccountNumber, sum(coalesce(Principal, 0)) principal, sum(coalesce(Interest, 0)) interest, sum(coalesce(cast(Penalty as int64), 0)) penalty 
, sum(coalesce(Principal_paid, 0))principalpaid, sum(coalesce(Interest_paid, 0)) interestpaid, sum(coalesce(Penalty_paid, 0)) penaltypaid from `risk_credit_mis.loan_installments_table` where installmentNumber >= 2 
and date_trunc(installmentDueDate, day) <= DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY)
and isDelinquent = 1 and isCurrentDelinquent = 1 and loanAccountNumber not in (select loanAccountNumber from inst1def)
group by loanAccountNumber)
),
activeloans as 
(select loanAccountNumber from `risk_credit_mis.loan_master_table` where flagDisbursement = 1 and coalesce(loanPaidStatus, 'NA') not in ('Written Off',	
'Completed',	
'Settled')),
fraud as 
(select * from `dap_ds_poweruser_playground.fraudlist`),
i1 as 
(
select count(distinct loanAccountNumber) cntlid, 
count(distinct case when loanAccountNumber in (select loanAccountNumber from inst1def) then loanAccountNumber end) cntfirstinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst1def) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst1)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i2 as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then loanAccountNumber end) cntsecondinstallmentdefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when loanAccountNumber in (select loanAccountNumber from inst2plusdef) then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from inst2plus)
and loanAccountNumber in (select loanAccountNumber from activeloans)
),
i90total as 
(select count(distinct loanAccountNumber) cntlid
, count(distinct case when coalesce(Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum(case when coalesce(Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core`  where date_trunc(bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) 
and loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i902022total as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusoveralldefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from `risk_credit_mis.loan_master_table` where format_date('%Y', disbursementDateTime) = '2022'and flagDisbursement = 1)
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
),
i90fraudtotal as 
(select count(distinct lbfrc.loanAccountNumber) cntlid, count(distinct case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then loanAccountNumber end) cnt90plusfrauddefault ,
sum(coalesce(Loan_Principal,0) + coalesce(Loan_Interest,0) + coalesce(Overdue_Penalty,0) + coalesce(Loan_Fee,0)) totalamount,
sum( case when coalesce(lbfrc.Max_current_DPD, 0) > 90 then coalesce(Total_Outstanding_Amount, 0) else 0 end) Total_Outstanding_Amount
from `risk_credit_mis.loan_bucket_flow_report_core` lbfrc where date_trunc(lbfrc.bucketDate, day) = DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) 
and lbfrc.loanAccountNumber in (select loanAccountNumber from activeloans)
and loanAccountNumber in (select loanAccountNumber from inst1)
and cast(loanAccountNumber as integer) in (select ACCOUNTNUMBER from fraud)
)
select 
DATE_SUB(DATE_TRUNC(date_sub(CURRENT_DATE(), interval 3 month), MONTH), INTERVAL 1 DAY) Balance_Date,
-- (select count(loanAccountNumber) from activeloans) activeloans,
-- (select count(loanAccountNumber) from inst1) installmentone,
-- (select count(loanAccountNumber) from inst1def) installmentonedefault,
(select cntfirstinstallmentdefault from i1) firstpaymentdefaultcnt,
(select cntlid from i1) firstpaymentoverallobservation,
(select cntfirstinstallmentdefault from i1)/(select cntlid from i1) firstpaymentdefault,
(select Total_Outstanding_Amount from i1) firstpaymentTotal_Outstanding_Amount,
(select totalamount from i1) firstpaymenttotalamount,
(select Total_Outstanding_Amount from i1)/(select totalamount from i1) firstpaymentdefaultv,

(select cntsecondinstallmentdefault from i2) cntsecondinstallmentdefault,
(select cntlid from i2) secondplusoverallobservation,
(select cntsecondinstallmentdefault from i2)/(select cntlid from i2)secondpluspaymentdefault,

(select Total_Outstanding_Amount from i2) cntsecondTotal_Outstanding_Amount,
(select totalamount from i2) secondplustotalamount,
(select Total_Outstanding_Amount from i2)/(select totalamount from i2)secondpluspaymentdefaultv,

(select cnt90plusoveralldefault from i90total) cnt90plusoveralldefault,
(select cntlid from i90total) totalobservedportfolio,
(select cnt90plusoveralldefault from i90total) / (select cntlid from i90total) overall90plusdefaultrate,

(select Total_Outstanding_Amount from i90total) cnt90plusoverallTotal_Outstanding_Amount,
(select totalamount from i90total) totalobservedportfoliototalamount,
(select Total_Outstanding_Amount from i90total) / (select totalamount from i90total) overall90plusdefaultratev,

(select cnt90plusfrauddefault from i90fraudtotal)cnt90plusfrauddefault,
(select cntlid from i90fraudtotal)totalobservationfraud90plusportfolio,
(select cnt90plusfrauddefault from i90fraudtotal)/(select cntlid from i90fraudtotal) fraud90plusdefaultrate,

(select Total_Outstanding_Amount from i90fraudtotal)cnt90plusfrauddefaultTotal_Outstanding_Amount,
(select totalamount from i90fraudtotal)totalobservationfraud90plusportfoliototalamount,
(select Total_Outstanding_Amount from i90fraudtotal)/(select totalamount from i90fraudtotal) fraud90plusdefaultratev,

(select cnt90plusoveralldefault from i902022total) total90plustdefault2022portfolio,
(select cntlid from i902022total) totalobservation2022portfolio,
(select cnt90plusoveralldefault from i902022total)/(select cntlid from i902022total) total90plus2022defaultrate,

(select Total_Outstanding_Amount from i902022total) total90plustdefault2022portfolioTotal_Outstanding_Amount,
(select totalamount from i902022total) totalobservation2022portfoliototalamount,
(select Total_Outstanding_Amount from i902022total)/(select totalamount from i902022total) total90plus2022defaultratev
;



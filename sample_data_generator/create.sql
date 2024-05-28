CREATE TABLE people (
    customer_id varchar(255),
    gender varchar(5),
    name_prefix varchar(10),
    name_first varchar(255),
    name_last varchar(255),
    email varchar(255),
    employment varchar(255),
    address varchar(255),
    city varchar(255),
    county varchar(255),
    state varchar(255),
    postal_code varchar(255),
    birth_dt date,
    is_deleted boolean,
    job_type varchar(255),
    account_type varchar(255),
    phone_number varchar(255),
    ssn varchar(255),
    allergies varchar(255),
    blood_type varchar(10),
    last_ipaddress varchar(100)
    )

CREATE TABLE transactions (
    trans_customer_id varchar(255),
    orderid varchar(255),
    purchasedatetime datetime,
    transactiontotal decimal,
    numberofitems int,
    productcode varchar(255),
    productcategory varchar(255),
    cc_number varchar(30)
    )

CREATE TABLE social (
    social_customer_id varchar(255),
    social_email varchar(255),
    social_hashtags varchar(255),
    social_last_uri varchar(255),
    social_timestamp varchar(255),
    social_ip varchar(255),
    social_sha256 varchar(255),
    social_uuid4 varchar(255)
    )

select * from people inner join transactions on people.customer_id = transactions.trans_cust_id limit 2600

SELECT TOP 500 t.customer_id, t.orderid, t.purchasedatetime, t.cc_number, p.gender, p.name_prefix, p.name_first, p.name_last, p.email, p.employment, p.address, p.city, p.county, p.state, p.postal_code, p.birth_dt, p.job_type, p.account_type, p.phone_number, p.ssn, p.allergies, p.blood_type, p.last_ipaddress, t.transactiontotal, t.numberofitems, t.productcode FROM dbo.output_transactions t left join dbo.output_people p on t.customer_id = p.customer_id


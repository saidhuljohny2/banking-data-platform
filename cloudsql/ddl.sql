-- TABLE 1: CUSTOMERS (KYC SYSTEM)
CREATE TABLE customers (
    customer_id      BIGINT PRIMARY KEY,
    first_name       VARCHAR(50),
    last_name        VARCHAR(50),
    date_of_birth    DATE,
    email            VARCHAR(100),
    phone            VARCHAR(15),
    kyc_status       VARCHAR(20),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TABLE 2: ACCOUNTS (BANK ACCOUNTS)
CREATE TABLE accounts (
    account_id       BIGINT PRIMARY KEY,
    customer_id      BIGINT,
    account_type     VARCHAR(20),
    balance          DECIMAL(15,2),
    currency         VARCHAR(3),
    status           VARCHAR(20),
    opened_date      DATE,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- TABLE 3: TRANSACTIONS (MOST CRITICAL)
CREATE TABLE transactions (
    transaction_id   BIGINT PRIMARY KEY,
    account_id       BIGINT,
    transaction_type VARCHAR(20),
    amount           DECIMAL(15,2),
    transaction_ts   TIMESTAMP,
    channel          VARCHAR(20),
    status           VARCHAR(20),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);
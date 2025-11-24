CREATE TABLE trades(

    id BIGSERIAL PRIMARY KEY,
    token_id TEXT NOT NULL REFERENCES tokens(token_id), -- id of the token traded -- TOOD-> maybe make a token table
    transaction_hash TEXT,
    side TEXT,
    size FLOAT,
    price FLOAT,
    trade_timestamp TIMESTAMPTZ,
    user_id INT NOT NULL REFERENCES users(id),
    proxy_wallet TEXT, -- proxy wallet of the user
    name TEXT, -- name of the user
    pseudonym TEXT -- pseudonym of the user?
);
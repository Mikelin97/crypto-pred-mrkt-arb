CREATE TABLE users(

    id BIGSERIAL PRIMARY KEY,
    proxy_wallet TEXT, -- proxy wallet of the user
    name TEXT,
    pseudonym TEXT,
    base_address TEXT,
    bio TEXT
);
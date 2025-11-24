CREATE TABLE order_book_updates(

    id BIGSERIAL PRIMARY KEY,
    token_id TEXT NOT NULL REFERENCES tokens(token_id), -- TODO FOREIGN KEY
    price FLOAT NOT NULL,
    size FLOAT NOT NULL,
    side TEXT NOT NULL,
    update_timestamp TIMESTAMPTZ(3) NOT NULL, -- timestamp of update
    send_timestamp TIMESTAMPTZ(3), -- timestamp POLYMARKET SENDS US
    arrival_timestamp TIMESTAMPTZ(3) -- timestamp we got the update

);
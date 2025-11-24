CREATE TABLE order_book_snapshots(

    -- similar to order_book_updates, but we only store the books once every minute to make it easier
    -- to reconstruct an order book at a certain time

    id BIGSERIAL PRIMARY KEY,
    token_id TEXT NOT NULL REFERENCES tokens(token_id), -- TODO FOREIGN KEY
    price FLOAT NOT NULL,
    size FLOAT NOT NULL,
    side TEXT NOT NULL,
    snapshot_timestamp TIMESTAMPTZ(3) NOT NULL
);
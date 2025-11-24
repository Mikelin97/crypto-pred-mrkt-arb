CREATE TABLE tokens(

    id SERIAL PRIMARY KEY,
    token_id TEXT UNIQUE,
    market_id TEXT REFERENCES markets(market_id), -- TOOD -> foreign KEY
    outcome TEXT -- e.g. YES or NO
);
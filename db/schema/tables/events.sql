CREATE TABLE events (

    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    seried_id TEXT REFERENCES series(series_id),
    parent_event_id TEXT, -- sports have subevents like 'spread'
    ticker TEXT,
    slug TEXT,
    title TEXT,
    description TEXT,
    resolution_source TEXT,
    
    category TEXT, -- like Sports, Pop-Culture, NFTs, etc
    subcategory TEXT,

    active BOOLEAN,
    closed BOOLEAN,
    archived BOOLEAN,
    new BOOlEAN,
    featured BOOLEAN,
    restricted BOOLEAN,
    cyom BOOLEAN,
    enable_order_book BOOLEAN,
    neg_risk BOOLEAN,
    enable_neg_risk BOOLEAN,
    neg_risk_augmented BOOLEAN,
    automatically_active BOOLEAN,
    automatically_resolved BOOLEAN,

    creation_date TIMESTAMPTZ(3), -- when birthed in system?
    create_at TIMESTAMPTZ(3),   -- when stored in db?
    updated_at TIMESTAMPTZ(3),   -- last update time
    start_date TIMESTAMPTZ(3),   -- when event starts (often same as endDate for games)
    start_time TIMESTAMPTZ(3),  -- not always there
    end_date TIMESTAMPTZ(3),  -- when event ends (same as SD for sports)
    closed_time TIMESTAMPTZ(3),-- when the event is closed on polymarket
    finished_timestamp TIMESTAMPTZ(3), -- (sports) when the event finished real world

    volume FLOAT,
    open_interest FLOAT,
    liquidity FLOAT,
    volume_24hr FLOAT,
    volume_1wk FLOAT,
    volume_1mo FLOAT,
    liquidity_amm FLOAT,
    liquidity_clob FLOAT,
    comment_count INTEGER

    -- series BIGINT, -> this could be a many to many?
    -- seriesSlug TEXT, 

);
CREATE TABLE markets (
    -- Primary Key / IDs
    id BIGSERIAL PRIMARY KEY,
    market_id TEXT NOT NULL UNIQUE,          -- Polymarket market ID (string/hex)
    event_id TEXT REFERENCES events(event_id),
    condition_id TEXT,                -- Optional condition reference
    slug TEXT,
    resolution_source TEXT,
    game_id TEXT,                     -- ID of the game in hex
    sports_market_type TEXT,
    
    -- Core Market Info
    question TEXT,
    description TEXT,
    category TEXT,
    subcategory TEXT,
    market_type TEXT,
    market_maker_address TEXT,
    
    -- Outcomes / Tokens
    outcomes TEXT[],                  -- Array of outcome names
    clob_token_ids TEXT[],            -- Array of corresponding token IDs
    -- outcome_prices FLOAT[],        -- Optional: store prices per outcome
    
    -- Market Status Flags
    active BOOLEAN,
    closed BOOLEAN,
    fpmm_live BOOLEAN,                -- AMM live
    ready BOOLEAN,                    -- Ready for trading
    funded BOOLEAN,                   -- Liquidity funded
    approved BOOLEAN,
    neg_risk BOOLEAN,                 -- Negative correlation market
    neg_risk_other BOOLEAN,
    accepting_orders BOOLEAN,
    holding_rewards_enabled BOOLEAN,  -- Incentive for holding positions
    enable_order_book BOOLEAN,
    comments_enabled BOOLEAN,
    uma_resolution_status TEXT,       -- e.g., "resolved"

    -- Reward / Incentive Info (Optional)
    rewards_min_size FLOAT,
    rewards_max_spread FLOAT,
    clob_rewards FLOAT,
    
    -- Market Mechanics (Optional / only for orderbook / scalar markets)
    seconds_delay FLOAT,              -- Trade delay time (0 for most markets)
    lower_bound FLOAT,                -- Lower bound on market
    upper_bound FLOAT,                -- Upper bound on market
    order_price_min_tick_size FLOAT,
    order_min_size FLOAT,
    maker_base_fee FLOAT,
    taker_base_fee FLOAT,
    
    -- Trading Prices
    spread FLOAT,
    last_trade_price FLOAT,
    best_bid FLOAT,
    best_ask FLOAT,
    fee FLOAT,
    
    -- Timestamps
    start_date TIMESTAMPTZ(3),
    end_date TIMESTAMPTZ(3),
    created_at TIMESTAMPTZ(3),
    updated_at TIMESTAMPTZ(3),
    closed_time TIMESTAMPTZ(3),
    game_start_time TIMESTAMPTZ(3),
    event_start_time TIMESTAMPTZ(3),
    accepting_orders_timestamp TIMESTAMPTZ(3),
    
    -- Liquidity / Volume
    liquidity FLOAT,
    liquidity_num FLOAT,
    volume FLOAT,
    volume_num FLOAT,
    volume_24h FLOAT,
    volume_1wk FLOAT,
    volume_1mo FLOAT,
    volume_1yr FLOAT
);
CREATE TABLE series(

    id SERIAL PRIMARY KEY,
    series_id TEXT NOT NULL UNIQUE,
    ticker TEXT NOT NULL,
    slug TEXT NOT NULL,
    title TEXT,
    series_type TEXT,
    recurrence TEXT NOT NULL,
    active BOOLEAN,
    closed BOOLEAN,
    published_at TIMESTAMPTZ(3),
    updated_at TIMESTAMPTZ(3),
    start_date TIMESTAMPTZ(3),
    created_at TIMESTAMPTZ(3)

);

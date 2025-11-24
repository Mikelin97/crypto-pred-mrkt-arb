CREATE TABLE tags(

    id SERIAL PRIMARY KEY,
    tag_id TEXT UNIQUE,
    label TEXT,
    updated_at TIMESTAMPTZ(3),
    created_at TIMESTAMPTZ(3),
    published_at TIMESTAMPTZ(3)
);
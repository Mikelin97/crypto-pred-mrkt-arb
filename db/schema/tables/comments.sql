CREATE TABLE comments(

    id SERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    body TEXT,
    parent_entity_type TEXT, -- e.g. Market, Event, 
    parent_entity_id TEXT
    

);
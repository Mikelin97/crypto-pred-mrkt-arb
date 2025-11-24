CREATE TABLE tag_list(
    id SERIAL PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    tag_id TEXT NOT NULL REFERENCES tags(tag_id)
);
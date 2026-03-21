CREATE TABLE IF NOT EXISTS breweries (
    brewery_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    brewery_type TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    phone TEXT,
    website_url TEXT,
    state_province TEXT,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

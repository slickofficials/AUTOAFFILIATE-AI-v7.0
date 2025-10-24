-- AUTOAFFILIATE AI v7.0 DATABASE SCHEMA
-- Run once on Render PostgreSQL Console

-- USERS TABLE
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- POSTS TABLE
CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    platform TEXT,
    content TEXT,
    link TEXT,
    status TEXT DEFAULT 'queued',
    created_at TIMESTAMP DEFAULT NOW(),
    posted_at TIMESTAMP
);

-- EARNINGS TABLE
CREATE TABLE IF NOT EXISTS earnings (
    id SERIAL PRIMARY KEY,
    reference TEXT,
    amount DECIMAL,
    currency TEXT DEFAULT 'NGN',
    network TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- OFFERS TABLE (for Awin/Rakuten)
CREATE TABLE IF NOT EXISTS offers (
    id SERIAL PRIMARY KEY,
    network TEXT,
    product_name TEXT,
    affiliate_link TEXT,
    commission_rate TEXT,
    image_url TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- DEFAULT ADMIN USER (password: beastmode2025)
INSERT INTO users (email, password) 
VALUES ('admin@slick.com', '$2b$12$W8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8')
ON CONFLICT (email) DO NOTHING;

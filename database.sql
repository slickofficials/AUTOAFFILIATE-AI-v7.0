CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT UNIQUE, password TEXT);
CREATE TABLE posts (id SERIAL PRIMARY KEY, platform TEXT, content TEXT, link TEXT, status TEXT, created_at TIMESTAMP);
CREATE TABLE earnings (id SERIAL PRIMARY KEY, reference TEXT, amount DECIMAL, currency TEXT, created_at TIMESTAMP DEFAULT NOW());

INSERT INTO users (email, password) 
VALUES ('admin@slick.com', '$2b$12$W8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8');

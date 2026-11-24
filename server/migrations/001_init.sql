-- BlueRed Central Server Schema (Phase 2)
-- Matches Figure 55 (users, file_catalog, file_reviews) + Community & Chat tables

CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    username VARCHAR(64) NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    failed_login_attempts INTEGER NOT NULL DEFAULT 0 CHECK (failed_login_attempts >= 0),
    locked_until TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS file_catalog (
    file_hash CHAR(64) PRIMARY KEY,
    review_count INTEGER NOT NULL DEFAULT 0 CHECK (review_count >= 0),
    rating_sum INTEGER NOT NULL DEFAULT 0 CHECK (rating_sum >= 0),
    avg_rating DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (avg_rating >= 0.0 AND avg_rating <= 5.0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS file_reviews (
    review_id TEXT PRIMARY KEY,
    file_hash CHAR(64) NOT NULL REFERENCES file_catalog(file_hash) ON DELETE CASCADE,
    author_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    author_username VARCHAR(64) NOT NULL,
    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    comment TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (file_hash, author_id)
);

CREATE INDEX IF NOT EXISTS idx_file_reviews_hash_updated
    ON file_reviews (file_hash, updated_at DESC);

CREATE TABLE IF NOT EXISTS communities (
    community_id TEXT PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    creator_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    parent_community_id TEXT NULL REFERENCES communities(community_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS community_members (
    community_id TEXT NOT NULL REFERENCES communities(community_id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (community_id, user_id)
);

CREATE TABLE IF NOT EXISTS chat_rooms (
    room_id TEXT PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    community_id TEXT NULL REFERENCES communities(community_id) ON DELETE CASCADE,
    is_private BOOLEAN NOT NULL DEFAULT FALSE,
    password_hash TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chat_room_members (
    room_id TEXT NOT NULL REFERENCES chat_rooms(room_id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (room_id, user_id)
);

CREATE TABLE IF NOT EXISTS debate_posts (
    post_id TEXT PRIMARY KEY,
    community_id TEXT NOT NULL REFERENCES communities(community_id) ON DELETE CASCADE,
    author_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    title VARCHAR(200) NOT NULL,
    body TEXT NOT NULL,
    parent_post_id TEXT NULL REFERENCES debate_posts(post_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_debate_posts_community_created
    ON debate_posts (community_id, created_at ASC);

CREATE TABLE IF NOT EXISTS personal_library (
    item_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    file_hash CHAR(64) NOT NULL,
    title VARCHAR(256) NOT NULL,
    saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (owner_id, file_hash)
);

CREATE INDEX IF NOT EXISTS idx_personal_library_owner
    ON personal_library (owner_id, saved_at DESC);

INSERT INTO chat_rooms (room_id, name, community_id, is_private)
VALUES
    ('general', 'General', NULL, FALSE),
    ('music-lossless', 'Audio & Vinilos', NULL, FALSE),
    ('papers-open', 'Documentos Abiertos', NULL, FALSE)
ON CONFLICT (room_id) DO NOTHING;

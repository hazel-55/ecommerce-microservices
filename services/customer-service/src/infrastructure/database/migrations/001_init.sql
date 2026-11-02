BEGIN;

CREATE TABLE IF NOT EXISTS memberships (
    mid INTEGER PRIMARY KEY,
    mname VARCHAR(100) NOT NULL UNIQUE,
    score INTEGER NOT NULL CHECK (score >= 0)
);

CREATE TABLE IF NOT EXISTS customers (
    uid INTEGER PRIMARY KEY CHECK (uid > 0),
    fullname VARCHAR(255) NOT NULL,
    mid INTEGER NOT NULL REFERENCES memberships(mid)
);

INSERT INTO memberships (mid, mname, score)
VALUES
    (1, 'Standard', 0),
    (2, 'Silver', 100),
    (3, 'Gold', 500)
ON CONFLICT (mid) DO NOTHING;

COMMIT;
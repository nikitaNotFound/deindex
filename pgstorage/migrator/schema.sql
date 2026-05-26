CREATE TABLE blocks (
    block_number BIGINT      NOT NULL,
    block_hash   TEXT         NOT NULL,
    parent_hash  TEXT         NOT NULL,
    block_time   TIMESTAMP   NOT NULL,
    network      TEXT         NOT NULL,
    created_at   TIMESTAMP   NOT NULL DEFAULT now(),

    PRIMARY KEY (network, block_number)
);

CREATE INDEX idx_blocks_hash ON blocks (network, block_hash);

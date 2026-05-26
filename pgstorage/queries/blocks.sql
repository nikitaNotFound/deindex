-- name: InsertBlock :exec
INSERT INTO blocks (block_number, block_hash, parent_hash, block_time, network)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (network, block_number) DO NOTHING;

-- name: GetBlockByNumber :one
SELECT * FROM blocks
WHERE network = $1 AND block_number = $2;

-- name: GetBlockByHash :one
SELECT * FROM blocks
WHERE network = $1 AND block_hash = $2;

-- name: GetLatestBlock :one
SELECT * FROM blocks
WHERE network = $1
ORDER BY block_number DESC
LIMIT 1;

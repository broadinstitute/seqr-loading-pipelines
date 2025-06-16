CREATE TABLE seqr.`GRCh38/MITO/key_lookup`
(
    `variantId` String,
    `key` UInt32
)
ENGINE = EmbeddedRocksDB(0, '/var/seqr/clickhouse-data/GRCh38/MITO/key_lookup')
PRIMARY KEY variantId

CREATE TABLE seqr.`GRCh38/MITO/project_gt_stats`
(
    `project_guid` LowCardinality(String),
    `key` UInt32,
    `sample_type` Enum8('WES' = 1, 'WGS' = 2),
    `ref_samples` UInt32,
    `het_samples` UInt32,
    `hom_samples` UInt32
)
ENGINE = SummingMergeTree
PARTITION BY project_guid
ORDER BY (project_guid, key, sample_type)

CREATE MATERIALIZED VIEW seqr.`GRCh38/MITO/entries_to_project_gt_stats_mv` TO seqr.`GRCh38/MITO/project_gt_stats`
AS SELECT
    project_guid,
    key,
    sample_type,
    sum(toInt32(arrayCount(s -> (s.hl == '0'), calls) * sign)) AS ref_samples,
    sum(toInt32(arrayCount(s -> (s.hl > '0' AND s.hl < '0.95'), calls) * sign)) AS het_samples,
    sum(toInt32(arrayCount(s -> (s.hl >= '0.95'), calls) * sign)) AS hom_samples
FROM seqr.`GRCh38/MITO/entries`
GROUP BY
    project_guid,
    key,
    sample_type;

CREATE TABLE seqr.`GRCh38/MITO/gt_stats`
(
    `key` UInt32,
    `het_wes` UInt32,
    `het_wgs` UInt32,
    `hom_wes` UInt32,
    `hom_wgs` UInt32
)
ENGINE = SummingMergeTree
ORDER BY key;

CREATE MATERIALIZED VIEW seqr.`GRCh38/MITO/project_gt_stats_to_gt_stats_mv`
REFRESH EVERY 10 YEAR TO seqr.`GRCh38/MITO/gt_stats`
AS SELECT
    key,
    sumIf(het_samples, sample_type = 'WES') AS het_wes,
    sumIf(het_samples, sample_type = 'WGS') AS het_wgs,
    sumIf(hom_samples, sample_type = 'WES') AS hom_wes,
    sumIf(hom_samples, sample_type = 'WGS') AS hom_wgs
FROM seqr.`GRCh38/MITO/project_gt_stats`
GROUP BY key



CREATE TABLE seqr.`GRCh37/MITO/key_lookup`
(
    `variantId` String,
    `key` UInt32
)
ENGINE = EmbeddedRocksDB(0, '/var/seqr/clickhouse-data/GRCh37/MITO/key_lookup')
PRIMARY KEY variantId;

CREATE TABLE seqr.`GRCh37/SNV_INDEL/project_gt_stats`
(
    `project_guid` String,
    `key` UInt32,
    `sample_type` Enum8('WES' = 1, 'WGS' = 2),
    `ref_samples` UInt32,
    `het_samples` UInt32,
    `hom_samples` UInt32
)
ENGINE = SummingMergeTree
PARTITION BY project_guid
ORDER BY (project_guid, key, sample_type);

CREATE MATERIALIZED VIEW seqr.`GRCh37/SNV_INDEL/entries_to_project_gt_stats_mv` TO seqr.`GRCh37/SNV_INDEL/project_gt_stats`
AS SELECT
    project_guid,
    key,
    sample_type,
    sum(toInt32(arrayCount(s -> (s.gt = 'REF'), calls) * sign)) AS ref_samples,
    sum(toInt32(arrayCount(s -> (s.gt = 'HET'), calls) * sign)) AS het_samples,
    sum(toInt32(arrayCount(s -> (s.gt = 'HOM'), calls) * sign)) AS hom_samples
FROM seqr.`GRCh37/SNV_INDEL/entries`
GROUP BY
    project_guid,
    key,
    sample_type

CREATE TABLE seqr.`GRCh37/SNV_INDEL/gt_stats`
(
    `key` UInt32,
    `ac_wes` UInt32,
    `ac_wgs` UInt32,
    `hom_wes` UInt32,
    `hom_wgs` UInt32
)
ENGINE = SummingMergeTree
ORDER BY key;

CREATE MATERIALIZED VIEW seqr.`GRCh37/SNV_INDEL/project_gt_stats_to_gt_stats_mv`
REFRESH EVERY 10 YEAR TO seqr.`GRCh37/SNV_INDEL/gt_stats`
AS SELECT
    key,
    sumIf((het_samples * 1) + (hom_samples * 2), sample_type = 'WES') AS ac_wes,
    sumIf((het_samples * 1) + (hom_samples * 2), sample_type = 'WGS') AS ac_wgs,
    sumIf(hom_samples, sample_type = 'WES') AS hom_wes,
    sumIf(hom_samples, sample_type = 'WGS') AS hom_wgs
FROM seqr.`GRCh37/SNV_INDEL/project_gt_stats`
GROUP BY key

CREATE DICTIONARY seqr.`GRCh37/SNV_INDEL/gt_stats_dict`
(
    `key` UInt32,
    `ac_wes` UInt32,
    `ac_wgs` UInt32,
    `hom_wes` UInt32,
    `hom_wgs` UInt32
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(USER 'seqr_clickhouse_writer' PASSWORD '[HIDDEN]' DB seqr TABLE `GRCh37/SNV_INDEL/gt_stats`))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT(MAX_ARRAY_SIZE 1000000000))
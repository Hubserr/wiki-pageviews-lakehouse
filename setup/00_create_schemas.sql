
CREATE SCHEMA IF NOT EXISTS workspace.landing
    COMMENT 'Raw files as downloaded from source, before loading into tables';

CREATE SCHEMA IF NOT EXISTS workspace.bronze
    COMMENT 'Raw data loaded 1:1 from landing files into Delta tables';

CREATE SCHEMA IF NOT EXISTS workspace.silver
    COMMENT 'Cleaned and typed data, deduplicated';

CREATE SCHEMA IF NOT EXISTS workspace.gold
    COMMENT 'Business-ready data: dimensional model (facts and dimensions)';

CREATE VOLUME IF NOT EXISTS workspace.landing.wikimedia
    COMMENT 'Raw Wikimedia pageview dump files (.gz)';

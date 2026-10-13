-- Table descriptions for the pageviews pipeline.

COMMENT ON TABLE workspace.bronze.pageviews_raw IS
'Raw Wikimedia hourly pageview dumps loaded 1:1 from landing files. One row per line of a source file (value = raw text). Technical columns: _source_file (path of the source .gz file), _ingested_at (load time). Loaded per day with replaceWhere on _source_file.';

COMMENT ON TABLE workspace.silver.pageviews IS
'Hourly Wikimedia pageviews: one row per project (domain_code), page title and hour. All timestamps are in UTC. total_response_size dropped (always 0 in source). Loaded per day with replaceWhere on view_date. Source: bronze.pageviews_raw.';

COMMENT ON TABLE workspace.gold.pl_top_articles_hourly IS
'Top 100 most viewed articles of Polish Wikipedia (pl + pl.m, desktop and mobile summed) per hour. One row per hour and rank. Non-article namespaces excluded. Timestamps in UTC. Source: silver.pageviews.';
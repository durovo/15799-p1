- pg_qualstats <- get columns from where indexes <- might need pg-replay for this
- use client to run explain and extract cost
- get original query <- map to indexes and run
- check if pg_replay is needed


Ordered:
- Get all used columns
- generate single width index recommendations
- run explain using hypopg
- Use generalize 
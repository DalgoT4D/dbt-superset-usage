# Dbt run
```sh
export DBT_USAGE_VER=v1 && dbt run
```

# Onboard a new org
1. Create a new connection in the production Dalgo org `superset_usage`. The connection is to pull superset metadata 

2. Make sure the destination schema name used is same as org slug in the dalgo backend. Since this is the slug on which we do the `rls` & make sure that the org sees only their data when they head to `Analysis -> Usage`

3. Add the following tables/streams in the connection
    - logs (append/dedup on `dttm` with pk as `id`)
    - ab_user (overwrite)
    - ab_user_role (overwrite)
    - ab_role (overwrite)
    - slices (overwrite)
    - dashboards (overwrite)
    - dashboard_roles (overwrite)

4. Sync the connection

5. Create a new branch on this repo

6. Update the `macros/fetch_org_names.sql` macros & add new org slug name

7. Add a new source for the org (those 7 streams) in `models/sources/schema.yml`

8. Run a full refresh on logs. This is need for the first time 
```sh
dbt run --select logs --vars '{"full_refresh_logs_for_org": "<new_org_slug>"}'
```

9. Push all the code to git and submit the PR. 

10. Setup a daily pipeline to sync the superset metadata for this org frmo the orchestrate page. Make sure its setup before the `Dbt transform` pipeline
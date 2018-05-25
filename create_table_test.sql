create keyspace test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE test
(
       uuid uuid,
       code text,
       user_id text,
       text text,
       is_test boolean,
       created_at timestamp,
       PRIMARY KEY ((user_id), code)
);

create materialized view test_by_created_at AS
  select
    *
  from
    test
  where
    code is not null and user_id is not null and created_at is not null
  primary key ((user_id), created_at, code)
with clustering order by (created_at desc);

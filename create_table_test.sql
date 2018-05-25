create keyspace test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE test
(
       uuid uuid,
       code text,
       text text,
       is_test boolean,
       created_at timestamp,
       PRIMARY KEY (code)
);

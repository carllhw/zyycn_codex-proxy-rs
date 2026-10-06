alter table runtime_settings
  add column openai_session_affinity_ttl_hours bigint not null default 24
    check (openai_session_affinity_ttl_hours between 1 and 720);

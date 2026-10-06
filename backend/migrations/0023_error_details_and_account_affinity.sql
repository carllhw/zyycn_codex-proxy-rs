-- 运维错误详情同时承载本地来源链与上游响应，运行时只使用统一字段
alter table model_requests rename column raw_upstream_error to error_details;
alter table ops_events rename column raw_upstream_error to error_details;

alter table runtime_settings
  add column openai_account_affinity text not null default 'relaxed'
    check (openai_account_affinity in ('relaxed', 'strict')),
  add column max_account_rotations bigint not null default 3
    check (max_account_rotations between 0 and 31),
  add column openai_session_affinity_ttl_hours bigint not null default 24
    check (openai_session_affinity_ttl_hours between 1 and 720);

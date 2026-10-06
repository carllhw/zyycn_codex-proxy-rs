alter table runtime_settings
  add column openai_account_affinity text not null default 'relaxed'
    check (openai_account_affinity in ('relaxed', 'strict')),
  add column max_account_rotations bigint not null default 3
    check (max_account_rotations between 0 and 31);

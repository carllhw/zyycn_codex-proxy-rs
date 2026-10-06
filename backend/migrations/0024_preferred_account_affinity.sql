-- 会话主账号优先作为独立模式，保留已有亲和配置
alter table runtime_settings
  drop constraint runtime_settings_openai_account_affinity_check,
  add constraint runtime_settings_openai_account_affinity_check
    check (openai_account_affinity in ('relaxed', 'preferred', 'strict')),
  alter column openai_account_affinity set default 'strict';

-- 尚未发布过配置的初始记录采用严格模式，已经保存的设置保留原选项
update runtime_settings set openai_account_affinity = 'strict' where config_revision = 1;

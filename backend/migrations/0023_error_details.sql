-- 运维错误详情同时承载本地来源链与上游响应，运行时只使用统一字段
alter table model_requests rename column raw_upstream_error to error_details;
alter table ops_events rename column raw_upstream_error to error_details;

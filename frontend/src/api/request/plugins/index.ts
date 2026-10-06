import type { AxiosResponse } from 'axios'
import type { RequestConfig } from '../index'
import type { RawResponseOptions } from './raw'

export interface RequestContext {
  config: RequestConfig
  raw?: RawResponseOptions
  // 会话插件补充身份代际检查，重试与提示消费同一请求有效性
  isCurrent: () => boolean
}

export type RequestNext = () => Promise<AxiosResponse<unknown>>

export type RequestPlugin = (context: RequestContext, next: RequestNext) => Promise<AxiosResponse<unknown>>

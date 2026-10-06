import type { RequestConfig } from '../index'
import type { RequestContext, RequestNext } from './index'
import { toast } from '@codex-proxy/ui'
import { ApiError, isSessionRequired } from './error'

// 会话失效是后端业务事实，登录凭据错误和 403 不清除已有会话
const SESSION_REQUIRED = 40101
let unauthorizedHandled = false
let sessionGeneration = 0
let unauthorizedHandler: (() => void | Promise<void>) | undefined
let sessionRecoveryHandler: (() => Promise<boolean>) | undefined
let sessionRecovery: Promise<boolean> | undefined
let successfulRecoveries = 0

export function setSessionRecoveryHandler(handler: () => Promise<boolean>) {
  sessionRecoveryHandler = handler
}

export function setUnauthorizedHandler(handler: () => void | Promise<void>) {
  unauthorizedHandler = handler
}

export function resetUnauthorizedHandling() {
  sessionGeneration += 1
  unauthorizedHandled = false
}

function handleUnauthorizedOnce() {
  if (unauthorizedHandled || !unauthorizedHandler)
    return
  unauthorizedHandled = true
  void Promise.resolve(unauthorizedHandler()).catch(() => {
    unauthorizedHandled = false
  })
}

function recoverSession(): Promise<boolean> {
  if (sessionRecovery)
    return sessionRecovery
  const recovery = sessionRecoveryHandler!().then((authenticated) => {
    if (authenticated)
      successfulRecoveries += 1
    return authenticated
  }).finally(() => {
    if (sessionRecovery === recovery)
      sessionRecovery = undefined
  })
  sessionRecovery = recovery
  return recovery
}

function expireSession(error: ApiError, config: RequestConfig, generation: number): never {
  if (!config.signal?.aborted && generation === sessionGeneration) {
    const alreadyHandled = unauthorizedHandled
    handleUnauthorizedOnce()
    if (!config.silent && !alreadyHandled)
      toast.error(error.message)
  }
  throw error
}

export async function sessionPlugin(context: RequestContext, send: RequestNext) {
  const { config } = context
  const generation = sessionGeneration
  const isCurrent = context.isCurrent
  context.isCurrent = () => isCurrent() && generation === sessionGeneration
  if (!config.skipSessionRecovery && sessionRecovery && !await sessionRecovery)
    return expireSession(new ApiError('登录已失效，请重新登录', 401, SESSION_REQUIRED), config, generation)
  if (config.signal?.aborted || generation !== sessionGeneration)
    throw new ApiError('请求已取消', 0, undefined, undefined, 'cancelled')
  const recovered = successfulRecoveries
  try {
    return await send()
  }
  catch (error) {
    if (!isSessionRequired(error) || config.signal?.aborted || generation !== sessionGeneration)
      throw error
    if (!config.skipSessionRecovery && sessionRecoveryHandler
      && (successfulRecoveries !== recovered || await recoverSession())) {
      // 40101 表示认证阶段拒绝；恢复成功后原请求最多重放一次
      if (config.signal?.aborted || generation !== sessionGeneration)
        throw error
      try {
        return await send()
      }
      catch (retryError) {
        if (!isSessionRequired(retryError))
          throw retryError
        return expireSession(retryError, config, generation)
      }
    }
    return expireSession(error, config, generation)
  }
}

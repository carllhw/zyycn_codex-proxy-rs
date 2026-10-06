import type { RequestContext, RequestNext } from './index'
import { ApiError, isTransientApiError } from './error'

export async function retryPlugin(context: RequestContext, next: RequestNext) {
  const { config } = context
  const retry = config.retry ?? ['GET', 'HEAD'].includes((config.method ?? 'GET').toUpperCase())
  for (let attempt = 0; ; attempt += 1) {
    if (!context.isCurrent())
      throw new ApiError('请求已取消', 0, undefined, undefined, 'cancelled')
    try {
      return await next()
    }
    catch (error) {
      if (!retry || attempt >= 2 || !isTransientApiError(error) || !context.isCurrent())
        throw error
      await new Promise<void>((resolve) => {
        let timer: ReturnType<typeof setTimeout>
        const finish = () => {
          clearTimeout(timer)
          config.signal?.removeEventListener('abort', finish)
          resolve()
        }
        timer = setTimeout(finish, 500 * 2 ** attempt)
        config.signal?.addEventListener('abort', finish, { once: true })
      })
    }
  }
}

import request from '../request'

export interface AuthSession {
  role: 'admin' | 'key'
  expiresAt: string
}

export type LoginParam
  = | { mode: 'admin', username: string, password: string }
    | { mode: 'key', apiKey: string }

export interface AuthStatusResponse {
  authenticated: boolean
  session: AuthSession | null
}

export interface LogoutResponse {
  message: string
}

let pendingMutation: Promise<unknown> = Promise.resolve()

// Cookie 写入由浏览器完成，跨标签页串行认证操作以免晚到的续期覆盖登录或退出
function withSessionLock<T>(operation: () => Promise<T>): Promise<T> {
  const run = () => navigator.locks
    ? navigator.locks.request('cpr-session', operation)
    : operation()
  const result = pendingMutation.then(run, run)
  pendingMutation = result.catch(() => {})
  return result
}

export function login(data: LoginParam) {
  return withSessionLock(() => request<AuthSession>({
    url: '/api/auth/login',
    method: 'POST',
    skipSessionRecovery: true,
    data,
  }))
}

export function getAuthStatus() {
  return request<AuthStatusResponse>({
    url: '/api/auth/status',
    method: 'GET',
    skipSessionRecovery: true,
    silent: true,
    timeout: 10_000,
  })
}

export function refreshAuthSession() {
  return withSessionLock(() => request<AuthStatusResponse>({
    url: '/api/auth/refresh',
    method: 'POST',
    skipSessionRecovery: true,
    retry: true,
    silent: true,
    timeout: 10_000,
  }))
}

export function logout() {
  return withSessionLock(() => request<LogoutResponse>({
    url: '/api/auth/logout',
    method: 'POST',
    skipSessionRecovery: true,
  }))
}

export function changeAdminPassword(data: { currentPassword: string, newPassword: string }) {
  return withSessionLock(() => request<{ message: string }>({
    url: '/api/auth/password',
    method: 'POST',
    skipSessionRecovery: true,
    data,
  }))
}

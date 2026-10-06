import type { AuthSession } from '@/api'

import { defineStore } from 'pinia'
import { computed, shallowRef } from 'vue'

import { login as apiLogin, logout as apiLogout, refreshAuthSession } from '@/api'
import { resetUnauthorizedHandling } from '@/api/request'

export const useAuthStore = defineStore('auth', () => {
  const session = shallowRef<AuthSession | null>(null)
  const isAuthenticated = computed(() => session.value !== null)
  const isAdmin = computed(() => session.value?.role === 'admin')
  const sessionChecked = shallowRef(false)
  const loading = shallowRef(false)
  let revision = 0
  const pendingCheck = shallowRef<Promise<boolean>>()
  const checking = computed(() => pendingCheck.value !== undefined)

  function checkAuth(): Promise<boolean> {
    if (pendingCheck.value)
      return pendingCheck.value
    const currentRevision = revision
    const check = refreshAuthSession().then((status) => {
      // 登录或退出之后到达的旧状态响应，不覆盖新会话。
      if (currentRevision === revision) {
        const wasAuthenticated = isAuthenticated.value
        session.value = status.session
        sessionChecked.value = true
        if (status.authenticated && !wasAuthenticated)
          resetUnauthorizedHandling()
      }
      return isAuthenticated.value
    }).finally(() => {
      if (pendingCheck.value === check) {
        pendingCheck.value = undefined
      }
    })
    pendingCheck.value = check
    return check
  }

  async function login(payload: Parameters<typeof apiLogin>[0]) {
    if (loading.value)
      return null
    revision += 1
    pendingCheck.value = undefined
    loading.value = true
    try {
      const result = await apiLogin(payload)
      revision += 1
      pendingCheck.value = undefined
      session.value = result
      sessionChecked.value = true
      resetUnauthorizedHandling()
      return result
    }
    catch {
      return null
    }
    finally {
      loading.value = false
    }
  }

  async function logout() {
    if (loading.value)
      return false
    loading.value = true
    revision += 1
    pendingCheck.value = undefined
    try {
      await apiLogout()
      invalidateSession()
      return true
    }
    catch {
      // 只有服务端确认撤销后才退出，避免刷新又恢复一个未撤销的会话。
      return false
    }
    finally {
      loading.value = false
    }
  }

  function invalidateSession() {
    revision += 1
    pendingCheck.value = undefined
    session.value = null
    sessionChecked.value = true
    resetUnauthorizedHandling()
  }

  return { session, isAuthenticated, isAdmin, sessionChecked, loading, checking, checkAuth, login, logout, invalidateSession }
})

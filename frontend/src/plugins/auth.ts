import type { Plugin } from 'vue'

import { setSessionRecoveryHandler, setUnauthorizedHandler } from '@/api/request'
import { router } from '@/router'
import { pinia } from '@/stores'
import { useAuthStore } from '@/stores/modules/auth'

export const authPlugin: Plugin = {
  install(app) {
    const authStore = useAuthStore(pinia)
    let lastAttempt = 0

    async function redirectToLogin(loginMode = authStore.session?.role ?? 'admin') {
      authStore.invalidateSession()
      if (router.currentRoute.value.name !== 'login')
        await router.replace({ name: 'login', state: { loginMode } })
    }

    setUnauthorizedHandler(() => redirectToLogin())
    setSessionRecoveryHandler(() => authStore.checkAuth())

    async function restore(force = false) {
      if (document.visibilityState !== 'visible' || authStore.loading || authStore.checking)
        return
      if (!authStore.sessionChecked) {
        if (force)
          await router.replace(`${window.location.pathname}${window.location.search}${window.location.hash}`)
        return
      }
      if (!authStore.isAdmin)
        return
      // 只由可见页面的使用行为触发，后台轮询不会无限延长不活动期限
      const remaining = Date.parse(authStore.session!.expiresAt) - Date.now()
      const interval = Math.max(0, Math.min(60_000, remaining / 4))
      if (!force && Date.now() - lastAttempt < interval)
        return
      lastAttempt = Date.now()
      try {
        if (!await authStore.checkAuth())
          await redirectToLogin('admin')
      }
      catch {
        // 服务不可用不代表退出，后续操作或恢复联网后再次确认
      }
    }

    const activity = () => {
      void restore()
    }
    const resume = () => {
      void restore(true)
    }
    document.addEventListener('pointerdown', activity, { passive: true })
    document.addEventListener('keydown', activity)
    document.addEventListener('scroll', activity, { passive: true, capture: true })
    document.addEventListener('visibilitychange', resume)
    window.addEventListener('focus', resume)
    window.addEventListener('online', resume)
    app.onUnmount(() => {
      document.removeEventListener('pointerdown', activity)
      document.removeEventListener('keydown', activity)
      document.removeEventListener('scroll', activity, true)
      document.removeEventListener('visibilitychange', resume)
      window.removeEventListener('focus', resume)
      window.removeEventListener('online', resume)
    })
  },
}

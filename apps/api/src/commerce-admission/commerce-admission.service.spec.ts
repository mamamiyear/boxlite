/*
 * Copyright 2025 BoxLite AI
 * SPDX-License-Identifier: AGPL-3.0
 */

import { Logger } from '@nestjs/common'
import axios from 'axios'
import { CommerceAdmissionService } from './commerce-admission.service'

jest.mock('axios', () => ({
  __esModule: true,
  default: {
    post: jest.fn(),
    isAxiosError: (error: any) => Boolean(error?.isAxiosError),
  },
}))

const post = axios.post as jest.Mock
const reservationId = '550e8400-e29b-41d4-a716-446655440000'

function service(enabled = true) {
  const config = {
    get: jest.fn().mockReturnValue({
      enabled,
      url: enabled ? 'https://commerce.test' : undefined,
      token: enabled ? 'token' : undefined,
      timeoutMs: 500,
    }),
  }
  return new CommerceAdmissionService(config as any)
}

const denied = {
  admission: false,
  reason: 'INSUFFICIENT_AVAILABLE_CREDIT',
  requiredCreditCents: '7',
  effectiveAvailableCreditCents: '3',
}

const allowed = {
  admission: true,
  reason: 'SUFFICIENT_AVAILABLE_CREDIT',
  reservationId,
  requiredCreditCents: '7',
  effectiveAvailableCreditCents: '100',
}

const resources = { cpu: 1, gpu: 0, mem: 1, disk: 10 }

function request(scenario: 'CREATE-BOX' | 'START-BOX', requestedResources = resources) {
  return { scenario, organizationId: 'org-1', resources: requestedResources }
}

describe('CommerceAdmissionService', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: new Date('2026-08-13T10:00:00.000Z') })
    post.mockReset()
    jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined)
  })

  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  it('caches an explicit organization denial for exactly 30 seconds', async () => {
    const admission = service()
    post.mockResolvedValue({ data: denied })

    await expect(admission.admit(request('CREATE-BOX'))).rejects.toMatchObject({
      status: 402,
      response: { statusCode: 402, message: 'INSUFFICIENT_AVAILABLE_CREDIT', error: 'Payment Required' },
    })
    await expect(
      admission.admit(request('START-BOX', { cpu: 1, gpu: 0, mem: 1, disk: 1 })),
    ).rejects.toMatchObject({
      status: 402,
    })
    expect(post).toHaveBeenCalledTimes(1)

    jest.advanceTimersByTime(30_001)
    await expect(
      admission.admit(request('START-BOX', { cpu: 1, gpu: 0, mem: 1, disk: 1 })),
    ).rejects.toMatchObject({
      status: 402,
    })
    expect(post).toHaveBeenCalledTimes(2)
  })

  it('does not cache technical failures and fails open', async () => {
    const admission = service()
    post.mockRejectedValue({ isAxiosError: true, code: 'ECONNABORTED', message: 'timeout' })

    await expect(admission.admit(request('START-BOX'))).resolves.toBeNull()
    await expect(admission.admit(request('START-BOX'))).resolves.toBeNull()
    expect(post).toHaveBeenCalledTimes(2)
    expect(Logger.prototype.warn).toHaveBeenCalledWith(expect.stringContaining('allowing START-BOX'))
  })

  it.each(['CREATE-BOX', 'START-BOX'] as const)(
    'accepts %s and sends the fixed contract with a 500ms timeout',
    async (scenario) => {
      const admission = service()
      post.mockResolvedValue({ data: allowed })

      await expect(
        admission.admit(request(scenario, { cpu: 2, gpu: 1, mem: 4, disk: 20 })),
      ).resolves.toEqual({
        organizationId: 'org-1',
        reservationId,
      })
      expect(post).toHaveBeenCalledWith(
        'https://commerce.test/internal/admission',
        expect.objectContaining({
          requestId: expect.stringMatching(/^[0-9a-f-]{36}$/),
          scenario,
          organizationId: 'org-1',
          resources: { cpu: 2, gpu: 1, mem: 4, disk: 20 },
        }),
        expect.objectContaining({ timeout: 500 }),
      )
    },
  )

  it('treats a malformed 200 response as fail-open rather than a denial', async () => {
    const admission = service()
    post.mockResolvedValue({ data: { admission: false, reason: 'unknown' } })

    await expect(admission.admit(request('START-BOX'))).resolves.toBeNull()
    expect(Logger.prototype.warn).toHaveBeenCalledWith(expect.stringContaining('malformed'))
  })

  it('rejects an explicit stale-snapshot decision without caching it', async () => {
    const admission = service()
    post.mockResolvedValue({
      data: {
        admission: false,
        reason: 'STALE_USAGE_SNAPSHOT',
        requiredCreditCents: '7',
        effectiveAvailableCreditCents: '100',
      },
    })

    await expect(admission.admit(request('START-BOX'))).rejects.toMatchObject({
      status: 503,
      response: { statusCode: 503, message: 'STALE_USAGE_SNAPSHOT', error: 'Service Unavailable' },
    })
    await expect(admission.admit(request('START-BOX'))).rejects.toMatchObject({
      status: 503,
    })
    expect(post).toHaveBeenCalledTimes(2)
  })

  it('does nothing when Commerce admission is not configured', async () => {
    const admission = service(false)

    await expect(admission.admit(request('START-BOX'))).resolves.toBeNull()
    expect(post).not.toHaveBeenCalled()
  })

  it('best-effort releases an accepted reservation', async () => {
    const admission = service()
    post.mockResolvedValue({ data: { released: true } })

    await admission.release({ organizationId: 'org-1', reservationId })

    expect(post).toHaveBeenCalledWith(
      'https://commerce.test/internal/admission/release',
      { organizationId: 'org-1', reservationId },
      expect.objectContaining({ timeout: 500 }),
    )
  })
})

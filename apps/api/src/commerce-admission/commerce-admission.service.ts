/*
 * Copyright 2025 BoxLite AI
 * SPDX-License-Identifier: AGPL-3.0
 */

import { HttpException, Injectable, Logger, OnModuleDestroy, ServiceUnavailableException } from '@nestjs/common'
import axios from 'axios'
import { randomUUID } from 'node:crypto'
import NodeCache from 'node-cache'
import { TypedConfigService } from '../config/typed-config.service'

const DENIAL_CACHE_TTL_SECONDS = 30
const DENIAL_CACHE_MAX_ORGANIZATIONS = 100_000

export type CreateBoxAdmissionResources = {
  cpu: number
  gpu: number
  mem: number
  disk: number
}

export type CommerceAdmissionReservation = {
  organizationId: string
  reservationId: string
}

type AdmissionDecision =
  | {
      admission: true
      reason: 'SUFFICIENT_AVAILABLE_CREDIT'
      reservationId: string
      requiredCreditCents: string
      effectiveAvailableCreditCents: string
    }
  | {
      admission: false
      reason: 'INSUFFICIENT_AVAILABLE_CREDIT' | 'STALE_USAGE_SNAPSHOT'
      requiredCreditCents: string
      effectiveAvailableCreditCents: string
    }

type CachedDenial = {
  reason: 'INSUFFICIENT_AVAILABLE_CREDIT'
}

@Injectable()
export class CommerceAdmissionService implements OnModuleDestroy {
  private readonly logger = new Logger(CommerceAdmissionService.name)
  private readonly denials = new NodeCache({
    stdTTL: DENIAL_CACHE_TTL_SECONDS,
    checkperiod: DENIAL_CACHE_TTL_SECONDS,
    useClones: false,
    maxKeys: DENIAL_CACHE_MAX_ORGANIZATIONS,
  })

  constructor(private readonly config: TypedConfigService) {}

  onModuleDestroy(): void {
    this.denials.close()
  }

  async admitCreateBox(
    organizationId: string,
    resources: CreateBoxAdmissionResources,
  ): Promise<CommerceAdmissionReservation | null> {
    const cached = this.denials.get<CachedDenial>(organizationId)
    if (cached) throw paymentRequired(cached.reason)

    const settings = this.config.get('commerceAdmission')
    if (!settings.enabled) return null
    const requestId = randomUUID()
    const startedAt = Date.now()
    let raw: unknown
    try {
      const response = await axios.post(
        `${settings.url}/internal/admission`,
        { requestId, scenario: 'CREATE-BOX', organizationId, resources },
        {
          timeout: settings.timeoutMs,
          headers: {
            authorization: `Bearer ${settings.token}`,
            'content-type': 'application/json',
          },
        },
      )
      raw = response.data
    } catch (error) {
      this.warnFailOpen('request failed', organizationId, startedAt, error)
      return null
    }

    const decision = parseDecision(raw)
    if (!decision) {
      this.warnFailOpen('response was malformed', organizationId, startedAt)
      return null
    }
    if (decision.admission === false) {
      if (decision.reason === 'STALE_USAGE_SNAPSHOT') {
        throw new ServiceUnavailableException(decision.reason)
      }
      try {
        this.denials.set(organizationId, { reason: decision.reason })
      } catch (error) {
        this.logger.warn(`Commerce admission denial cache write failed for ${organizationId}: ${message(error)}`)
      }
      throw paymentRequired(decision.reason)
    }
    return { organizationId, reservationId: decision.reservationId }
  }

  async release(reservation: CommerceAdmissionReservation): Promise<void> {
    const settings = this.config.get('commerceAdmission')
    if (!settings.enabled) return
    const startedAt = Date.now()
    try {
      await axios.post(`${settings.url}/internal/admission/release`, reservation, {
        timeout: settings.timeoutMs,
        headers: {
          authorization: `Bearer ${settings.token}`,
          'content-type': 'application/json',
        },
      })
    } catch (error) {
      this.logger.warn(
        `Commerce admission release failed for ${reservation.organizationId} after ${Date.now() - startedAt}ms: ${describe(error)}`,
      )
    }
  }

  private warnFailOpen(operation: string, organizationId: string, startedAt: number, error?: unknown): void {
    const detail = error === undefined ? '' : `: ${describe(error)}`
    this.logger.warn(
      `Commerce admission ${operation} for ${organizationId} after ${Date.now() - startedAt}ms; allowing create${detail}`,
    )
  }
}

function parseDecision(raw: unknown): AdmissionDecision | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null
  const value = raw as Record<string, unknown>
  if (
    typeof value.admission !== 'boolean' ||
    typeof value.reason !== 'string' ||
    typeof value.requiredCreditCents !== 'string' ||
    !/^\d+$/.test(value.requiredCreditCents) ||
    typeof value.effectiveAvailableCreditCents !== 'string' ||
    !/^-?\d+$/.test(value.effectiveAvailableCreditCents)
  ) {
    return null
  }
  if (value.admission) {
    if (value.reason !== 'SUFFICIENT_AVAILABLE_CREDIT' || !isUuid(value.reservationId)) return null
  } else if (!['INSUFFICIENT_AVAILABLE_CREDIT', 'STALE_USAGE_SNAPSHOT'].includes(value.reason)) {
    return null
  }
  return value as unknown as AdmissionDecision
}

function paymentRequired(reason: 'INSUFFICIENT_AVAILABLE_CREDIT'): HttpException {
  return new HttpException({ statusCode: 402, message: reason, error: 'Payment Required' }, 402)
}

function isUuid(value: unknown): value is string {
  return typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)
}

function describe(error: unknown): string {
  if (axios.isAxiosError(error)) {
    return `${error.code ?? 'HTTP'} ${error.response?.status ?? ''} ${error.message}`.trim()
  }
  return message(error)
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

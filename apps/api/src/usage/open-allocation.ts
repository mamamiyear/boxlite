/*
 * Copyright 2025 BoxLite AI
 * SPDX-License-Identifier: AGPL-3.0
 */

import { identityString, InvalidUsagePeriodError, quantityString, timestampString } from './usage-event'

/**
 * One allocation that Commerce has not yet acknowledged as finalized.
 * `endAt` is absent for a live period and is the real close time while its
 * finalized event remains undelivered in the transactional outbox.
 */
export interface OpenAllocation {
  organizationId: string
  boxId: string
  region: string
  startAt: Date
  endAt?: Date | null
  cpu: number
  gpu: number
  mem: number
  disk: number
}

/**
 * One allocation, exactly as it crosses the wire in a snapshot push.
 *
 * No `eventKey`: the snapshot is a replace-all observation, while
 * organizationId+boxId+startAt is the interval identity Commerce uses to
 * reconcile an optional closing row with its finalized event.
 */
export interface OpenAllocationDto {
  organizationId: string
  boxId: string
  region: string
  startAt: string
  endAt?: string
  cpu: string
  gpu: string
  mem: string
  disk: string
}

/**
 * Builds the exact bytes sent to Commerce for one unfinalized allocation.
 *
 * Reuses `usage-event.ts`'s field encoders so snapshot and finalized forms
 * describe organizationId/boxId/region/quantities identically.
 */
export function toOpenAllocationDto(allocation: OpenAllocation): OpenAllocationDto {
  const dto: OpenAllocationDto = {
    organizationId: identityString(allocation.organizationId, 'organizationId'),
    boxId: identityString(allocation.boxId, 'boxId'),
    region: identityString(allocation.region, 'region'),
    startAt: timestampString(allocation.startAt, 'startAt'),
    cpu: quantityString(allocation.cpu, 'cpu'),
    gpu: quantityString(allocation.gpu, 'gpu'),
    mem: quantityString(allocation.mem, 'mem'),
    disk: quantityString(allocation.disk, 'disk'),
  }

  if (allocation.endAt == null) {
    return dto
  }

  const endAt = timestampString(allocation.endAt, 'endAt')
  if (allocation.endAt < allocation.startAt) {
    throw new InvalidUsagePeriodError('a closing allocation must not end before it starts')
  }
  return { ...dto, endAt }
}

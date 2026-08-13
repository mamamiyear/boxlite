/*
 * Copyright 2025 BoxLite AI
 * SPDX-License-Identifier: AGPL-3.0
 */

import { InvalidUsagePeriodError } from './usage-event'
import { OpenAllocation, toOpenAllocationDto } from './open-allocation'

const allocation = (overrides: Partial<OpenAllocation> = {}): OpenAllocation => ({
  organizationId: 'org-1',
  boxId: 'box-1',
  region: 'us',
  startAt: new Date('2026-08-01T00:00:00.000Z'),
  cpu: 2,
  gpu: 0,
  mem: 4,
  disk: 10,
  ...overrides,
})

describe('toOpenAllocationDto', () => {
  it('encodes an open allocation with no end and no event key', () => {
    expect(toOpenAllocationDto(allocation())).toEqual({
      organizationId: 'org-1',
      boxId: 'box-1',
      region: 'us',
      startAt: '2026-08-01T00:00:00.000Z',
      cpu: '2',
      gpu: '0',
      mem: '4',
      disk: '10',
    })
  })

  it('carries the real endAt for an allocation awaiting finalized delivery', () => {
    const closing = {
      ...allocation(),
      endAt: new Date('2026-08-01T01:00:00.000Z'),
    }

    expect(toOpenAllocationDto(closing)).toEqual(
      expect.objectContaining({
        startAt: '2026-08-01T00:00:00.000Z',
        endAt: '2026-08-01T01:00:00.000Z',
      }),
    )
  })

  // Snapshot quantities use the same fixed-point encoding and six-decimal
  // storage grid as finalized usage events.
  it.each<[number, string]>([
    [0, '0'],
    [2, '2'],
    [0.5, '0.5'],
    [0.000001, '0.000001'],
    [1.25, '1.25'],
  ])('encodes quantity %p as %p', (cpu, expected) => {
    expect(toOpenAllocationDto(allocation({ cpu })).cpu).toBe(expected)
  })

  it.each<[string, Partial<OpenAllocation>]>([
    ['a NaN quantity', { cpu: Number.NaN }],
    ['an infinite quantity', { mem: Number.POSITIVE_INFINITY }],
    ['a negative quantity', { disk: -1 }],
    ['a blank organizationId', { organizationId: '  ' }],
    ['a blank boxId', { boxId: '' }],
    ['a region longer than Commerce accepts', { region: 'r'.repeat(201) }],
    ['an invalid startAt', { startAt: new Date('nonsense') }],
    ['a quantity past the Commerce ceiling', { mem: 1_000_001 }],
    ['a quantity finer than the Commerce storage grid', { cpu: 0.0000001 }],
  ])('rejects %s', (_case, override) => {
    expect(() => toOpenAllocationDto(allocation(override))).toThrow(InvalidUsagePeriodError)
  })

  it('accepts quantities at the Commerce magnitude and resolution boundaries', () => {
    expect(toOpenAllocationDto(allocation({ mem: 1_000_000 })).mem).toBe('1000000')
    expect(toOpenAllocationDto(allocation({ cpu: 0.000001 })).cpu).toBe('0.000001')
  })

  // Zero is a real quantity — a box holding disk but with cpu momentarily
  // idle-billed at 0 — so it must remain distinct from invalid sub-grid values.
  it('still encodes a genuine zero', () => {
    expect(toOpenAllocationDto(allocation({ cpu: 0 })).cpu).toBe('0')
  })
})

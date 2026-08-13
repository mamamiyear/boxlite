/*
 * Copyright 2025 BoxLite AI
 * SPDX-License-Identifier: AGPL-3.0
 */

jest.mock('axios', () => ({
  __esModule: true,
  default: {
    post: jest.fn(),
    isAxiosError: (error: any) => Boolean(error?.isAxiosError),
  },
}))

jest.mock('timers/promises', () => ({
  setTimeout: jest.fn().mockResolvedValue(undefined),
}))

import { PassThrough, Readable } from 'node:stream'
import { setTimeout as sleep } from 'timers/promises'
import axios from 'axios'
import { BOX_WARM_POOL_UNASSIGNED_ORGANIZATION } from '../../box/constants/box.constants'
import { UsageExportStatus } from '../entities/box-usage-export-outbox.entity'
import { BoxUsagePeriod } from '../entities/box-usage-period.entity'
import { UsageAllocationSnapshotService } from './usage-allocation-snapshot.service'

const post = axios.post as jest.Mock
const retrySleep = sleep as jest.Mock
const OBSERVED_AT = new Date('2026-08-01T02:00:00.000Z')
const SNAPSHOT_SCHEMA_VERSION = 3
const MAX_CHUNK_ALLOCATIONS = 1_000
const MAX_CHUNK_BYTES = 8 * 1024 * 1024

const CONFIG: Record<string, unknown> = {
  'usageExport.allocationSnapshotEnabled': true,
  'usageExport.url': 'https://commerce.test',
  'usageExport.token': 'tok-1',
  'usageExport.timeoutMs': 10_000,
}

const openPeriod = (overrides: Partial<BoxUsagePeriod> = {}): BoxUsagePeriod =>
  Object.assign(new BoxUsagePeriod(), {
    id: 'period-1',
    organizationId: 'org-1',
    boxId: 'box-1',
    region: 'us',
    startAt: new Date('2026-08-01T00:00:00.000Z'),
    endAt: null,
    cpu: 2,
    gpu: 0,
    mem: 4,
    disk: 10,
    ...overrides,
  })

const observedRows = (periods: BoxUsagePeriod[]) =>
  periods.length > 0
    ? periods.map((period) => ({
        asOf: OBSERVED_AT,
        allocation: {
          ...period,
          startAt: period.startAt instanceof Date ? period.startAt.toISOString() : period.startAt,
          endAt: period.endAt instanceof Date ? period.endAt.toISOString() : period.endAt,
        },
      }))
    : [
        {
          asOf: OBSERVED_AT,
          allocation: null,
        },
      ]

const allocationRows = (periods: BoxUsagePeriod[]) =>
  observedRows(periods).flatMap((row) => (row.allocation === null ? [] : [{ allocation: row.allocation }]))

const makeService = (
  livePeriods: BoxUsagePeriod[],
  overrides: Record<string, unknown> = {},
  undeliveredFinalizedPeriods: BoxUsagePeriod[] = [],
) => {
  const queryRunner = {
    connect: jest.fn().mockResolvedValue(undefined),
    startTransaction: jest.fn().mockResolvedValue(undefined),
    query: jest.fn(async (sql: string) => {
      if (/SET TRANSACTION READ ONLY/i.test(sql)) {
        return []
      }
      if (/statement_timestamp\(\)/i.test(sql)) {
        return [{ asOf: OBSERVED_AT }]
      }
      throw new Error(`usage-allocation-snapshot.service.spec: unexpected QueryRunner SQL ${sql}`)
    }),
    stream: jest.fn(async (sql: string, parameters?: unknown[]) => {
      void parameters
      if (sql.includes('"box_usage_periods"')) {
        return Readable.from(allocationRows(livePeriods))
      }
      if (sql.includes('"box_usage_export_outbox"')) {
        return Readable.from(allocationRows(undeliveredFinalizedPeriods))
      }
      throw new Error(`usage-allocation-snapshot.service.spec: unexpected stream SQL ${sql}`)
    }),
    rollbackTransaction: jest.fn().mockResolvedValue(undefined),
    release: jest.fn().mockResolvedValue(undefined),
  }
  const boxUsagePeriodRepository = {
    find: jest.fn().mockResolvedValue(livePeriods),
    query: jest.fn().mockResolvedValue(observedRows([...livePeriods, ...undeliveredFinalizedPeriods])),
    manager: {
      connection: {
        createQueryRunner: jest.fn().mockReturnValue(queryRunner),
      },
    },
  }
  const leaseController = new AbortController()
  const lease = {
    signal: leaseController.signal,
    release: jest.fn().mockResolvedValue(undefined),
  }
  const redisLockProvider = {
    lock: jest.fn().mockResolvedValue(true),
    unlock: jest.fn().mockResolvedValue(undefined),
    acquireLease: jest.fn().mockResolvedValue(lease),
  }
  const configService = {
    get: jest.fn((key: string) => {
      const settings = { ...CONFIG, ...overrides }
      if (!(key in settings)) {
        throw new Error(`usage-allocation-snapshot.service.spec: unexpected config key "${key}"`)
      }
      return settings[key]
    }),
  }

  const service = new UsageAllocationSnapshotService(
    boxUsagePeriodRepository as any,
    redisLockProvider as any,
    configService as any,
  )

  return { service, boxUsagePeriodRepository, redisLockProvider, queryRunner, lease, leaseController }
}

beforeEach(() => {
  post.mockReset()
  retrySleep.mockClear()
})

const acknowledgeChunks = () =>
  post.mockImplementation(async (_url: string, body: Record<string, any>) => ({
    status: 200,
    data: {
      accepted: body.allocations.length,
      asOf: body.asOf,
      complete: body.chunkIndex === body.chunkCount - 1,
      applied: body.chunkIndex === body.chunkCount - 1,
    },
  }))

const axiosFailure = (status: number | undefined, message: string) => ({
  isAxiosError: true,
  message,
  response: status === undefined ? undefined : { status },
})

describe('UsageAllocationSnapshotService.snapshotOpenAllocations', () => {
  it('does nothing while the snapshot is disabled', async () => {
    const { service, boxUsagePeriodRepository } = makeService([openPeriod()], {
      'usageExport.allocationSnapshotEnabled': false,
    })

    await service.snapshotOpenAllocations()

    expect(boxUsagePeriodRepository.manager.connection.createQueryRunner).not.toHaveBeenCalled()
    expect(post).not.toHaveBeenCalled()
  })

  // A generation can span many bounded requests, so its lock must renew instead
  // of expiring after the timeout of just its first chunk.
  it.each([
    [10_000, 40],
    [45_000, 75],
  ])(
    'leases for the configured POST timeout plus a renewal margin (timeout %ims -> %is)',
    async (timeoutMs, expectedTtl) => {
      const { service, redisLockProvider, lease } = makeService([openPeriod()], {
        'usageExport.timeoutMs': timeoutMs,
      })
      acknowledgeChunks()

      await service.snapshotOpenAllocations()

      expect(redisLockProvider.acquireLease).toHaveBeenCalledWith('snapshot-open-allocations', expectedTtl)
      expect(lease.release).toHaveBeenCalledTimes(1)
    },
  )

  it('yields to whichever replica holds the lock', async () => {
    const { service, boxUsagePeriodRepository, redisLockProvider } = makeService([openPeriod()])
    redisLockProvider.acquireLease.mockResolvedValue(null)

    await service.snapshotOpenAllocations()

    expect(boxUsagePeriodRepository.manager.connection.createQueryRunner).not.toHaveBeenCalled()
    expect(post).not.toHaveBeenCalled()
  })

  it('validates then streams both allocation sources from one read-only repeatable-read observation', async () => {
    const closing = openPeriod({ boxId: 'closing', endAt: new Date('2026-08-01T01:00:00.000Z') })
    const { service, boxUsagePeriodRepository, queryRunner } = makeService([openPeriod()], {}, [closing])
    acknowledgeChunks()

    await service.snapshotOpenAllocations()

    expect(boxUsagePeriodRepository.query).not.toHaveBeenCalled()
    expect(queryRunner.connect).toHaveBeenCalledTimes(1)
    expect(queryRunner.startTransaction).toHaveBeenCalledWith('REPEATABLE READ')
    expect(queryRunner.query.mock.calls.some(([sql]) => /SET TRANSACTION READ ONLY/i.test(sql))).toBe(true)

    const clockCall = queryRunner.query.mock.calls.find(([sql]) => /statement_timestamp\(\)/i.test(sql))
    expect(clockCall?.[0]).toContain('statement_timestamp()')

    // Pass one validates/counts without retaining rows; pass two sends the
    // exact same MVCC snapshot in bounded chunks.
    expect(queryRunner.stream).toHaveBeenCalledTimes(4)
    const [liveSql, liveParameters] = queryRunner.stream.mock.calls[0]
    const [outboxSql, outboxParameters] = queryRunner.stream.mock.calls[1]
    expect(liveSql).toContain('"box_usage_periods"')
    expect(outboxSql).toContain('"box_usage_export_outbox"')
    expect(outboxSql).toContain('outbox."status" <> $2')
    expect(liveParameters).toEqual([BOX_WARM_POOL_UNASSIGNED_ORGANIZATION])
    expect(outboxParameters).toEqual([BOX_WARM_POOL_UNASSIGNED_ORGANIZATION, UsageExportStatus.DELIVERED])
    expect(queryRunner.stream.mock.calls[2]).toEqual(queryRunner.stream.mock.calls[0])
    expect(queryRunner.stream.mock.calls[3]).toEqual(queryRunner.stream.mock.calls[1])
    expect(queryRunner.rollbackTransaction).toHaveBeenCalledTimes(1)
    expect(queryRunner.release).toHaveBeenCalledTimes(1)
  })

  it('keeps a valid blocked allocation until Commerce acknowledges delivery', async () => {
    const blocked = openPeriod({ endAt: new Date('2026-08-01T01:00:00.000Z') })
    const { service, queryRunner } = makeService([], {}, [blocked])
    acknowledgeChunks()

    await service.snapshotOpenAllocations()

    const [sql] = queryRunner.stream.mock.calls[1]
    const [, body] = post.mock.calls[0]
    expect(sql).toContain('outbox."status" <> $2')
    expect(body.allocations).toEqual([
      expect.objectContaining({
        boxId: 'box-1',
        startAt: '2026-08-01T00:00:00.000Z',
        endAt: '2026-08-01T01:00:00.000Z',
      }),
    ])
  })

  it('counts only encodable rows and isolates one malformed allocation without tearing metadata', async () => {
    const malformedBlocked = {
      ...openPeriod({ boxId: 'bad' }),
      startAt: 'not-a-timestamp',
      endAt: 'not-a-timestamp',
    } as unknown as BoxUsagePeriod
    const { service, queryRunner } = makeService([], {}, [malformedBlocked, openPeriod({ boxId: 'good' })])
    acknowledgeChunks()

    await expect(service.snapshotOpenAllocations()).resolves.toBeUndefined()

    const [, body] = post.mock.calls[0]
    expect(body).toMatchObject({ allocationCount: 1, chunkCount: 1 })
    expect(body.allocations).toEqual([expect.objectContaining({ boxId: 'good' })])
    expect(queryRunner.stream).toHaveBeenCalledTimes(4)
    expect(queryRunner.rollbackTransaction).toHaveBeenCalledTimes(1)
    expect(queryRunner.release).toHaveBeenCalledTimes(1)
  })

  it('streams the complete set in deterministic bounded v3 chunks without truncation', async () => {
    const live = Array.from({ length: 600 }, (_, index) => openPeriod({ boxId: `live-${index}` }))
    const closing = Array.from({ length: 401 }, (_, index) =>
      openPeriod({
        boxId: `closing-${index}`,
        endAt: new Date('2026-08-01T01:00:00.000Z'),
      }),
    )
    const { service, queryRunner } = makeService(live, {}, closing)
    acknowledgeChunks()

    await service.snapshotOpenAllocations()

    expect(queryRunner.stream).toHaveBeenCalledTimes(4)
    const bodies = post.mock.calls.map(([, body]) => body)
    expect(bodies).toHaveLength(2)
    expect(bodies.map((body) => body.allocations.length)).toEqual([MAX_CHUNK_ALLOCATIONS, 1])
    expect(bodies.flatMap((body) => body.allocations)).toHaveLength(1_001)

    const [first, second] = bodies
    expect(first).toMatchObject({
      schemaVersion: SNAPSHOT_SCHEMA_VERSION,
      asOf: OBSERVED_AT.toISOString(),
      chunkIndex: 0,
      chunkCount: 2,
      allocationCount: 1_001,
    })
    expect(first.generationId).toMatch(/^[0-9a-f]{64}$/)
    expect(second).toMatchObject({
      schemaVersion: SNAPSHOT_SCHEMA_VERSION,
      generationId: first.generationId,
      asOf: first.asOf,
      chunkIndex: 1,
      chunkCount: 2,
      allocationCount: 1_001,
    })
    for (const body of bodies) {
      expect(body.allocations.length).toBeLessThanOrEqual(MAX_CHUNK_ALLOCATIONS)
      expect(Buffer.byteLength(JSON.stringify(body))).toBeLessThan(MAX_CHUNK_BYTES)
    }
  })

  it('keeps a closing allocation beside its reopened successor until finalized delivery', async () => {
    const closedAt = new Date('2026-08-01T01:00:00.000Z')
    const closing = openPeriod({ endAt: closedAt })
    const reopened = openPeriod({ id: 'period-2', startAt: closedAt })
    const { service } = makeService([reopened], {}, [closing])
    acknowledgeChunks()

    await service.snapshotOpenAllocations()

    const [, body] = post.mock.calls[0]
    expect(body).toMatchObject({
      schemaVersion: SNAPSHOT_SCHEMA_VERSION,
      generationId: expect.stringMatching(/^[0-9a-f]{64}$/),
      asOf: OBSERVED_AT.toISOString(),
      chunkIndex: 0,
      chunkCount: 1,
      allocationCount: 2,
    })
    expect(body.allocations).toHaveLength(2)
    expect(body.allocations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          boxId: 'box-1',
          startAt: '2026-08-01T00:00:00.000Z',
          endAt: closedAt.toISOString(),
        }),
        expect.objectContaining({
          boxId: 'box-1',
          startAt: closedAt.toISOString(),
        }),
      ]),
    )
    expect(body.allocations.find((allocation: Record<string, unknown>) => allocation.startAt === closedAt.toISOString()))
      .not.toHaveProperty('endAt')
  })

  it('excludes zero-duration identities consistently from count and both streams', async () => {
    const instant = new Date('2026-08-01T01:00:00.000Z')
    const successor = openPeriod({ id: 'period-2', startAt: instant })
    const { service, queryRunner } = makeService([successor])
    acknowledgeChunks()

    await service.snapshotOpenAllocations()

    const [liveSql] = queryRunner.stream.mock.calls[0]
    const [outboxSql] = queryRunner.stream.mock.calls[1]
    const [, body] = post.mock.calls[0]
    expect(liveSql).toContain('period."endAt" <> period."startAt"')
    expect(outboxSql).toContain("outbox.\"payload\"->>'endAt' <> outbox.\"payload\"->>'startAt'")
    expect(body.allocations).toEqual([
      expect.objectContaining({ boxId: 'box-1', startAt: instant.toISOString() }),
    ])
    expect(body.allocations[0]).not.toHaveProperty('endAt')
  })

  it('defensively omits a semantic zero-duration row without counting it', async () => {
    const instant = new Date('2026-08-01T01:00:00.000Z')
    const escapedZero = openPeriod({
      startAt: instant,
      endAt: '2026-07-31T21:00:00.000-04:00' as unknown as Date,
    })
    const successor = openPeriod({ id: 'period-2', startAt: instant })
    const { service, queryRunner } = makeService([successor], {}, [escapedZero])
    acknowledgeChunks()

    await expect(service.snapshotOpenAllocations()).resolves.toBeUndefined()

    const [, body] = post.mock.calls[0]
    expect(body).toMatchObject({ allocationCount: 1, chunkCount: 1 })
    expect(body.allocations).toEqual([
      expect.objectContaining({ boxId: 'box-1', startAt: instant.toISOString() }),
    ])
    expect(body.allocations[0]).not.toHaveProperty('endAt')
    expect(queryRunner.rollbackTransaction).toHaveBeenCalledTimes(1)
    expect(queryRunner.release).toHaveBeenCalledTimes(1)
  })

  it('posts v3 chunks on the rollout-safe route with the service token, timeout, and lease signal', async () => {
    const { service } = makeService([openPeriod(), openPeriod({ boxId: 'box-2' })])
    acknowledgeChunks()

    await service.snapshotOpenAllocations()

    expect(post).toHaveBeenCalledWith(
      'https://commerce.test/internal/allocation-snapshot-chunks',
      expect.objectContaining({
        schemaVersion: SNAPSHOT_SCHEMA_VERSION,
        generationId: expect.stringMatching(/^[0-9a-f]{64}$/),
        asOf: expect.any(String),
        chunkIndex: 0,
        chunkCount: 1,
        allocationCount: 2,
        allocations: [expect.objectContaining({ boxId: 'box-1' }), expect.objectContaining({ boxId: 'box-2' })],
      }),
      expect.objectContaining({
        timeout: 10_000,
        headers: expect.objectContaining({ authorization: 'Bearer tok-1' }),
        signal: expect.anything(),
      }),
    )
  })

  // Advancing the consumer's asOf watermark when every box has stopped requires
  // sending the empty set, not skipping the push.
  it('still pushes an empty snapshot when no box is open', async () => {
    const { service } = makeService([])
    acknowledgeChunks()

    await service.snapshotOpenAllocations()

    expect(post).toHaveBeenCalledWith(
      'https://commerce.test/internal/allocation-snapshot-chunks',
      expect.objectContaining({
        schemaVersion: SNAPSHOT_SCHEMA_VERSION,
        chunkIndex: 0,
        chunkCount: 1,
        allocationCount: 0,
        allocations: [],
      }),
      expect.anything(),
    )
  })

  it.each([
    ['a network timeout', undefined],
    ['HTTP 503', 503],
  ])('retries %s with the exact same frozen chunk', async (_label, status) => {
    const { service, lease } = makeService([openPeriod()])
    post
      .mockRejectedValueOnce(axiosFailure(status, 'transient'))
      .mockImplementationOnce(async (_url: string, body: Record<string, any>) => ({
        status: 200,
        data: {
          accepted: body.allocations.length,
          asOf: body.asOf,
          complete: true,
          applied: true,
        },
      }))

    await expect(service.snapshotOpenAllocations()).resolves.toBeUndefined()

    expect(post).toHaveBeenCalledTimes(2)
    expect(post.mock.calls[1][1]).toEqual(post.mock.calls[0][1])
    expect(retrySleep.mock.calls[0].slice(0, 2)).toEqual([250, undefined])
    expect(retrySleep.mock.calls[0][2].signal).toBe(post.mock.calls[0][2].signal)
    expect(retrySleep.mock.calls[0][2].signal).not.toBe(lease.signal)
  })

  it('retries a lost final ACK without changing its generation or partition', async () => {
    const periods = Array.from({ length: 1_001 }, (_, index) => openPeriod({ boxId: `box-${index}` }))
    const { service } = makeService(periods)
    post
      .mockImplementationOnce(async (_url: string, body: Record<string, any>) => ({
        status: 200,
        data: { accepted: body.allocations.length, asOf: body.asOf, complete: false, applied: false },
      }))
      .mockRejectedValueOnce(axiosFailure(undefined, 'response lost'))
      .mockImplementationOnce(async (_url: string, body: Record<string, any>) => ({
        status: 200,
        data: { accepted: body.allocations.length, asOf: body.asOf, complete: true, applied: false },
      }))

    await expect(service.snapshotOpenAllocations()).resolves.toBeUndefined()

    expect(post).toHaveBeenCalledTimes(3)
    const first = post.mock.calls[0][1]
    const lostFinal = post.mock.calls[1][1]
    const retriedFinal = post.mock.calls[2][1]
    expect(first.chunkIndex).toBe(0)
    expect(lostFinal.chunkIndex).toBe(1)
    expect(retriedFinal).toEqual(lostFinal)
    expect(retriedFinal.generationId).toBe(first.generationId)
    expect(retriedFinal.chunkCount).toBe(first.chunkCount)
    expect(retriedFinal.allocationCount).toBe(first.allocationCount)
  })

  it('bounds transient attempts and applies explicit backoff', async () => {
    const { service, lease } = makeService([openPeriod()])
    post.mockRejectedValue(axiosFailure(503, 'unavailable'))

    await expect(service.snapshotOpenAllocations()).resolves.toBeUndefined()

    expect(post).toHaveBeenCalledTimes(3)
    expect(post.mock.calls[1][1]).toEqual(post.mock.calls[0][1])
    expect(post.mock.calls[2][1]).toEqual(post.mock.calls[0][1])
    expect(retrySleep.mock.calls.map(([delay]) => delay)).toEqual([250, 1_000])
    expect(retrySleep.mock.calls[0][2].signal).toBe(post.mock.calls[0][2].signal)
    expect(retrySleep.mock.calls[1][2].signal).toBe(post.mock.calls[0][2].signal)
    expect(retrySleep.mock.calls[0][2].signal).not.toBe(lease.signal)
  })

  it('never sends the last chunk when the validation and delivery passes disagree', async () => {
    const first = openPeriod({ boxId: 'first' })
    const extra = openPeriod({ boxId: 'extra' })
    const { service, queryRunner } = makeService([first])
    let streamCall = 0
    queryRunner.stream.mockImplementation(async (sql: string, parameters?: unknown[]) => {
      void parameters
      const pass = Math.floor(streamCall++ / 2)
      if (sql.includes('"box_usage_periods"')) {
        return Readable.from(allocationRows(pass === 0 ? [first] : [first, extra]))
      }
      return Readable.from([])
    })
    acknowledgeChunks()

    await expect(service.snapshotOpenAllocations()).rejects.toThrow('changed during one repeatable-read observation')

    expect(post).not.toHaveBeenCalled()
    expect(queryRunner.rollbackTransaction).toHaveBeenCalledTimes(1)
    expect(queryRunner.release).toHaveBeenCalledTimes(1)
  })

  it.each([400, 409])('does not retry permanent HTTP %i', async (status) => {
    const { service } = makeService([openPeriod()])
    post.mockRejectedValue(axiosFailure(status, 'permanent'))

    await expect(service.snapshotOpenAllocations()).resolves.toBeUndefined()

    expect(post).toHaveBeenCalledTimes(1)
    expect(retrySleep).not.toHaveBeenCalled()
  })

  it('rejects an impossible complete=false/applied=true ACK', async () => {
    const { service } = makeService([openPeriod()])
    post.mockImplementation(async (_url: string, body: Record<string, any>) => ({
      status: 200,
      data: { accepted: body.allocations.length, asOf: body.asOf, complete: false, applied: true },
    }))

    await expect(service.snapshotOpenAllocations()).resolves.toBeUndefined()

    expect(post).toHaveBeenCalledTimes(1)
    expect(retrySleep).not.toHaveBeenCalled()
  })

  it('rejects a same-generation complete ACK before the final sequential chunk', async () => {
    const periods = Array.from({ length: 1_001 }, (_, index) => openPeriod({ boxId: `box-${index}` }))
    const { service } = makeService(periods)
    const successLog = jest.spyOn((service as any).logger, 'log')
    const errorLog = jest.spyOn((service as any).logger, 'error')
    post.mockImplementationOnce(async (_url: string, body: Record<string, any>) => ({
      status: 200,
      data: { accepted: body.allocations.length, asOf: body.asOf, complete: true, applied: false },
    }))

    await expect(service.snapshotOpenAllocations()).resolves.toBeUndefined()

    expect(post).toHaveBeenCalledTimes(1)
    expect(successLog).not.toHaveBeenCalledWith(expect.stringContaining('Pushed allocation snapshot generation'))
    expect(errorLog).toHaveBeenCalledWith(expect.stringContaining('before its final sequential chunk'))
  })

  it('application shutdown aborts a database stream and releases both observation and lease', async () => {
    const { service, queryRunner, lease, leaseController } = makeService([openPeriod()])
    const pendingStream = new PassThrough({ objectMode: true })
    let streamStarted!: () => void
    const started = new Promise<void>((resolve) => {
      streamStarted = resolve
    })
    queryRunner.stream.mockImplementationOnce(async () => {
      streamStarted()
      return pendingStream
    })

    const snapshot = service.snapshotOpenAllocations().catch((error) => error)
    await started
    const shutdown = service.onApplicationShutdown()
    await Promise.resolve()
    const abortedByShutdown = pendingStream.destroyed
    if (!abortedByShutdown) leaseController.abort(new Error('test cleanup'))

    await expect(snapshot).resolves.toEqual(expect.objectContaining({ message: 'UsageAllocationSnapshotService is shutting down' }))
    await shutdown
    expect(abortedByShutdown).toBe(true)
    expect(queryRunner.rollbackTransaction).toHaveBeenCalledTimes(1)
    expect(queryRunner.release).toHaveBeenCalledTimes(1)
    expect(lease.release).toHaveBeenCalledTimes(1)
  })

  it('application shutdown aborts an in-flight Commerce request and releases both observation and lease', async () => {
    const { service, queryRunner, lease, leaseController } = makeService([openPeriod()])
    let requestSignal: AbortSignal | undefined
    let requestStarted!: () => void
    const started = new Promise<void>((resolve) => {
      requestStarted = resolve
    })
    post.mockImplementation(
      (_url: string, _body: unknown, options: { signal: AbortSignal }) =>
        new Promise((_resolve, reject) => {
          requestSignal = options.signal
          options.signal.addEventListener('abort', () => reject(options.signal.reason), { once: true })
          requestStarted()
        }),
    )

    const snapshot = service.snapshotOpenAllocations().catch((error) => error)
    await started
    const shutdown = service.onApplicationShutdown()
    await Promise.resolve()
    const abortedByShutdown = requestSignal?.aborted === true
    if (!abortedByShutdown) leaseController.abort(new Error('test cleanup'))

    await expect(snapshot).resolves.toEqual(expect.objectContaining({ message: 'UsageAllocationSnapshotService is shutting down' }))
    await shutdown
    expect(abortedByShutdown).toBe(true)
    expect(queryRunner.rollbackTransaction).toHaveBeenCalledTimes(1)
    expect(queryRunner.release).toHaveBeenCalledTimes(1)
    expect(lease.release).toHaveBeenCalledTimes(1)
  })

  it('application shutdown aborts retry backoff and releases both observation and lease', async () => {
    const { service, queryRunner, lease, leaseController } = makeService([openPeriod()])
    let retrySignal: AbortSignal | undefined
    let retryStarted!: () => void
    const started = new Promise<void>((resolve) => {
      retryStarted = resolve
    })
    post.mockRejectedValue(axiosFailure(503, 'unavailable'))
    retrySleep.mockImplementationOnce(
      (_delay: number, _value: undefined, options: { signal: AbortSignal }) =>
        new Promise((_resolve, reject) => {
          retrySignal = options.signal
          options.signal.addEventListener('abort', () => reject(options.signal.reason), { once: true })
          retryStarted()
        }),
    )

    const snapshot = service.snapshotOpenAllocations().catch((error) => error)
    await started
    const shutdown = service.onApplicationShutdown()
    await Promise.resolve()
    const abortedByShutdown = retrySignal?.aborted === true
    if (!abortedByShutdown) leaseController.abort(new Error('test cleanup'))

    await expect(snapshot).resolves.toEqual(expect.objectContaining({ message: 'UsageAllocationSnapshotService is shutting down' }))
    await shutdown
    expect(abortedByShutdown).toBe(true)
    expect(queryRunner.rollbackTransaction).toHaveBeenCalledTimes(1)
    expect(queryRunner.release).toHaveBeenCalledTimes(1)
    expect(lease.release).toHaveBeenCalledTimes(1)
  })

  it('rolls back and releases the database observation and Redis lease when observation setup throws', async () => {
    const { service, queryRunner, lease } = makeService([openPeriod()])
    queryRunner.query.mockImplementation(async (sql: string) => {
      if (/SET TRANSACTION READ ONLY/i.test(sql)) return []
      throw new Error('database is down')
    })

    await expect(service.snapshotOpenAllocations()).rejects.toThrow('database is down')
    expect(queryRunner.rollbackTransaction).toHaveBeenCalledTimes(1)
    expect(queryRunner.release).toHaveBeenCalledTimes(1)
    expect(lease.release).toHaveBeenCalledTimes(1)
  })
})

/*
 * Copyright 2025 BoxLite AI
 * SPDX-License-Identifier: AGPL-3.0
 */

import { randomBytes } from 'node:crypto'
import { Readable } from 'node:stream'
import { setTimeout as sleep } from 'timers/promises'
import { Injectable, Logger, OnApplicationShutdown } from '@nestjs/common'
import { Cron, CronExpression } from '@nestjs/schedule'
import { InjectRepository } from '@nestjs/typeorm'
import axios from 'axios'
import { QueryRunner, Repository } from 'typeorm'
import { BOX_WARM_POOL_UNASSIGNED_ORGANIZATION } from '../../box/constants/box.constants'
import { RedisLockProvider, withRedisLockLease } from '../../box/common/redis-lock.provider'
import { LogExecution } from '../../common/decorators/log-execution.decorator'
import { WithInstrumentation } from '../../common/decorators/otel.decorator'
import { TrackJobExecution } from '../../common/decorators/track-job-execution.decorator'
import { TrackableJobExecutions } from '../../common/interfaces/trackable-job-executions'
import { TypedConfigService } from '../../config/typed-config.service'
import { UsageExportStatus } from '../entities/box-usage-export-outbox.entity'
import { BoxUsagePeriod } from '../entities/box-usage-period.entity'
import { InvalidUsagePeriodError, timestampString } from '../usage-event'
import { OpenAllocation, OpenAllocationDto, toOpenAllocationDto } from '../open-allocation'

const SNAPSHOT_LOCK_KEY = 'snapshot-open-allocations'
const SNAPSHOT_SCHEMA_VERSION = 3
const MAX_CHUNK_ALLOCATIONS = 1_000
const MAX_CHUNK_BYTES = 8 * 1024 * 1024
const RETRY_DELAYS_MS = [250, 1_000] as const

/**
 * Safety margin the lock lease keeps over the configured POST timeout.
 *
 * The renewable lease has to outlive each bounded POST/renewal operation. A
 * generation can contain any number of chunks, so a fixed lease for the whole
 * job would either expire mid-generation or grow without a defensible bound.
 */
const SNAPSHOT_LOCK_MARGIN_MS = 30_000

interface SnapshotClockRow {
  asOf: Date
}

interface SnapshotAllocationRow {
  allocation: Record<string, unknown>
}

interface AllocationSnapshotChunk {
  schemaVersion: typeof SNAPSHOT_SCHEMA_VERSION
  generationId: string
  asOf: string
  chunkIndex: number
  chunkCount: number
  allocationCount: number
  allocations: OpenAllocationDto[]
}

interface AllocationSnapshotAck {
  accepted: number
  asOf: string
  complete: boolean
  applied: boolean
}

class SnapshotDeliveryStoppedError extends Error {}

const CANONICAL_QUANTITY = /^(?:0|[1-9]\d*)(?:\.\d+)?$/

const LIVE_ALLOCATIONS_SQL = `
  SELECT to_jsonb(period) - 'id' AS "allocation"
  FROM "box_usage_periods" period
  WHERE period."organizationId" <> $1
    AND (period."endAt" IS NULL OR period."endAt" <> period."startAt")
  ORDER BY period."organizationId", period."boxId", period."startAt", period."endAt" NULLS LAST
`

const UNDELIVERED_ALLOCATIONS_SQL = `
  SELECT outbox."payload" AS "allocation"
  FROM "box_usage_export_outbox" outbox
  WHERE outbox."status" <> $2
    AND COALESCE(outbox."payload"->>'organizationId', '') <> $1
    AND (
      outbox."payload"->>'endAt' IS NULL
      OR outbox."payload"->>'endAt' <> outbox."payload"->>'startAt'
    )
  ORDER BY
    outbox."payload"->>'organizationId',
    outbox."payload"->>'boxId',
    outbox."payload"->>'startAt',
    outbox."payload"->>'endAt'
`

/**
 * Pushes every allocation not yet acknowledged as finalized by Commerce as one
 * atomic replace-all generation, on a fixed interval.
 *
 * A closing period remains here with its real endAt while its finalized event
 * is not delivered, so independent snapshot and publisher crons cannot make
 * usage disappear between close and acknowledgement. One repeatable-read
 * observation is validated once, then streamed again into bounded chunks; the
 * consumer publishes it only after every chunk is present. A failed generation
 * remains incomplete there and the next tick sends a new observation. Sending
 * an empty generation is deliberate, because it advances the consumer's asOf
 * watermark when every allocation has been finalized.
 */
@Injectable()
export class UsageAllocationSnapshotService implements TrackableJobExecutions, OnApplicationShutdown {
  activeJobs = new Set<string>()
  private readonly logger = new Logger(UsageAllocationSnapshotService.name)
  private readonly shutdownController = new AbortController()

  constructor(
    @InjectRepository(BoxUsagePeriod)
    private readonly boxUsagePeriodRepository: Repository<BoxUsagePeriod>,
    private readonly redisLockProvider: RedisLockProvider,
    private readonly configService: TypedConfigService,
  ) {}

  async onApplicationShutdown(): Promise<void> {
    this.shutdownController.abort(new Error('UsageAllocationSnapshotService is shutting down'))
    while (this.activeJobs.size > 0) {
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }
  @Cron(CronExpression.EVERY_5_MINUTES, { name: SNAPSHOT_LOCK_KEY })
  @TrackJobExecution()
  @LogExecution(SNAPSHOT_LOCK_KEY)
  @WithInstrumentation()
  async snapshotOpenAllocations(): Promise<void> {
    this.shutdownController.signal.throwIfAborted()
    if (!this.configService.get('usageExport.allocationSnapshotEnabled')) {
      return
    }
    const lockTtlSeconds = Math.ceil((this.configService.get('usageExport.timeoutMs') + SNAPSHOT_LOCK_MARGIN_MS) / 1000)
    const lease = await this.redisLockProvider.acquireLease(SNAPSHOT_LOCK_KEY, lockTtlSeconds)
    if (!lease) {
      return
    }

    await withRedisLockLease(
      lease,
      (leaseSignal) =>
        this.publishObservedGeneration(AbortSignal.any([leaseSignal, this.shutdownController.signal])),
      (error) => {
        this.logger.error(`Failed to release allocation snapshot lease: ${this.describe(error)}`)
      },
    )
  }

  /**
   * Reads live periods and undelivered finalized events through one PostgreSQL
   * repeatable-read snapshot.
   *
   * Archival deletes the live row and inserts its outbox row atomically, so one
   * observation sees the closing allocation on exactly one side. The first
   * stream validates and counts without retaining rows; the second streams the
   * same rows into bounded chunks. Claims leave rows pending, and even an
   * exhausted row remains visible while blocked; only Commerce acknowledgement
   * moves it to delivered and removes it from this bridge.
   */
  private async publishObservedGeneration(signal: AbortSignal): Promise<void> {
    const queryRunner = this.boxUsagePeriodRepository.manager.connection.createQueryRunner()
    let transactionStarted = false
    try {
      await queryRunner.connect()
      await queryRunner.startTransaction('REPEATABLE READ')
      transactionStarted = true
      await queryRunner.query('SET TRANSACTION READ ONLY')
      signal.throwIfAborted()

      const asOf = await this.observeAsOf(queryRunner)
      const allocationCount = await this.countEncodableAllocations(queryRunner, signal)
      const chunkCount = Math.max(1, Math.ceil(allocationCount / MAX_CHUNK_ALLOCATIONS))
      const generationId = randomBytes(32).toString('hex')

      const delivered = await this.streamGeneration(
        queryRunner,
        { asOf, allocationCount, chunkCount, generationId },
        signal,
      )
      if (delivered) {
        this.logger.log(
          `Pushed allocation snapshot generation ${generationId} with ${allocationCount} allocation(s) in ${chunkCount} chunk(s), as of ${asOf}`,
        )
      }
    } finally {
      try {
        if (transactionStarted) {
          await queryRunner.rollbackTransaction()
        }
      } finally {
        await queryRunner.release()
      }
    }
  }

  private async observeAsOf(queryRunner: QueryRunner): Promise<string> {
    const rows = (await queryRunner.query('SELECT statement_timestamp() AS "asOf"')) as SnapshotClockRow[]
    const first = rows[0]
    if (!first) {
      throw new Error('allocation snapshot observation returned no database timestamp')
    }
    return timestampString(first.asOf, 'asOf')
  }

  private async countEncodableAllocations(queryRunner: QueryRunner, signal: AbortSignal): Promise<number> {
    let allocationCount = 0
    await this.streamAllocationPayloads(queryRunner, signal, async (payload) => {
      if (this.encode(payload, true) === null) {
        return
      }
      allocationCount += 1
      if (!Number.isSafeInteger(allocationCount)) {
        throw new Error('allocation snapshot count exceeds the JavaScript safe-integer range')
      }
    })
    return allocationCount
  }

  private encode(payload: Record<string, unknown>, reportInvalid: boolean): OpenAllocationDto | null {
    try {
      const allocation = this.decode(payload)
      if (allocation.endAt?.getTime() === allocation.startAt.getTime()) {
        return null
      }
      return toOpenAllocationDto(allocation)
    } catch (error) {
      if (!(error instanceof InvalidUsagePeriodError)) {
        throw error
      }
      if (reportInvalid) {
        const boxId = typeof payload.boxId === 'string' ? payload.boxId : '<unknown>'
        this.logger.error(`Skipping unfinalized allocation for box ${boxId}: ${error.message}`)
      }
      return null
    }
  }

  private async streamAllocationPayloads(
    queryRunner: QueryRunner,
    signal: AbortSignal,
    visit: (payload: Record<string, unknown>) => Promise<void>,
  ): Promise<void> {
    await this.consumeAllocationStream(
      queryRunner,
      LIVE_ALLOCATIONS_SQL,
      [BOX_WARM_POOL_UNASSIGNED_ORGANIZATION],
      signal,
      visit,
    )
    await this.consumeAllocationStream(
      queryRunner,
      UNDELIVERED_ALLOCATIONS_SQL,
      [BOX_WARM_POOL_UNASSIGNED_ORGANIZATION, UsageExportStatus.DELIVERED],
      signal,
      visit,
    )
  }

  private async consumeAllocationStream(
    queryRunner: QueryRunner,
    sql: string,
    parameters: unknown[],
    signal: AbortSignal,
    visit: (payload: Record<string, unknown>) => Promise<void>,
  ): Promise<void> {
    signal.throwIfAborted()
    const stream = (await queryRunner.stream(sql, parameters)) as Readable
    const abort = () => stream.destroy()
    if (signal.aborted) {
      abort()
      signal.throwIfAborted()
    }
    signal.addEventListener('abort', abort, { once: true })
    try {
      for await (const value of stream) {
        signal.throwIfAborted()
        const row = value as Partial<SnapshotAllocationRow>
        if (typeof row.allocation !== 'object' || row.allocation === null || Array.isArray(row.allocation)) {
          throw new Error('allocation snapshot stream returned a malformed database row')
        }
        await visit(row.allocation)
      }
      signal.throwIfAborted()
    } finally {
      signal.removeEventListener('abort', abort)
    }
  }

  private async streamGeneration(
    queryRunner: QueryRunner,
    metadata: Omit<AllocationSnapshotChunk, 'schemaVersion' | 'chunkIndex' | 'allocations'>,
    signal: AbortSignal,
  ): Promise<boolean> {
    if (metadata.allocationCount === 0) {
      const chunk = this.chunk(metadata, 0, [])
      const ack = await this.push(chunk, signal)
      return ack?.complete === true
    }

    let observedCount = 0
    let chunkIndex = 0
    let allocations: OpenAllocationDto[] = []
    let generationAlreadyComplete = false
    let deliveryStopped = false

    await this.streamAllocationPayloads(queryRunner, signal, async (payload) => {
      if (generationAlreadyComplete) {
        return
      }
      const allocation = this.encode(payload, false)
      if (allocation === null) {
        return
      }
      observedCount += 1
      if (observedCount > metadata.allocationCount) {
        throw new Error('allocation set changed during one repeatable-read observation')
      }
      allocations.push(allocation)

      // The planned last chunk is withheld until both streams end and the
      // second-pass count is proven. Earlier chunks cannot make the generation
      // visible, so a driver/data mismatch can never publish a torn snapshot.
      if (allocations.length === MAX_CHUNK_ALLOCATIONS && chunkIndex < metadata.chunkCount - 1) {
        const ack = await this.push(this.chunk(metadata, chunkIndex, allocations), signal)
        if (ack === null) {
          deliveryStopped = true
          throw new SnapshotDeliveryStoppedError()
        }
        if (ack.complete) {
          generationAlreadyComplete = true
          return
        }
        allocations = []
        chunkIndex += 1
      }
    }).catch((error) => {
      if (error instanceof SnapshotDeliveryStoppedError) {
        return
      }
      throw error
    })

    if (generationAlreadyComplete) {
      return true
    }
    if (deliveryStopped) {
      return false
    }
    if (observedCount !== metadata.allocationCount) {
      throw new Error('allocation set changed during one repeatable-read observation')
    }
    if (chunkIndex !== metadata.chunkCount - 1) {
      throw new Error('allocation snapshot chunk count does not match its validated allocation count')
    }

    const ack = await this.push(this.chunk(metadata, chunkIndex, allocations), signal)
    if (ack === null) {
      return false
    }
    if (!ack.complete) {
      this.logger.error(
        `Commerce kept completed allocation snapshot generation ${metadata.generationId} incomplete after ${metadata.chunkCount} chunk(s)`,
      )
      return false
    }
    return true
  }

  private chunk(
    metadata: Omit<AllocationSnapshotChunk, 'schemaVersion' | 'chunkIndex' | 'allocations'>,
    chunkIndex: number,
    allocations: OpenAllocationDto[],
  ): AllocationSnapshotChunk {
    const chunk: AllocationSnapshotChunk = {
      schemaVersion: SNAPSHOT_SCHEMA_VERSION,
      ...metadata,
      chunkIndex,
      allocations,
    }
    if (allocations.length > MAX_CHUNK_ALLOCATIONS) {
      throw new Error(`allocation snapshot chunk ${chunkIndex} exceeds ${MAX_CHUNK_ALLOCATIONS} allocations`)
    }
    const encodedBytes = Buffer.byteLength(JSON.stringify(chunk))
    if (encodedBytes >= MAX_CHUNK_BYTES) {
      throw new Error(`allocation snapshot chunk ${chunkIndex} is ${encodedBytes} bytes; it must stay below ${MAX_CHUNK_BYTES}`)
    }
    return chunk
  }

  private async push(chunk: AllocationSnapshotChunk, signal: AbortSignal): Promise<AllocationSnapshotAck | null> {
    const attempts = RETRY_DELAYS_MS.length + 1
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try {
        const response = await axios.post(
          `${this.configService.get('usageExport.url')}/internal/allocation-snapshot-chunks`,
          chunk,
          {
            timeout: this.configService.get('usageExport.timeoutMs'),
            signal,
            headers: {
              authorization: `Bearer ${this.configService.get('usageExport.token')}`,
              'content-type': 'application/json',
            },
          },
        )
        signal.throwIfAborted()
        return this.acknowledgement(response.data, chunk)
      } catch (error) {
        signal.throwIfAborted()
        const retryDelay = RETRY_DELAYS_MS[attempt]
        if (!this.isTransient(error) || retryDelay === undefined) {
          this.logger.error(
            `Failed to push allocation snapshot generation ${chunk.generationId} chunk ${chunk.chunkIndex + 1}/${chunk.chunkCount}: ${this.describe(error)}`,
          )
          return null
        }
        this.logger.warn(
          `Retrying allocation snapshot generation ${chunk.generationId} chunk ${chunk.chunkIndex + 1}/${chunk.chunkCount} in ${retryDelay}ms: ${this.describe(error)}`,
        )
        await sleep(retryDelay, undefined, { signal })
      }
    }
    return null
  }

  private decode(payload: Record<string, unknown>): OpenAllocation {
    return {
      organizationId: this.string(payload.organizationId, 'organizationId'),
      boxId: this.string(payload.boxId, 'boxId'),
      region: this.string(payload.region, 'region'),
      startAt: this.timestamp(payload.startAt, 'startAt'),
      endAt: payload.endAt == null ? null : this.timestamp(payload.endAt, 'endAt'),
      cpu: this.quantity(payload.cpu, 'cpu'),
      gpu: this.quantity(payload.gpu, 'gpu'),
      mem: this.quantity(payload.mem, 'mem'),
      disk: this.quantity(payload.disk, 'disk'),
    }
  }

  private string(value: unknown, field: string): string {
    if (typeof value !== 'string') {
      throw new InvalidUsagePeriodError(`${field} must be a string`)
    }
    return value
  }

  private timestamp(value: unknown, field: string): Date {
    if (typeof value !== 'string') {
      throw new InvalidUsagePeriodError(`${field} must be a timestamp string`)
    }
    return new Date(value)
  }

  private quantity(value: unknown, field: string): number {
    if (typeof value === 'number') {
      return value
    }
    if (typeof value !== 'string' || !CANONICAL_QUANTITY.test(value)) {
      throw new InvalidUsagePeriodError(`${field} must be a plain decimal quantity`)
    }
    return Number(value)
  }

  private acknowledgement(value: unknown, chunk: AllocationSnapshotChunk): AllocationSnapshotAck {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
      throw new Error('Commerce returned a malformed allocation snapshot acknowledgement')
    }
    const ack = value as Partial<AllocationSnapshotAck>
    if (typeof ack.accepted !== 'number' || !Number.isSafeInteger(ack.accepted) || ack.accepted < 0) {
      throw new Error('Commerce allocation snapshot acknowledgement has an invalid accepted count')
    }
    if (typeof ack.asOf !== 'string' || Number.isNaN(new Date(ack.asOf).getTime())) {
      throw new Error('Commerce allocation snapshot acknowledgement has an invalid asOf')
    }
    if (typeof ack.complete !== 'boolean' || typeof ack.applied !== 'boolean' || (ack.applied && !ack.complete)) {
      throw new Error('Commerce allocation snapshot acknowledgement has an invalid completion state')
    }

    const acknowledgedAt = new Date(ack.asOf).getTime()
    const submittedAt = new Date(chunk.asOf).getTime()
    if (acknowledgedAt < submittedAt) {
      throw new Error('Commerce allocation snapshot acknowledgement moved its asOf backwards')
    }
    if (acknowledgedAt === submittedAt) {
      if (ack.accepted !== chunk.allocations.length) {
        throw new Error('Commerce allocation snapshot acknowledgement accepted count does not match the chunk')
      }
      if (ack.complete && chunk.chunkIndex !== chunk.chunkCount - 1) {
        throw new Error('Commerce completed an allocation snapshot generation before its final sequential chunk')
      }
    } else if (ack.accepted !== 0 || !ack.complete || ack.applied) {
      throw new Error('Commerce allocation snapshot acknowledgement for an obsolete generation is inconsistent')
    }

    return ack as AllocationSnapshotAck
  }

  private isTransient(error: unknown): boolean {
    if (!axios.isAxiosError(error)) {
      return false
    }
    const status = error.response?.status
    return status === undefined || status >= 500
  }

  private describe(error: unknown): string {
    if (axios.isAxiosError(error)) {
      return `${error.code ?? 'HTTP'} ${error.response?.status ?? ''} ${error.message}`.trim()
    }
    return error instanceof Error ? error.message : String(error)
  }
}

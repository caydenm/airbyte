/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.cdc

import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.read.CdcAware
import io.airbyte.cdk.read.CdcContext
import io.airbyte.cdk.read.CdcSharedState
import io.airbyte.cdk.read.DebeziumRecord
import io.airbyte.cdk.read.PartitionReadCheckpoint
import io.airbyte.cdk.read.PartitionReader
import io.airbyte.cdk.read.PartitionReader.TryAcquireResourcesStatus
import io.airbyte.cdk.read.PartitionReader.TryAcquireResourcesStatus.*
import io.airbyte.cdk.read.cdcResourceTaker
import io.airbyte.commons.json.Jsons
import io.debezium.engine.ChangeEvent
import io.debezium.engine.DebeziumEngine
import io.debezium.engine.format.Json
import io.debezium.engine.spi.OffsetCommitPolicy
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

class CdcPartitionReader<S : CdcSharedState>(
    private val sharedState: S,
    cdcContext: CdcContext,
    opaqueStateValue: OpaqueStateValue?,
) : PartitionReader, CdcAware, cdcResourceTaker {

    private val log = KotlinLogging.logger {}
    private var engine: DebeziumEngine<ChangeEvent<String?, String?>>? = null
    private var numRecords = 0L
    private val outputConsumer = cdcContext.outputConsumer
    private val eventConverter = cdcContext.eventConverter
    private val propertyManager = cdcContext.debeziumManager
    private val positionMapper = cdcContext.positionMapperFactory.get()
    private val acquiredResources = AtomicReference<AcquiredResources>()
    val opaqueStateValue = opaqueStateValue

    /** Calling [close] releases the resources acquired for the [JdbcPartitionReader]. */
    fun interface AcquiredResources : AutoCloseable

    override fun tryAcquireResources(): TryAcquireResourcesStatus {
        if (!cdcReadyToRun()) {
            return RETRY_LATER
        }

        val acquiredResources: AcquiredResources =
            sharedState.tryAcquireResourcesForReader() ?: return RETRY_LATER
        this.acquiredResources.set(acquiredResources)
        return READY_TO_RUN
    }

    override suspend fun run() {
        engine = createDebeziumEngine()
        engine?.run()
    }

    override fun checkpoint(): PartitionReadCheckpoint {
        return PartitionReadCheckpoint(propertyManager.readOffsetState(), numRecords)
    }

    override fun releaseResources() {
        acquiredResources.getAndSet(null)?.close()
        cdcRunEnded()
        // Release global CDC lock
    }

    fun createDebeziumEngine(): DebeziumEngine<ChangeEvent<String?, String?>>? {
        log.info {
            "Using DBZ version: ${DebeziumEngine::class.java.getPackage().implementationVersion}"
        }
        return DebeziumEngine.create(Json::class.java)
            .using(propertyManager.getPropertiesForSync(opaqueStateValue))
            .using(OffsetCommitPolicy.AlwaysCommitOffsetPolicy())
            .notifying { event: ChangeEvent<String?, String?> ->
                if (event.value() == null) {
                    return@notifying
                }
                val record = DebeziumRecord(Jsons.deserialize(event.value()))
                // TODO : Migrate over all of the timeout/heartbeat timeout logic
                if (!record.isHeartbeat) {
                    numRecords++
                    outputConsumer.accept(eventConverter.toAirbyteMessage(record))
                }
                if (positionMapper.reachedTargetPosition(record)) {
                    // Stop if we've reached the upper bound.
                    if (record.isHeartbeat) {
                        requestClose(
                            "Closing: Heartbeat indicates sync is done by reaching the target position",
                            DebeziumCloseReason.HEARTBEAT_REACHED_TARGET_POSITION
                        )
                    } else {
                        requestClose(
                            "Closing: Heartbeat indicates sync is not progressing",
                            DebeziumCloseReason.HEARTBEAT_NOT_PROGRESSING
                        )
                    }
                }
            }
            .using { success: Boolean, message: String?, error: Throwable? ->
                log.info { "Debezium engine shutdown. Engine terminated successfully : $success" }
                log.info { message }
                if (!success) {
                    if (error != null) {
                        log.info { "Debezium failed with: $error" }
                    } else {
                        // There are cases where Debezium doesn't succeed but only fills the
                        // message field.
                        // In that case, we still want to fail loud and clear
                        log.info { "Debezium failed with: $message" }
                    }
                }
            }
            .using(
                object : DebeziumEngine.ConnectorCallback {
                    override fun connectorStarted() {
                        log.info { "DebeziumEngine notify: connector started" }
                    }

                    override fun connectorStopped() {
                        log.info { "DebeziumEngine notify: connector stopped" }
                    }

                    override fun taskStarted() {
                        log.info { "DebeziumEngine notify: task started" }
                    }

                    override fun taskStopped() {
                        log.info { "DebeziumEngine notify: task stopped" }
                    }
                },
            )
            .build()
    }

    enum class DebeziumCloseReason() {
        TIMEOUT,
        HEARTBEAT_REACHED_TARGET_POSITION,
        CHANGE_EVENT_REACHED_TARGET_POSITION,
        HEARTBEAT_NOT_PROGRESSING
    }

    private fun requestClose(closeLogMessage: String, closeReason: DebeziumCloseReason) {
        log.info { closeLogMessage }
        log.info { closeReason }
        // TODO : send close analytics message
        // TODO : Close the engine. Note that engine must be closed in a different thread
        // than the running engine.
        CoroutineScope(Dispatchers.Default).launch { engine?.close() }
    }
}

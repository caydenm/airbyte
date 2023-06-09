/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination_async;

import com.google.common.base.Preconditions;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Track the number of flush workers (and their size) that are currently running for a given stream.
 */
@Slf4j
public class RunningFlushWorkers {

  private final ConcurrentMap<StreamDescriptor, ConcurrentMap<UUID, Optional<Long>>> streamToFlushWorkerToBatchSize;
  private final ScheduledExecutorService debugLoop;

  public RunningFlushWorkers() {
    streamToFlushWorkerToBatchSize = new ConcurrentHashMap<>();
    debugLoop = Executors.newSingleThreadScheduledExecutor();
    debugLoop.scheduleAtFixedRate(this::printRunningWorkerInfo, 0, 500, TimeUnit.MILLISECONDS);
  }

  /**
   * Call this when a worker starts flushing a stream.
   *
   * @param stream the stream that is being flushed
   * @param flushWorkerId flush worker id
   */
  public void trackFlushWorker(final StreamDescriptor stream, final UUID flushWorkerId) {
    streamToFlushWorkerToBatchSize.computeIfAbsent(
        stream,
        ignored -> new ConcurrentHashMap<>()).computeIfAbsent(flushWorkerId, ignored -> Optional.empty());
  }

  /**
   * Call this when a worker completes flushing a stream.
   *
   * @param stream the stream that was flushed
   * @param flushWorkerId flush worker id
   */
  public void completeFlushWorker(final StreamDescriptor stream, final UUID flushWorkerId) {
    Preconditions.checkState(streamToFlushWorkerToBatchSize.containsKey(stream)
        && streamToFlushWorkerToBatchSize.get(stream).containsKey(flushWorkerId),
        "Cannot complete flush worker for stream that has not started.");
    streamToFlushWorkerToBatchSize.get(stream).remove(flushWorkerId);
    if (streamToFlushWorkerToBatchSize.get(stream).isEmpty()) {
      streamToFlushWorkerToBatchSize.remove(stream);
    }
  }

  /**
   * When a worker gets a batch of records, register its size so that it can be referenced for
   * estimating how many records will be left in the queue after the batch is done.
   *
   * @param stream stream
   * @param batchSize batch size
   */
  public void registerBatchSize(final StreamDescriptor stream, final UUID flushWorkerId, final long batchSize) {
    Preconditions.checkState(streamToFlushWorkerToBatchSize.containsKey(stream)
        && streamToFlushWorkerToBatchSize.get(stream).containsKey(flushWorkerId),
        "Cannot register a batch size for a flush worker that has not been initialized");
    streamToFlushWorkerToBatchSize.get(stream).put(flushWorkerId, Optional.of(batchSize));
  }

  /**
   * For a stream get how many bytes are in each running worker. If the worker doesn't have a batch
   * yet, return empty optional.
   *
   * @param stream stream
   * @return bytes in batches currently being processed
   */
  public List<Optional<Long>> getSizesOfRunningWorkerBatches(final StreamDescriptor stream) {
    return new ArrayList<>(streamToFlushWorkerToBatchSize.getOrDefault(stream, new ConcurrentHashMap<>()).values());
  }

  private void printRunningWorkerInfo() {
    final var workerInfo = new StringBuilder().append("FLUSH WORKER INFO").append(System.lineSeparator());
    if (streamToFlushWorkerToBatchSize.isEmpty()) {
      return;
    }

    for (final var entry : streamToFlushWorkerToBatchSize.entrySet()) {
      final var workerToBatchSize = entry.getValue();
      workerInfo.append(
          String.format("  Stream name: %s, num of in-flight workers: %d, num bytes: %s",
              entry.getKey().getName(), workerToBatchSize.size(), AirbyteFileUtils.byteCountToDisplaySize(workerToBatchSize.values().stream()
                  .filter(Optional::isPresent)
                  .map(Optional::get)
                  .reduce(0L, Long::sum))))
              .append(System.lineSeparator());
    }
    log.info(workerInfo.toString());
  }

  /**
   * Closes the debug loop for printing all in-flight workers and memory that is still pending
   */
  public void close() throws Exception {
    debugLoop.shutdownNow();
  }

}

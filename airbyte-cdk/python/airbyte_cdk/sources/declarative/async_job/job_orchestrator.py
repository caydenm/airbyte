# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import logging
import time
import traceback
from itertools import chain
from typing import Any, Generator, Iterable, List, Mapping, Optional, Set, Tuple, Type

from airbyte_cdk import StreamSlice
from airbyte_cdk.logger import lazy_log
from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.async_job.job import AsyncJob
from airbyte_cdk.sources.declarative.async_job.job_tracker import ConcurrentJobLimitReached, JobTracker
from airbyte_cdk.sources.declarative.async_job.repository import AsyncJobRepository
from airbyte_cdk.sources.declarative.async_job.status import AsyncJobStatus
from airbyte_cdk.utils.airbyte_secrets_utils import filter_secrets
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

LOGGER = logging.getLogger("airbyte")


class AsyncPartition:
    """
    This bucket of api_jobs is a bit useless for this iteration but should become interesting when we will be able to split jobs
    """

    _MAX_NUMBER_OF_ATTEMPTS = 3

    def __init__(self, jobs: List[AsyncJob], stream_slice: StreamSlice) -> None:
        self._attempts_per_job = {job: 1 for job in jobs}
        self._stream_slice = stream_slice

    def has_reached_max_attempt(self) -> bool:
        return any(map(lambda attempt_count: attempt_count >= self._MAX_NUMBER_OF_ATTEMPTS, self._attempts_per_job.values()))

    def replace_job(self, job_to_replace: AsyncJob, new_jobs: List[AsyncJob]) -> None:
        current_attempt_count = self._attempts_per_job.pop(job_to_replace, None)
        if current_attempt_count is None:
            raise ValueError("Could not find job to replace")
        elif current_attempt_count >= self._MAX_NUMBER_OF_ATTEMPTS:
            raise ValueError(f"Max attempt reached for job in partition {self._stream_slice}")

        new_attempt_count = current_attempt_count + 1
        for job in new_jobs:
            self._attempts_per_job[job] = new_attempt_count

    def should_split(self, job: AsyncJob) -> bool:
        """
        Not used right now but once we support job split, we should split based on the number of attempts
        """
        return False

    @property
    def jobs(self) -> Iterable[AsyncJob]:
        return self._attempts_per_job.keys()

    @property
    def stream_slice(self) -> StreamSlice:
        return self._stream_slice

    @property
    def status(self) -> AsyncJobStatus:
        """
        Given different job statuses, the priority is: FAILED, TIMED_OUT, RUNNING. Else, it means everything is completed.
        """
        statuses = set(map(lambda job: job.status(), self.jobs))
        if statuses == {AsyncJobStatus.COMPLETED}:
            return AsyncJobStatus.COMPLETED
        elif AsyncJobStatus.FAILED in statuses:
            return AsyncJobStatus.FAILED
        elif AsyncJobStatus.TIMED_OUT in statuses:
            return AsyncJobStatus.TIMED_OUT
        else:
            return AsyncJobStatus.RUNNING

    def __repr__(self) -> str:
        return f"AsyncPartition(stream_slice={self._stream_slice}, attempt_per_job={self._attempts_per_job})"

    def __json_serializable__(self) -> Any:
        return self._stream_slice


class AsyncJobOrchestrator:
    _WAIT_TIME_BETWEEN_STATUS_UPDATE_IN_SECONDS = 5
    _KNOWN_JOB_STATUSES = {AsyncJobStatus.COMPLETED, AsyncJobStatus.FAILED, AsyncJobStatus.RUNNING, AsyncJobStatus.TIMED_OUT}

    def __init__(
        self,
        job_repository: AsyncJobRepository,
        slices: Iterable[StreamSlice],
        job_tracker: JobTracker,
        exceptions_to_break_on: Iterable[Type[Exception]] = tuple(),
    ) -> None:
        if {*AsyncJobStatus} != self._KNOWN_JOB_STATUSES:
            # this is to prevent developers updating the possible statuses without updating the logic of this class
            raise ValueError(
                "An AsyncJobStatus has been either removed or added which means the logic of this class needs to be reviewed. Once the logic has been updated, please update _KNOWN_JOB_STATUSES"
            )

        self._job_repository: AsyncJobRepository = job_repository
        self._slice_iterator = iter(slices)
        self._running_partitions: List[AsyncPartition] = []
        self._job_tracker = job_tracker
        self._exceptions_to_break_on: Tuple[Type[Exception], ...] = tuple(exceptions_to_break_on)

        self._has_started_a_job = False
        self._has_consumed_every_slice = True
        self._non_breaking_exceptions: List[Exception] = []

    def _replace_failed_jobs(self, partition: AsyncPartition) -> None:
        failed_status_jobs = (AsyncJobStatus.FAILED, AsyncJobStatus.TIMED_OUT)
        jobs_to_replace = [job for job in partition.jobs if job.status() in failed_status_jobs]
        for job in jobs_to_replace:
            new_job = self._start_job(job.job_parameters(), job.api_job_id())
            partition.replace_job(job, [new_job])

    def _start_jobs(self) -> None:
        """
        Retry failed jobs and start jobs for each slice in the slice iterator.
        This method iterates over the running jobs and slice iterator and starts a job for each slice.
        The started jobs are added to the running partitions.
        Returns:
            None

        However, the first iteration is for sendgrid which only has one job.
        """
        at_least_one_slice_consumed_from_slice_iterator_during_current_iteration = False
        _slice = None
        try:
            for partition in self._running_partitions:
                self._replace_failed_jobs(partition)

            for _slice in self._slice_iterator:
                at_least_one_slice_consumed_from_slice_iterator_during_current_iteration = True
                job = self._start_job(_slice)
                self._has_started_a_job = True
                self._running_partitions.append(AsyncPartition([job], _slice))
            else:
                self._has_consumed_every_slice = True
        except ConcurrentJobLimitReached:
            if at_least_one_slice_consumed_from_slice_iterator_during_current_iteration:
                # this means a slice has been consumed and we need to put it back at the beginning of the _slice_iterator
                self._slice_iterator = chain([_slice], self._slice_iterator)  # type: ignore  # we know the slice comes from the _slice_iterator so it will not be None at this point
            LOGGER.debug("Waiting before creating more jobs as the limit of concurrent jobs has been reached. Will try again later...")

    def _start_job(self, _slice: StreamSlice, previous_job_id: Optional[str] = None) -> AsyncJob:
        if previous_job_id:
            id_to_replace = previous_job_id
        else:
            id_to_replace = self._job_tracker.try_to_get_intent()
        try:
            job = self._job_repository.start(_slice)
            self._job_tracker.add_job(id_to_replace, job.api_job_id())
        except Exception as exception:
            self._job_tracker.remove_job(id_to_replace)
            raise exception
        return job

    def _get_running_jobs(self) -> Set[AsyncJob]:
        """
        Returns a set of running AsyncJob objects.

        Returns:
            Set[AsyncJob]: A set of AsyncJob objects that are currently running.
        """
        return {job for partition in self._running_partitions for job in partition.jobs if job.status() == AsyncJobStatus.RUNNING}

    def _get_timeout_jobs(self) -> Set[AsyncJob]:
        """
        Returns a set of timeouted AsyncJob objects.

        Returns:
            Set[AsyncJob]: A set of AsyncJob objects that are currently running.
        """
        return {job for partition in self._running_partitions for job in partition.jobs if job.status() == AsyncJobStatus.TIMED_OUT}

    def _update_jobs_status(self) -> None:
        """
        Update the status of all running jobs in the repository.
        """
        running_jobs = self._get_running_jobs()
        if running_jobs:
            # update the status only if there are RUNNING jobs
            self._job_repository.update_jobs_status(running_jobs)

    def _wait_on_status_update(self) -> None:
        """
        Waits for a specified amount of time between status updates.


        This method is used to introduce a delay between status updates in order to avoid excessive polling.
        The duration of the delay is determined by the value of `_WAIT_TIME_BETWEEN_STATUS_UPDATE_IN_SECONDS`.

        Returns:
            None
        """
        lazy_log(
            LOGGER,
            logging.DEBUG,
            lambda: f"Polling status in progress. There are currently {len(self._running_partitions)} running partitions.",
        )

        # wait only when there are running partitions
        if self._running_partitions:
            lazy_log(
                LOGGER,
                logging.DEBUG,
                lambda: f"Waiting for {self._WAIT_TIME_BETWEEN_STATUS_UPDATE_IN_SECONDS} seconds before next poll...",
            )
            time.sleep(self._WAIT_TIME_BETWEEN_STATUS_UPDATE_IN_SECONDS)

    def _process_completed_partition(self, partition: AsyncPartition) -> None:
        """
        Process a completed partition.
        Args:
            partition (AsyncPartition): The completed partition to process.
        """
        job_ids = list(map(lambda job: job.api_job_id(), {job for job in partition.jobs}))
        LOGGER.info(f"The following jobs for stream slice {partition.stream_slice} have been completed: {job_ids}.")

    def _process_running_partitions_and_yield_completed_ones(self) -> Generator[AsyncPartition, Any, None]:
        """
        Process the running partitions.

        Yields:
            AsyncPartition: The processed partition.

        Raises:
            Any: Any exception raised during processing.
        """
        current_running_partitions: List[AsyncPartition] = []
        for partition in self._running_partitions:
            match partition.status:
                case AsyncJobStatus.COMPLETED:
                    self._process_completed_partition(partition)
                    yield partition
                case AsyncJobStatus.RUNNING:
                    current_running_partitions.append(partition)
                case _ if partition.has_reached_max_attempt():
                    self._stop_partition(partition)
                    self._process_partitions_with_errors(partition)
                case _:
                    self._stop_timed_out_jobs(partition)

                    # job will be restarted in `_start_job`
                    current_running_partitions.insert(0, partition)

            for job in partition.jobs:
                # We only remove completed jobs as we want failed/timed out jobs to be re-allocated in priority
                if job.status() == AsyncJobStatus.COMPLETED:
                    self._job_tracker.remove_job(job.api_job_id())

        # update the referenced list with running partitions
        self._running_partitions = current_running_partitions

    def _stop_partition(self, partition: AsyncPartition) -> None:
        for job in partition.jobs:
            if job.status() in {AsyncJobStatus.RUNNING, AsyncJobStatus.TIMED_OUT}:
                self._abort_job(job)
            else:
                self._job_tracker.remove_job(job.api_job_id())

    def _stop_timed_out_jobs(self, partition: AsyncPartition) -> None:
        for job in partition.jobs:
            if job.status() == AsyncJobStatus.TIMED_OUT:
                # we don't free allocation here because it is expected to retry the job
                self._abort_job(job, free_job_allocation=False)

    def _abort_job(self, job: AsyncJob, free_job_allocation: bool = True) -> None:
        try:
            self._job_repository.abort(job)
            if free_job_allocation:
                self._job_tracker.remove_job(job.api_job_id())
        except Exception as exception:
            LOGGER.warning(f"Could not free budget for job {job.api_job_id()}: {exception}")

    def _process_partitions_with_errors(self, partition: AsyncPartition) -> None:
        """
        Process a partition with status errors (FAILED and TIMEOUT).

        Args:
            partition (AsyncPartition): The partition to process.
        Returns:
            AirbyteTracedException: An exception indicating that at least one job could not be completed.
        Raises:
            AirbyteTracedException: If at least one job could not be completed.
        """
        status_by_job_id = {job.api_job_id(): job.status() for job in partition.jobs}
        raise AirbyteTracedException(
            message=f"At least one job could not be completed. Job statuses were: {status_by_job_id}",
            failure_type=FailureType.system_error,
        )

    def create_and_get_completed_partitions(self) -> Iterable[AsyncPartition]:
        """
        Creates and retrieves completed partitions.
        This method continuously starts jobs, updates job status, processes running partitions,
        logs polling partitions, and waits for status updates. It yields completed partitions
        as they become available.

        Returns:
            An iterable of completed partitions, represented as AsyncPartition objects.
            Each partition is wrapped in an Optional, allowing for None values.
        """
        while True:
            try:
                self._start_jobs()
            except self._exceptions_to_break_on as e:
                self._abort_all_running_jobs()
                raise e
            except Exception as e:
                self._handle_non_breaking_error(e)
            if (self._has_started_a_job or self._has_consumed_every_slice) and not self._running_partitions:
                break

            self._update_jobs_status()
            yield from self._process_running_partitions_and_yield_completed_ones()
            self._wait_on_status_update()

        if self._non_breaking_exceptions:
            # We didn't break on non_breaking_exception, but we still need to raise an exception so that the stream is flagged as incomplete
            raise AirbyteTracedException(
                message="",
                internal_message="\n".join([filter_secrets(exception.__repr__()) for exception in self._non_breaking_exceptions]),
                failure_type=FailureType.config_error,
            )

    def _handle_non_breaking_error(self, exception: Exception) -> None:
        LOGGER.error(f"Failed to start the Job: {exception}, traceback: {traceback.format_exc()}")
        self._non_breaking_exceptions.append(exception)

    def _abort_all_running_jobs(self) -> None:
        [self._job_repository.abort(job) for job in self._get_running_jobs() | self._get_timeout_jobs()]

        self._running_partitions = []

    def fetch_records(self, partition: AsyncPartition) -> Iterable[Mapping[str, Any]]:
        """
        Fetches records from the given partition's jobs.

        Args:
            partition (AsyncPartition): The partition containing the jobs.

        Yields:
            Iterable[Mapping[str, Any]]: The fetched records from the jobs.
        """
        for job in partition.jobs:
            yield from self._job_repository.fetch_records(job)
            self._job_repository.delete(job)

"""Apache Beam pipeline for computing per-DB port-down update rates.

This module implements a portable Apache Beam pipeline (with zero Google-only
Flume dependencies) that processes a stream of commit-like database diffs from
networking switches.

Pipeline Architecture:
  1. `PruneAndProjectDiffsFn` (Map/ParDo before shuffle):
     - Drops diffs written after `end_time`, since future diffs cannot affect
       the database state during `[start_time, end_time]`.
     - Retains diffs written before `start_time`, because earlier diffs are
       required to reconstruct the active `BINARY_VERSION` and each port's
       prior `PortStatus` entering `start_time`.
     - Strips out unrelated tables (`VLAN`, `BGP_NEIGHBOR`, `ROUTE`,
       `other_tables`) and non-key/non-status `PORT` columns (`port_id`, `mtu`,
       `speed_gbps`, `admin_status`, `attributes`) so that `GroupByKey`
       shuffles only minimal `_SlimDiff` payloads across the network.
  2. `beam.GroupByKey()`:
     - Groups all `_SlimDiff` records by `db_id`.
  3. `ComputeDbPortDownRateFn` (ParDo after shuffle):
     - Sorts each database's diffs chronologically by `timestamp` (placing base
       snapshots first on timestamp ties).
     - Replays `BINARY_VERSION` and `PORT` state transitions in a single pass,
       counting `UP -> DOWN` transitions within `[start_time, end_time]` while
       `current_version == target_version`, and normalizing by the exact active
       time spent on `target_version` within `[start_time, end_time]`.
"""

from collections.abc import Iterable, Iterator, Sequence
import dataclasses

import apache_beam as beam
from apache_beam.options import pipeline_options

from google3.experimental.users.zhenyuzhou.beam import schema


@dataclasses.dataclass(frozen=True)
class _SlimDiff:
  """Compact projection of a `DatabaseDiff` for network shuffle efficiency.

  Only fields that influence the `BINARY_VERSION` state machine or `PORT`
  operational status transitions are kept. All other tables and columns are
  discarded prior to `GroupByKey`.

  Attributes:
    timestamp: Epoch timestamp (in seconds) when the diff was written.
    is_base_snapshot: Whether this diff explicitly marks a base snapshot.
    version: Updated binary version string, or `None` if unchanged.
    port_statuses: Tuple of `(port_name, status)` updates in this diff.
  """

  timestamp: float
  is_base_snapshot: bool
  version: str | None
  port_statuses: tuple[tuple[str, schema.PortStatus], ...]


class PruneAndProjectDiffsFn(beam.DoFn):
  """Prunes out-of-window or irrelevant diffs and strips unused columns."""

  def __init__(self, end_time: float):
    """Initializes the projection `DoFn`.

    Args:
      end_time: Inclusive upper bound timestamp (in seconds) of the query
        window.
    """
    super().__init__()
    self._end_time = end_time

  def process(
      self, diff: schema.DatabaseDiff
  ) -> Iterator[tuple[str, _SlimDiff]]:
    """Projects a `DatabaseDiff` into a keyed `(db_id, _SlimDiff)` pair.

    Diffs occurring strictly after `end_time` cannot affect database state
    within `[start_time, end_time]` and are dropped prior to `GroupByKey`. Diffs
    that modify neither `BINARY_VERSION` nor `PORT.status` are also dropped.

    Args:
      diff: Input `DatabaseDiff` record.

    Yields:
      A `(db_id, _SlimDiff)` tuple when the diff contains relevant state updates
      at or before `end_time`.
    """
    # Any diff after `self._end_time` is causally after our query window and
    # cannot affect either active time or port transitions in `[start, end]`.
    # Note: We intentionally keep diffs with `diff.timestamp < start_time`
    # because we need them to reconstruct the state at `start_time`.
    if diff.timestamp > self._end_time:
      return

    # Extract only the version string from `BINARY_VERSION`, discarding build
    # metadata such as `build_cl`, `package_name`, and `attributes`.
    version = (
        diff.binary_version.version if diff.binary_version is not None else None
    )

    # Project `PORT` updates down to `(port_name, status)`. `port_name` is the
    # primary key of the `PORT` table, whereas `port_id` is non-unique across
    # switches and is discarded along with `mtu`, `speed_gbps`, `fec_mode`, etc.
    # If a `PORT` row update did not touch `status` (`status is None`), omit it.
    port_statuses = tuple(
        (port_diff.port_name, port_diff.status)
        for port_diff in diff.port_diffs
        if port_diff.status is not None
    )

    # If this diff only touched unrelated tables (e.g., `VLAN`, `BGP_NEIGHBOR`,
    # `ROUTE`) or non-status `PORT` columns, drop it completely before shuffle.
    if version is None and not port_statuses and not diff.is_base_snapshot:
      return

    yield (
        diff.db_id,
        _SlimDiff(
            timestamp=diff.timestamp,
            is_base_snapshot=diff.is_base_snapshot,
            version=version,
            port_statuses=port_statuses,
        ),
    )


def _overlap_duration(
    interval_start: float,
    interval_end: float,
    window_start: float,
    window_end: float,
) -> float:
  """Returns the overlap length between `[interval_start, interval_end]` and window."""
  # Intersect `[interval_start, interval_end]` with `[window_start, window_end]`
  # and clamp negative durations to `0.0` when the intervals are disjoint.
  clipped_start = max(interval_start, window_start)
  clipped_end = min(interval_end, window_end)
  return max(0.0, clipped_end - clipped_start)


class ComputeDbPortDownRateFn(beam.DoFn):
  """Replays per-DB diffs chronologically to compute the port-down rate."""

  def __init__(
      self,
      target_version: str,
      start_time: float,
      end_time: float,
  ):
    """Initializes the per-DB replay `DoFn`.

    Args:
      target_version: Binary version string to filter for.
      start_time: Inclusive start timestamp (in seconds) of the query window.
      end_time: Inclusive end timestamp (in seconds) of the query window.
    """
    super().__init__()
    self._target_version = target_version
    self._start_time = start_time
    self._end_time = end_time

  def process(
      self, element: tuple[str, Iterable[_SlimDiff]]
  ) -> Iterator[schema.PortDownRateResult]:
    """Replays all diffs for a single database and yields its port-down rate.

    Args:
      element: Tuple of `(db_id, diffs)` produced by `GroupByKey`.

    Yields:
      A `PortDownRateResult` if the database spent positive active time on
      `target_version` during `[start_time, end_time]`.
    """
    db_id, diffs = element
    # Sort diffs in chronological order. If two diffs have the exact same
    # timestamp, `not item.is_base_snapshot` (`False < True`) ensures the base
    # snapshot is applied before incremental diffs.
    sorted_diffs = sorted(
        diffs,
        key=lambda item: (item.timestamp, not item.is_base_snapshot),
    )
    if not sorted_diffs:
      return

    # Track the latest snapshot state as we replay each diff in order:
    # - `current_version` holds the value from the `BINARY_VERSION` table.
    # - `port_state` maps `port_name` (primary key) to its latest `PortStatus`.
    current_version: str | None = None
    port_state: dict[str, schema.PortStatus] = {}
    port_down_count = 0
    active_time_seconds = 0.0

    for index, diff in enumerate(sorted_diffs):
      # The very first diff (or any diff explicitly flagged with
      # `is_base_snapshot=True`) establishes initial state rather than a state
      # transition event.
      is_initial_snapshot = index == 0 or diff.is_base_snapshot

      # Step 1: Before applying `diff` at `diff.timestamp`, accrue the active
      # time elapsed since `sorted_diffs[index - 1].timestamp` under the
      # previously active `current_version`.
      if index > 0:
        previous_timestamp = sorted_diffs[index - 1].timestamp
        if current_version == self._target_version:
          active_time_seconds += _overlap_duration(
              interval_start=previous_timestamp,
              interval_end=diff.timestamp,
              window_start=self._start_time,
              window_end=self._end_time,
          )

      # Step 2: If this diff updates `BINARY_VERSION`, update `current_version`.
      # If `diff.version` is `None`, the table remains unchanged from the
      # previous snapshot.
      if diff.version is not None:
        current_version = diff.version

      in_query_window = self._start_time <= diff.timestamp <= self._end_time
      on_target_version = current_version == self._target_version

      # Step 3: Apply `PORT` status updates and count `UP -> DOWN` transitions
      # that occur within `[self._start_time, self._end_time]` while the DB is
      # running `self._target_version`.
      for port_name, new_status in diff.port_statuses:
        previous_status = port_state.get(port_name)
        port_state[port_name] = new_status
        if (
            not is_initial_snapshot
            and in_query_window
            and on_target_version
            and previous_status == schema.PortStatus.UP
            and new_status == schema.PortStatus.DOWN
        ):
          port_down_count += 1

    # Step 4: Account for the trailing time interval from the last diff at or
    # before `self._end_time` up to `self._end_time`, during which the DB state
    # remains at the final snapshot's `current_version`.
    if current_version == self._target_version:
      active_time_seconds += _overlap_duration(
          interval_start=sorted_diffs[-1].timestamp,
          interval_end=self._end_time,
          window_start=self._start_time,
          window_end=self._end_time,
      )

    # Exclude databases that spent zero time on `self._target_version` during
    # `[self._start_time, self._end_time]`.
    if active_time_seconds <= 0.0:
      return

    yield schema.PortDownRateResult(
        db_id=db_id,
        port_down_rate=port_down_count / active_time_seconds,
        port_down_count=port_down_count,
        active_time_seconds=active_time_seconds,
    )


class ComputePortDownRates(
    beam.PTransform[
        beam.PCollection[schema.DatabaseDiff],
        beam.PCollection[schema.PortDownRateResult],
    ]
):
  """Composite `PTransform` computing per-DB `UP -> DOWN` port transition rates."""

  def __init__(
      self,
      target_version: str,
      start_time: float,
      end_time: float,
  ):
    """Initializes the `ComputePortDownRates` transform.

    Args:
      target_version: Binary version string to filter for.
      start_time: Inclusive start timestamp (in seconds) of the query window.
      end_time: Inclusive end timestamp (in seconds) of the query window.

    Raises:
      ValueError: If `end_time <= start_time` or `target_version` is empty.
    """
    super().__init__()
    if not target_version:
      raise ValueError('target_version must be a non-empty string.')
    if end_time <= start_time:
      raise ValueError(
          f'end_time ({end_time}) must be strictly greater than '
          f'start_time ({start_time}).'
      )
    self._target_version = target_version
    self._start_time = start_time
    self._end_time = end_time

  def expand(
      self, input_diffs: beam.PCollection[schema.DatabaseDiff]
  ) -> beam.PCollection[schema.PortDownRateResult]:
    """Applies pruning, grouping by `db_id`, and chronological state replay.

    Args:
      input_diffs: `PCollection` of `DatabaseDiff` records.

    Returns:
      `PCollection` of `PortDownRateResult` records for qualifying databases.
    """
    # Because the output rate is grouped by `db_id`, we only need a single
    # `GroupByKey` shuffle by `db_id` followed by per-key state replay.
    return (
        input_diffs
        | 'PruneAndProjectDiffs'
        >> beam.ParDo(PruneAndProjectDiffsFn(end_time=self._end_time))
        | 'GroupByDbId' >> beam.GroupByKey()
        | 'ComputePortDownRatePerDb'
        >> beam.ParDo(
            ComputeDbPortDownRateFn(
                target_version=self._target_version,
                start_time=self._start_time,
                end_time=self._end_time,
            )
        )
    )


def run_pipeline(
    input_path: str,
    output_path: str,
    target_version: str,
    start_time: float,
    end_time: float,
    beam_args: Sequence[str] = (),
) -> None:
  """Constructs and executes the Apache Beam pipeline on JSONL files.

  Args:
    input_path: Input file path or glob pattern for JSONL diff records.
    output_path: Output file path prefix for JSONL rate results.
    target_version: Target database binary version string.
    start_time: Inclusive start timestamp (in seconds) of the query window.
    end_time: Inclusive end timestamp (in seconds) of the query window.
    beam_args: Additional command-line arguments forwarded to Beam
      `PipelineOptions` (e.g., `--runner=DirectRunner`).
  """
  options = pipeline_options.PipelineOptions(list(beam_args))
  with beam.Pipeline(options=options) as root:
    _ = (
        root
        | 'ReadJsonLines' >> beam.io.ReadFromText(input_path)
        | 'ParseDatabaseDiffs' >> beam.Map(schema.DatabaseDiff.from_json)
        | 'ComputePortDownRates'
        >> ComputePortDownRates(
            target_version=target_version,
            start_time=start_time,
            end_time=end_time,
        )
        | 'FormatJsonLines' >> beam.Map(schema.PortDownRateResult.to_json)
        | 'WriteJsonLines'
        >> beam.io.WriteToText(output_path, shard_name_template='')
    )

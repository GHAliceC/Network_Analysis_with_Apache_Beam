"""CLI entry point for running the networking DB port-down rate Beam pipeline."""

from collections.abc import Sequence

from absl import app
from absl import flags

from google3.experimental.users.zhenyuzhou.beam import pipeline

_INPUT_PATH = flags.DEFINE_string(
    'input_path',
    '',
    'Input file pattern containing JSONL-encoded DatabaseDiff records.',
    required=True,
)
_OUTPUT_PATH = flags.DEFINE_string(
    'output_path',
    '',
    'Output file prefix for writing JSONL-encoded PortDownRateResult records.',
    required=True,
)
_TARGET_VERSION = flags.DEFINE_string(
    'target_version',
    '',
    'Target database binary version to filter for.',
    required=True,
)
_START_TIME = flags.DEFINE_float(
    'start_time',
    0.0,
    'Inclusive start timestamp (in seconds) of the query window.',
    required=True,
)
_END_TIME = flags.DEFINE_float(
    'end_time',
    0.0,
    'Inclusive end timestamp (in seconds) of the query window.',
    required=True,
)


def main(argv: Sequence[str]) -> None:
  """Parses flags and launches the port-down rate Beam pipeline."""
  pipeline.run_pipeline(
      input_path=_INPUT_PATH.value,
      output_path=_OUTPUT_PATH.value,
      target_version=_TARGET_VERSION.value,
      start_time=_START_TIME.value,
      end_time=_END_TIME.value,
      beam_args=argv[1:],
  )


if __name__ == '__main__':
  app.run(main)

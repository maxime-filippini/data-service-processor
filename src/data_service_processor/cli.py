import argparse
import json

from .runtime import processor_runtime


def main():
    parser = argparse.ArgumentParser(description="Execute a frozen market-data job")
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args()
    try:
        with processor_runtime() as processor:
            result = processor.execute(args.job_id)
    except Exception as error:  # noqa: BLE001 -- CLI must exit cleanly on all operational failures.
        parser.exit(
            1, f"Processing failed ({type(error).__name__}); inspect job state\n"
        )
    print(json.dumps(result))

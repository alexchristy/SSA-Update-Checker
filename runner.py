import argparse
import datetime
import logging
import os
import subprocess
import sys
from datetime import timezone
from pathlib import Path

import sentry_sdk

# Setup constants
LOCK_FILE_PATH = Path("/tmp/ssa-update-checker.lock")  # noqa S108 (Lock file path)
LOG_DIRECTORY_PATH = Path("/home/ssa-worker/SSA-Update-Checker/log")
BASE_DIRECTORY = Path("/home/ssa-worker/SSA-Update-Checker")


def init_sentry() -> None:
    """Initialize Sentry SDK for error tracking."""
    sentry_sdk.init(
        dsn="https://0a6117986a084203f3480083c2cb4237@o4506224652713984.ingest.us.sentry.io/4506900246691840",
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        traces_sample_rate=1.0,
        # Set profiles_sample_rate to 1.0 to profile 100%
        # of sampled transactions.
        # We recommend adjusting this value in production.
        profiles_sample_rate=1.0,
    )


def rotate_log_file(log_file_path: Path, log_type: str = "app") -> None:
    """Rotate a log file, appending a timestamp and moving it to the log directory.

    Args:
    ----
        log_file_path (Path): The path to the log file to rotate.
        log_type (str): The type of log file to rotate (default: "app").

    Returns:
    -------
        None

    """
    if log_file_path.exists():
        timestamp = datetime.datetime.now(tz=timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        if log_type == "app":
            rotated_log_file = log_file_path.with_name(f"app_{timestamp}.log")
        else:  # Assuming log_type == "wrapper"
            rotated_log_file = log_file_path.with_name(
                f"wrapper_script_{timestamp}.log"
            )
        log_file_path.rename(rotated_log_file)


def setup_logging() -> logging.Logger:
    """Set up logging for the wrapper script, with rotation for the log file."""
    wrapper_log_path = Path(LOG_DIRECTORY_PATH, "wrapper_script.log")
    rotate_log_file(wrapper_log_path, log_type="wrapper")

    logger = logging.getLogger("wrapper_logger")
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(wrapper_log_path)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def run_main_script(working_dir: str, venv_path: str) -> tuple[str, str]:
    """Execute the main script within the virtual environment and log its output.

    Args:
    ----
        working_dir (str): The working directory for the script.
        venv_path (str): The path to the virtual environment.

    Returns:
    -------
        tuple[str, str]: The standard output and standard error of the script.

    """
    # Construct the command to run the main.py script
    command = [
        "/usr/bin/env",
        "python3",
        os.path.join(working_dir, "main.py"),
        "--log",
        "INFO",
    ]

    # Setting the environment for subprocess
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = venv_path
    env["PATH"] = f"{venv_path}/bin:" + env["PATH"]

    # Execute the command
    process = subprocess.run(
        command,  # noqa S603 (Needed to execute the command and capture output)
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )

    # Process and return stdout and stderr
    return process.stdout, process.stderr


def main(working_dir: str, venv_name: str) -> None:
    """Execute the main script within the virtual environment and handle its output.

    Args:
    ----
        working_dir (str): The working directory for the script.
        venv_name (str): The name of the virtual environment.

    """
    init_sentry()
    wrapper_logger = setup_logging()
    # Check and handle the lock file
    lock_file = Path(LOCK_FILE_PATH)
    if lock_file.exists():
        wrapper_logger.error(
            "Lock file exists, indicating a potentially failed or ongoing run."
        )
        sys.exit("Lock file exists, aborting script.")

    # Create a lock file to prevent concurrent execution
    lock_file.touch()

    # Rotate the app.log file before running the script
    rotate_log_file(LOG_DIRECTORY_PATH)

    # Construct full path for the virtual environment
    venv_path = os.path.join(working_dir, venv_name)

    try:
        stdout, stderr = run_main_script(working_dir, venv_path)

        # Handle standard output (app.log content)
        if stdout:
            app_log_path = os.path.join(LOG_DIRECTORY_PATH, "app.log")
            with open(app_log_path, "a") as log_file:
                log_file.write(stdout)

        # Handle errors (if any)
        if stderr:
            error_log_filename = datetime.datetime.now(tz=timezone.utc).strftime(
                "error_%Y-%m-%d_%H-%M-%S.log"
            )
            error_log_path = os.path.join(LOG_DIRECTORY_PATH, error_log_filename)
            with open(error_log_path, "w") as error_file:
                error_file.write(stderr)
            msg = f"Errors occurred during script execution. See {error_log_path} for details."
            wrapper_logger.error(msg)

    except Exception as e:
        msg = f"An error occurred: {e}"
        wrapper_logger.exception(msg)
    finally:
        # Ensure the lock file is removed after execution
        lock_file.unlink()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Wrapper script for running the SSA Update Checker."
    )
    parser.add_argument(
        "-d",
        "--working-directory",
        type=str,
        help="Specify the working directory",
        default=BASE_DIRECTORY,
    )
    parser.add_argument(
        "-e",
        "--virtual-env",
        type=str,
        help="Specify the virtual environment name",
        required=True,
    )

    args = parser.parse_args()

    if not Path(args.working_directory).exists():
        sys.exit(
            f"The specified working directory does not exist: {args.working_directory}"
        )

    main(args.working_directory, args.virtual_env)

import argparse
import datetime
import logging
import os
import subprocess
import sys
import tempfile
from datetime import timezone
from pathlib import Path

# Setup constants
LOCK_FILE_PATH = tempfile.NamedTemporaryFile(
    delete=False
).name  # Remove the lock file after execution
LOG_DIRECTORY_PATH = "/home/ssa-worker/SSA-Update-Checker/log"
BASE_DIRECTORY = "/home/ssa-worker/SSA-Update-Checker"

# Setup logging for the wrapper script
wrapper_logger = logging.getLogger("wrapper_logger")
wrapper_logger.setLevel(logging.INFO)
fh = logging.FileHandler(f"{LOG_DIRECTORY_PATH}/wrapper_script.log")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
wrapper_logger.addHandler(fh)


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
    # Check and handle the lock file
    lock_file = Path(LOCK_FILE_PATH)
    if lock_file.exists():
        wrapper_logger.error(
            "Lock file exists, indicating a potentially failed or ongoing run."
        )
        sys.exit("Lock file exists, aborting script.")

    # Create a lock file to prevent concurrent execution
    lock_file.touch()

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

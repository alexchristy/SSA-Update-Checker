import logging
import os
import unittest
from typing import ClassVar, List
from unittest.mock import patch

from dotenv import load_dotenv

from main import check_env_variables, move_to_working_dir, parse_args, setup_logging


class TestSetupLogging(unittest.TestCase):
    """Test the setup_logging function."""

    def test_default_info_level(self: "TestSetupLogging") -> None:
        """Test that the default log level is INFO."""
        setup_logging()
        self.assertEqual(logging.getLogger().getEffectiveLevel(), logging.INFO)

    def test_accepts_custom_level(self: "TestSetupLogging") -> None:
        """Test that the setup_logging function accepts a custom log level."""
        setup_logging(default_level=logging.DEBUG)
        self.assertEqual(logging.getLogger().getEffectiveLevel(), logging.DEBUG)

    def test_default_log_file(self: "TestSetupLogging") -> None:
        """Test that the default log file is None."""
        setup_logging()

        logging.info("This is a test message.")

        file_exists = os.path.exists("./app.log")

        self.assertTrue(file_exists)

    def test_accepts_custom_log_file(self: "TestSetupLogging") -> None:
        """Test that the setup_logging function accepts a custom log file."""
        setup_logging(log_file="test.log")

        logging.info("This is a test message.")

        file_exists = os.path.exists("./test.log")

        self.assertTrue(file_exists)

        # Clean up
        os.remove("./test.log")

    def test_default_log_format(self: "TestSetupLogging") -> None:
        """Test that the default log format is as expected."""
        setup_logging()

        logging.info("This is a test message.")

        with open("./app.log", "r") as file:
            log_contents = file.read()

        self.assertIn("|| INFO - This is a test message.", log_contents)


class TestMoveToWorkingDir(unittest.TestCase):
    """Test the move_to_working_dir function."""

    def test_default_working_dir(self: "TestMoveToWorkingDir") -> None:
        """Test that the default working directory is the project root."""
        move_to_working_dir()
        self.assertEqual(os.getcwd(), os.path.dirname(os.path.abspath("main.py")))


class TestParseArgs(unittest.TestCase):
    """Test the parse_args function."""

    @patch("sys.argv", ["program_name", "--log", "DEBUG"])
    def test_parse_args_debug_level(self: "TestParseArgs") -> None:
        """Test parsing --log DEBUG argument."""
        args = parse_args()
        self.assertEqual(args.log, "DEBUG")

    @patch("sys.argv", ["program_name", "--log", "INFO"])
    def test_parse_args_info_level(self: "TestParseArgs") -> None:
        """Test parsing --log INFO argument."""
        args = parse_args()
        self.assertEqual(args.log, "INFO")

    @patch("sys.argv", ["program_name"])
    def test_parse_args_default_level(self: "TestParseArgs") -> None:
        """Test parsing with no --log argument, should default to INFO."""
        args = parse_args()
        self.assertEqual(args.log, "INFO")


class TestCheckEnvVariables(unittest.TestCase):
    """Test the check_env_variables function."""

    fake_env_path: ClassVar[str] = "tests/assets/TestCheckEnvVariables/env"
    vars_to_check: ClassVar[List[str]] = [
        "FS_CRED_PATH",
        "TERMINAL_COLL",
        "PDF_ARCHIVE_COLL",
        "AWS_BUCKET_NAME",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "PDF_DIR",
        "OPENAI_API_KEY",
        "GOOGLE_MAPS_API_KEY",
        "LOCK_COLL",
        "SENTRY_DSN",
    ]

    def setUp(self: "TestCheckEnvVariables") -> None:
        """Load in fake .env file for testing."""
        load_dotenv(dotenv_path=self.fake_env_path)

    def test_check_env_variables(self: "TestCheckEnvVariables") -> None:
        """Test that the check_env_variables function returns True when all required env variables are present."""
        self.assertTrue(check_env_variables(self.vars_to_check))

    def test_check_env_variables_missing_var(self: "TestCheckEnvVariables") -> None:
        """Test that the check_env_variables function returns False when a required env variable is missing."""
        os.unsetenv("FS_CRED_PATH")
        del os.environ["FS_CRED_PATH"]

        self.assertFalse(check_env_variables(self.vars_to_check))

    def test_check_env_variables_missing_all_vars(
        self: "TestCheckEnvVariables",
    ) -> None:
        """Test that the check_env_variables function returns False when all required env variables are missing."""
        for var in self.vars_to_check:
            os.unsetenv(var)
            del os.environ[var]

        self.assertFalse(check_env_variables(self.vars_to_check))

    def test_check_env_variables_empty_var(self: "TestCheckEnvVariables") -> None:
        """Test that the check_env_variables function returns False when a required env variable is empty."""
        os.environ["FS_CRED_PATH"] = ""
        self.assertFalse(check_env_variables(self.vars_to_check))

    def test_check_env_variables_empty_all_vars(
        self: "TestCheckEnvVariables",
    ) -> None:
        """Test that the check_env_variables function returns False when all required env variables are empty."""
        for var in self.vars_to_check:
            os.environ[var] = ""

        self.assertFalse(check_env_variables(self.vars_to_check))

    def tearDown(self: "TestCheckEnvVariables") -> None:
        """Remove all environment variables after each test."""
        try:
            for var in self.vars_to_check:
                os.unsetenv(var)
                del os.environ[var]
        except KeyError:
            pass

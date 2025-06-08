#!/usr/bin/env python3
"""
Test runner script for the BDM Movies DB project.

This script provides convenient ways to run tests with different configurations.
"""

import sys
import subprocess
import argparse


def run_command(command):
    """Run a command and return the result."""
    print(f"Running: {' '.join(command)}")
    result = subprocess.run(command, capture_output=True, text=True)

    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)

    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Run tests for BDM Movies DB")
    parser.add_argument(
        "--coverage", action="store_true", help="Run tests with coverage report"
    )
    parser.add_argument("--unit", action="store_true", help="Run only unit tests")
    parser.add_argument(
        "--integration", action="store_true", help="Run only integration tests"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--fast", action="store_true", help="Skip slow tests")
    parser.add_argument(
        "--parallel",
        "-n",
        type=int,
        help="Run tests in parallel (requires pytest-xdist)",
    )
    parser.add_argument(
        "test_path", nargs="?", help="Specific test file or directory to run"
    )

    args = parser.parse_args()

    # Build pytest command
    command = ["python", "-m", "pytest"]

    # Add verbosity
    if args.verbose:
        command.append("-v")

    # Add coverage
    if args.coverage:
        command.extend(["--cov=core", "--cov-report=html", "--cov-report=term-missing"])

    # Add test type filters
    if args.unit:
        command.extend(["-m", "unit"])
    elif args.integration:
        command.extend(["-m", "integration"])

    # Skip slow tests
    if args.fast:
        command.extend(["-m", "not slow"])

    # Parallel execution
    if args.parallel:
        command.extend(["-n", str(args.parallel)])

    # Add specific test path
    if args.test_path:
        command.append(args.test_path)
    else:
        command.append("tests/")

    # Run the tests
    return run_command(command)


if __name__ == "__main__":
    sys.exit(main())

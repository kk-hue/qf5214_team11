#!/usr/bin/env python

"""Tests for `qf5214_team11` package."""


import unittest
from click.testing import CliRunner

from qf5214_team11 import qf5214_team11
from qf5214_team11 import cli


class TestQf5214_team11(unittest.TestCase):
    """Tests for `qf5214_team11` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_000_something(self):
        """Test something."""

    def test_command_line_interface(self):
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        assert 'qf5214_team11.cli.main' in result.output
        help_result = runner.invoke(cli.main, ['--help'])
        assert help_result.exit_code == 0
        assert '--help  Show this message and exit.' in help_result.output

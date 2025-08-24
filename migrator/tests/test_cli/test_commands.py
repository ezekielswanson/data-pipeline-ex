import pytest
from click.testing import CliRunner

from migrator.cli.commands import cli

def test_test_command():
    """Test the test command"""
    runner = CliRunner()
    result = runner.invoke(cli, ['test'])
    assert result.exit_code == 0
    assert "Test command executed successfully" in result.output

# def test_extract_command(test_config_pack):
#     """Test the extract command"""
#     runner = CliRunner()
#     result = runner.invoke(cli, [
#         'extract',
#         '--object-type', 'contacts',
#         '--config-pack', str(test_config_pack)
#     ])
#     assert result.exit_code == 0

# def test_transform_command(test_config_pack):
#     """Test the transform command"""
#     runner = CliRunner()
#     result = runner.invoke(cli, [
#         'transform',
#         '--object-type', 'contacts',
#         '--config-pack', str(test_config_pack)
#     ])
#     assert result.exit_code == 0

# def test_load_command(test_config_pack):
#     """Test the load command"""
#     runner = CliRunner()
#     result = runner.invoke(cli, [
#         'load',
#         '--object-type', 'contacts',
#         '--config-pack', str(test_config_pack)
#     ])
#     assert result.exit_code == 0
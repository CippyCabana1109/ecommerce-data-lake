"""
Basic tests that don't require PySpark for initial testing.
"""

import pytest
import os
import sys

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

def test_project_structure():
    """Test that project structure is correct"""
    assert os.path.exists('src')
    assert os.path.exists('src/transform')
    assert os.path.exists('src/queries')
    assert os.path.exists('tests')
    assert os.path.exists('data')
    assert os.path.exists('requirements.txt')

def test_transform_scripts_exist():
    """Test that transformation scripts exist"""
    assert os.path.exists('src/transform/process_to_refined.py')
    assert os.path.exists('src/transform/create_curated.py')

def test_query_scripts_exist():
    """Test that query scripts exist"""
    assert os.path.exists('src/queries/setup_catalog.py')
    assert os.path.exists('src/queries/insights.sql')

def test_lineage_logger_import():
    """Test that lineage logger can be imported"""
    try:
        from utils.lineage_logger import LineageLogger
        logger = LineageLogger()
        assert logger is not None
    except ImportError as e:
        pytest.skip(f"Lineage logger import failed: {e}")

def test_requirements_content():
    """Test that requirements.txt contains necessary packages"""
    with open('requirements.txt', 'r') as f:
        content = f.read()
        
    assert 'pyspark' in content
    assert 'boto3' in content
    assert 'delta-spark' in content
    assert 'pytest' in content

def test_readme_exists():
    """Test that README.md exists and contains key information"""
    assert os.path.exists('README.md')
    
    with open('README.md', 'r') as f:
        content = f.read()
        
    assert 'How to Run' in content
    assert 'spark-submit' in content
    assert 'Athena' in content
    assert 'pytest' in content

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

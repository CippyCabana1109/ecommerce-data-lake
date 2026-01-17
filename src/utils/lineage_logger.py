"""
Lineage logging utility for data governance and audit trails.
Provides centralized logging for data pipeline operations.
"""

import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
import logging


class LineageLogger:
    """
    Centralized lineage logging for data pipeline governance.
    
    Tracks data movement, transformations, and quality metrics
    for compliance and audit purposes.
    """
    
    def __init__(self, log_dir: str = "data/lineage"):
        """
        Initialize lineage logger.
        
        Args:
            log_dir: Directory to store lineage logs
        """
        self.log_dir = log_dir
        self.ensure_log_directory()
        self.logger = self.setup_logger()
        
    def ensure_log_directory(self):
        """Ensure lineage log directory exists."""
        os.makedirs(self.log_dir, exist_ok=True)
        
    def setup_logger(self) -> logging.Logger:
        """Setup dedicated logger for lineage."""
        logger = logging.getLogger('lineage_logger')
        logger.setLevel(logging.INFO)
        
        # Create file handler
        log_file = os.path.join(self.log_dir, 'log.txt')
        handler = logging.FileHandler(log_file)
        handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        # Add handler to logger (avoid duplicates)
        if not logger.handlers:
            logger.addHandler(handler)
            
        return logger
        
    def log_transformation(self, 
                          job_name: str,
                          input_paths: list,
                          output_path: str,
                          transformations: list,
                          metrics: Dict[str, Any],
                          status: str = "SUCCESS") -> str:
        """
        Log a data transformation with full lineage information.
        
        Args:
            job_name: Name of the transformation job
            input_paths: List of input data paths
            output_path: Output data path
            transformations: List of transformation steps applied
            metrics: Dictionary of metrics (row counts, quality checks, etc.)
            status: Job status (SUCCESS, FAILED, PARTIAL)
            
        Returns:
            Path to the generated lineage log file
        """
        timestamp = datetime.now().isoformat()
        
        lineage_record = {
            'job_name': job_name,
            'timestamp': timestamp,
            'status': status,
            'input_paths': input_paths,
            'output_path': output_path,
            'transformations': transformations,
            'metrics': metrics,
            'data_lineage': {
                'source_system': 'ecommerce_data_lake',
                'pipeline_version': '1.0.0',
                'environment': os.getenv('ENVIRONMENT', 'development'),
                'executed_by': os.getenv('USER', 'system')
            }
        }
        
        # Generate log filename
        log_filename = f"{job_name}_{timestamp.replace(':', '-').replace('.', '-')}.json"
        log_filepath = os.path.join(self.log_dir, log_filename)
        
        # Write lineage record to file
        with open(log_filepath, 'w') as f:
            json.dump(lineage_record, f, indent=2, default=str)
            
        # Log to centralized log file
        self.logger.info(f"Transformation logged: {job_name} - {status}")
        self.logger.info(f"Input: {input_paths}")
        self.logger.info(f"Output: {output_path}")
        self.logger.info(f"Transformations: {len(transformations)} steps")
        self.logger.info(f"Metrics: {json.dumps(metrics, default=str)}")
        
        return log_filepath
        
    def log_data_quality(self,
                        job_name: str,
                        quality_checks: Dict[str, Any],
                        overall_status: str) -> str:
        """
        Log data quality check results.
        
        Args:
            job_name: Name of the job
            quality_checks: Dictionary of quality check results
            overall_status: Overall quality status
            
        Returns:
            Path to the quality log file
        """
        timestamp = datetime.now().isoformat()
        
        quality_record = {
            'job_name': job_name,
            'timestamp': timestamp,
            'quality_type': 'data_quality',
            'overall_status': overall_status,
            'checks': quality_checks,
            'compliance': {
                'data_governance': True,
                'quality_standards': overall_status == 'PASSED',
                'audit_required': overall_status != 'PASSED'
            }
        }
        
        # Generate log filename
        log_filename = f"quality_{job_name}_{timestamp.replace(':', '-').replace('.', '-')}.json"
        log_filepath = os.path.join(self.log_dir, log_filename)
        
        # Write quality record to file
        with open(log_filepath, 'w') as f:
            json.dump(quality_record, f, indent=2, default=str)
            
        # Log to centralized log file
        self.logger.info(f"Data quality logged: {job_name} - {overall_status}")
        
        return log_filepath
        
    def log_schema_change(self,
                          job_name: str,
                          table_name: str,
                          old_schema: Dict[str, Any],
                          new_schema: Dict[str, Any],
                          change_type: str = "EVOLUTION") -> str:
        """
        Log schema changes for governance.
        
        Args:
            job_name: Name of the job
            table_name: Name of the table
            old_schema: Previous schema definition
            new_schema: New schema definition
            change_type: Type of schema change
            
        Returns:
            Path to the schema change log file
        """
        timestamp = datetime.now().isoformat()
        
        schema_record = {
            'job_name': job_name,
            'timestamp': timestamp,
            'change_type': change_type,
            'table_name': table_name,
            'old_schema': old_schema,
            'new_schema': new_schema,
            'impact_analysis': {
                'breaking_changes': self._detect_breaking_changes(old_schema, new_schema),
                'backward_compatible': self._check_backward_compatibility(old_schema, new_schema)
            }
        }
        
        # Generate log filename
        log_filename = f"schema_{table_name}_{timestamp.replace(':', '-').replace('.', '-')}.json"
        log_filepath = os.path.join(self.log_dir, log_filename)
        
        # Write schema record to file
        with open(log_filepath, 'w') as f:
            json.dump(schema_record, f, indent=2, default=str)
            
        # Log to centralized log file
        self.logger.info(f"Schema change logged: {table_name} - {change_type}")
        
        return log_filepath
        
    def _detect_breaking_changes(self, old_schema: Dict, new_schema: Dict) -> list:
        """Detect breaking schema changes."""
        breaking_changes = []
        
        # Simple implementation - can be enhanced
        if 'fields' in old_schema and 'fields' in new_schema:
            old_fields = {f['name']: f for f in old_schema['fields']}
            new_fields = {f['name']: f for f in new_schema['fields']}
            
            # Check for removed fields
            for field_name in old_fields:
                if field_name not in new_fields:
                    breaking_changes.append(f"Removed field: {field_name}")
                    
            # Check for type changes
            for field_name in old_fields:
                if field_name in new_fields:
                    old_type = old_fields[field_name].get('type')
                    new_type = new_fields[field_name].get('type')
                    if old_type != new_type:
                        breaking_changes.append(f"Type change for {field_name}: {old_type} -> {new_type}")
                        
        return breaking_changes
        
    def _check_backward_compatibility(self, old_schema: Dict, new_schema: Dict) -> bool:
        """Check if schema change is backward compatible."""
        breaking_changes = self._detect_breaking_changes(old_schema, new_schema)
        return len(breaking_changes) == 0
        
    def get_lineage_summary(self, days: int = 7) -> Dict[str, Any]:
        """
        Get summary of lineage logs for the specified number of days.
        
        Args:
            days: Number of days to look back
            
        Returns:
            Summary statistics
        """
        cutoff_date = datetime.now().timestamp() - (days * 24 * 60 * 60)
        
        summary = {
            'period_days': days,
            'total_jobs': 0,
            'successful_jobs': 0,
            'failed_jobs': 0,
            'total_transformations': 0,
            'quality_checks_passed': 0,
            'quality_checks_failed': 0
        }
        
        # Scan lineage log files
        if os.path.exists(self.log_dir):
            for filename in os.listdir(self.log_dir):
                if filename.endswith('.json'):
                    filepath = os.path.join(self.log_dir, filename)
                    file_mtime = os.path.getmtime(filepath)
                    
                    if file_mtime >= cutoff_date:
                        try:
                            with open(filepath, 'r') as f:
                                record = json.load(f)
                                
                            if 'status' in record:
                                summary['total_jobs'] += 1
                                if record['status'] == 'SUCCESS':
                                    summary['successful_jobs'] += 1
                                else:
                                    summary['failed_jobs'] += 1
                                    
                            if 'transformations' in record:
                                summary['total_transformations'] += len(record['transformations'])
                                
                            if 'overall_status' in record:
                                if record['overall_status'] == 'PASSED':
                                    summary['quality_checks_passed'] += 1
                                else:
                                    summary['quality_checks_failed'] += 1
                                    
                        except Exception as e:
                            self.logger.warning(f"Error reading lineage file {filename}: {e}")
                            
        return summary


# Global lineage logger instance
_lineage_logger = None

def get_lineage_logger() -> LineageLogger:
    """Get global lineage logger instance."""
    global _lineage_logger
    if _lineage_logger is None:
        _lineage_logger = LineageLogger()
    return _lineage_logger

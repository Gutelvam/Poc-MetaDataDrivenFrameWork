"""
Data Quality Operators Module
Handles data quality checks with various rules and thresholds
"""

import pandas as pd
import numpy as np
import re
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import asdict

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from core.config import DataQualityRule

logger = logging.getLogger(__name__)

class DataQualityResult:
    """Represents the result of a data quality check"""
    
    def __init__(self, rule_name: str, rule_type: str, passed: bool, 
                 score: float, details: Dict[str, Any], severity: str = "warning"):
        self.rule_name = rule_name
        self.rule_type = rule_type
        self.passed = passed
        self.score = score  # Percentage score (0.0 to 1.0)
        self.details = details
        self.severity = severity
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'rule_name': self.rule_name,
            'rule_type': self.rule_type,
            'passed': self.passed,
            'score': self.score,
            'details': self.details,
            'severity': self.severity,
            'timestamp': self.timestamp.isoformat()
        }

class DataQualityChecker:
    """Core data quality checking logic"""
    
    @staticmethod
    def check_not_null(data: pd.DataFrame, rule: DataQualityRule) -> DataQualityResult:
        """Check for null values in specified column"""
        if rule.column not in data.columns:
            return DataQualityResult(
                rule.name, rule.rule_type, False, 0.0,
                {"error": f"Column '{rule.column}' not found in data"},
                rule.severity
            )
        
        total_rows = len(data)
        null_count = data[rule.column].isnull().sum()
        non_null_count = total_rows - null_count
        score = non_null_count / total_rows if total_rows > 0 else 1.0
        
        passed = score >= rule.threshold
        
        details = {
            "total_rows": total_rows,
            "null_count": int(null_count),
            "non_null_count": int(non_null_count),
            "null_percentage": (null_count / total_rows * 100) if total_rows > 0 else 0.0,
            "threshold": rule.threshold
        }
        
        return DataQualityResult(rule.name, rule.rule_type, passed, score, details, rule.severity)
    
    @staticmethod
    def check_unique(data: pd.DataFrame, rule: DataQualityRule) -> DataQualityResult:
        """Check for unique values in specified column"""
        if rule.column not in data.columns:
            return DataQualityResult(
                rule.name, rule.rule_type, False, 0.0,
                {"error": f"Column '{rule.column}' not found in data"},
                rule.severity
            )
        
        total_rows = len(data)
        unique_count = data[rule.column].nunique()
        duplicate_count = total_rows - unique_count
        score = unique_count / total_rows if total_rows > 0 else 1.0
        
        passed = score >= rule.threshold
        
        details = {
            "total_rows": total_rows,
            "unique_count": int(unique_count),
            "duplicate_count": int(duplicate_count),
            "uniqueness_percentage": (unique_count / total_rows * 100) if total_rows > 0 else 0.0,
            "threshold": rule.threshold
        }
        
        return DataQualityResult(rule.name, rule.rule_type, passed, score, details, rule.severity)
    
    @staticmethod
    def check_range(data: pd.DataFrame, rule: DataQualityRule) -> DataQualityResult:
        """Check if values are within specified range"""
        if rule.column not in data.columns:
            return DataQualityResult(
                rule.name, rule.rule_type, False, 0.0,
                {"error": f"Column '{rule.column}' not found in data"},
                rule.severity
            )
        
        params = rule.parameters or {}
        min_value = params.get('min_value')
        max_value = params.get('max_value')
        
        if min_value is None and max_value is None:
            return DataQualityResult(
                rule.name, rule.rule_type, False, 0.0,
                {"error": "Range check requires min_value and/or max_value parameters"},
                rule.severity
            )
        
        column_data = data[rule.column].dropna()  # Exclude null values
        total_non_null = len(column_data)
        
        if total_non_null == 0:
            return DataQualityResult(
                rule.name, rule.rule_type, True, 1.0,
                {"message": "No non-null values to check"},
                rule.severity
            )
        
        # Check range conditions
        valid_count = 0
        if min_value is not None and max_value is not None:
            valid_count = len(column_data[(column_data >= min_value) & (column_data <= max_value)])
        elif min_value is not None:
            valid_count = len(column_data[column_data >= min_value])
        elif max_value is not None:
            valid_count = len(column_data[column_data <= max_value])
        
        score = valid_count / total_non_null
        passed = score >= rule.threshold
        
        details = {
            "total_non_null_rows": total_non_null,
            "valid_count": int(valid_count),
            "invalid_count": int(total_non_null - valid_count),
            "validity_percentage": (valid_count / total_non_null * 100),
            "min_value": min_value,
            "max_value": max_value,
            "threshold": rule.threshold
        }
        
        return DataQualityResult(rule.name, rule.rule_type, passed, score, details, rule.severity)
    
    @staticmethod
    def check_pattern(data: pd.DataFrame, rule: DataQualityRule) -> DataQualityResult:
        """Check if values match a regex pattern"""
        if rule.column not in data.columns:
            return DataQualityResult(
                rule.name, rule.rule_type, False, 0.0,
                {"error": f"Column '{rule.column}' not found in data"},
                rule.severity
            )
        
        params = rule.parameters or {}
        pattern = params.get('pattern')
        
        if not pattern:
            return DataQualityResult(
                rule.name, rule.rule_type, False, 0.0,
                {"error": "Pattern check requires 'pattern' parameter"},
                rule.severity
            )
        
        column_data = data[rule.column].dropna().astype(str)
        total_non_null = len(column_data)
        
        if total_non_null == 0:
            return DataQualityResult(
                rule.name, rule.rule_type, True, 1.0,
                {"message": "No non-null values to check"},
                rule.severity
            )
        
        try:
            regex = re.compile(pattern)
            valid_count = column_data.str.match(regex).sum()
            score = valid_count / total_non_null
            passed = score >= rule.threshold
            
            details = {
                "total_non_null_rows": total_non_null,
                "valid_count": int(valid_count),
                "invalid_count": int(total_non_null - valid_count),
                "validity_percentage": (valid_count / total_non_null * 100),
                "pattern": pattern,
                "threshold": rule.threshold
            }
            
            return DataQualityResult(rule.name, rule.rule_type, passed, score, details, rule.severity)
            
        except re.error as e:
            return DataQualityResult(
                rule.name, rule.rule_type, False, 0.0,
                {"error": f"Invalid regex pattern: {str(e)}"},
                rule.severity
            )
    
    @staticmethod
    def check_completeness(data: pd.DataFrame, rule: DataQualityRule) -> DataQualityResult:
        """Check overall data completeness"""
        total_cells = data.size
        non_null_cells = data.count().sum()
        score = non_null_cells / total_cells if total_cells > 0 else 1.0
        
        passed = score >= rule.threshold
        
        details = {
            "total_cells": total_cells,
            "non_null_cells": int(non_null_cells),
            "null_cells": int(total_cells - non_null_cells),
            "completeness_percentage": (non_null_cells / total_cells * 100) if total_cells > 0 else 0.0,
            "threshold": rule.threshold
        }
        
        return DataQualityResult(rule.name, rule.rule_type, passed, score, details, rule.severity)
    
    @staticmethod
    def check_freshness(data: pd.DataFrame, rule: DataQualityRule) -> DataQualityResult:
        """Check data freshness based on timestamp column"""
        if rule.column not in data.columns:
            return DataQualityResult(
                rule.name, rule.rule_type, False, 0.0,
                {"error": f"Column '{rule.column}' not found in data"},
                rule.severity
            )
        
        params = rule.parameters or {}
        max_age_hours = params.get('max_age_hours', 24)
        
        try:
            # Convert to datetime if not already
            timestamp_col = pd.to_datetime(data[rule.column])
            current_time = datetime.now()
            
            # Calculate age in hours
            age_hours = (current_time - timestamp_col.max()).total_seconds() / 3600
            
            # Check if data is fresh enough
            is_fresh = age_hours <= max_age_hours
            score = max(0, 1 - (age_hours / max_age_hours)) if max_age_hours > 0 else 1.0
            passed = score >= rule.threshold
            
            details = {
                "latest_timestamp": timestamp_col.max().isoformat(),
                "current_timestamp": current_time.isoformat(),
                "age_hours": age_hours,
                "max_age_hours": max_age_hours,
                "is_fresh": is_fresh,
                "threshold": rule.threshold
            }
            
            return DataQualityResult(rule.name, rule.rule_type, passed, score, details, rule.severity)
            
        except Exception as e:
            return DataQualityResult(
                rule.name, rule.rule_type, False, 0.0,
                {"error": f"Failed to parse timestamps: {str(e)}"},
                rule.severity
            )
    
    @staticmethod
    def check_custom(data: pd.DataFrame, rule: DataQualityRule) -> DataQualityResult:
        """Execute custom data quality check"""
        params = rule.parameters or {}
        custom_function = params.get('function')
        
        if not custom_function:
            return DataQualityResult(
                rule.name, rule.rule_type, False, 0.0,
                {"error": "Custom check requires 'function' parameter"},
                rule.severity
            )
        
        try:
            # Execute custom function
            # This is a simplified implementation - in production, you'd want more security
            local_vars = {'data': data, 'rule': rule, 'pd': pd, 'np': np}
            exec(custom_function, {}, local_vars)
            
            result = local_vars.get('result')
            if not isinstance(result, dict):
                raise ValueError("Custom function must set 'result' dict with 'score' and 'details'")
            
            score = result.get('score', 0.0)
            details = result.get('details', {})
            passed = score >= rule.threshold
            
            return DataQualityResult(rule.name, rule.rule_type, passed, score, details, rule.severity)
            
        except Exception as e:
            return DataQualityResult(
                rule.name, rule.rule_type, False, 0.0,
                {"error": f"Custom function failed: {str(e)}"},
                rule.severity
            )

class DataQualityOperator(BaseOperator):
    """Operator for running data quality checks"""
    
    @apply_defaults
    def __init__(
        self,
        quality_rules: List[DataQualityRule],
        data_source_task_id: str = None,
        fail_on_error: bool = False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.quality_rules = quality_rules
        self.data_source_task_id = data_source_task_id
        self.fail_on_error = fail_on_error
        self.checker = DataQualityChecker()
    
    def execute(self, context):
        """Execute data quality checks"""
        # Get data from upstream task or context
        if self.data_source_task_id:
            data = context['task_instance'].xcom_pull(task_ids=self.data_source_task_id)
        else:
            data = context.get('data')
        
        if data is None:
            raise AirflowException("No data provided for quality checks")
        
        # Convert to DataFrame if needed
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise AirflowException(f"Unsupported data type for quality checks: {type(data)}")
        
        if df.empty:
            logger.warning("Empty dataset provided for quality checks")
            return {"message": "No data to check", "results": []}
        
        logger.info(f"Running {len(self.quality_rules)} data quality checks on {len(df)} rows")
        
        # Run all quality checks
        results = []
        failed_critical_checks = []
        
        for rule in self.quality_rules:
            try:
                result = self._run_quality_check(df, rule)
                results.append(result)
                
                # Log result
                status = "PASSED" if result.passed else "FAILED"
                logger.info(f"Quality check '{rule.name}' {status} with score {result.score:.2%}")
                
                # Track critical failures
                if not result.passed and rule.severity == "critical":
                    failed_critical_checks.append(result)
                    
            except Exception as e:
                logger.error(f"Quality check '{rule.name}' failed with error: {str(e)}")
                error_result = DataQualityResult(
                    rule.name, rule.rule_type, False, 0.0,
                    {"error": str(e)}, rule.severity
                )
                results.append(error_result)
                
                if rule.severity == "critical":
                    failed_critical_checks.append(error_result)
        
        # Calculate overall quality score
        total_score = sum(r.score for r in results) / len(results) if results else 0.0
        passed_checks = sum(1 for r in results if r.passed)
        
        summary = {
            "total_checks": len(results),
            "passed_checks": passed_checks,
            "failed_checks": len(results) - passed_checks,
            "overall_score": total_score,
            "results": [r.to_dict() for r in results],
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Data quality summary: {passed_checks}/{len(results)} checks passed, overall score: {total_score:.2%}")
        
        # Fail if critical checks failed and fail_on_error is True
        if failed_critical_checks and self.fail_on_error:
            critical_failures = [r.rule_name for r in failed_critical_checks]
            raise AirflowException(f"Critical data quality checks failed: {', '.join(critical_failures)}")
        
        return summary
    
    def _run_quality_check(self, df: pd.DataFrame, rule: DataQualityRule) -> DataQualityResult:
        """Run a single quality check"""
        if rule.rule_type == "not_null":
            return self.checker.check_not_null(df, rule)
        elif rule.rule_type == "unique":
            return self.checker.check_unique(df, rule)
        elif rule.rule_type == "range":
            return self.checker.check_range(df, rule)
        elif rule.rule_type == "pattern":
            return self.checker.check_pattern(df, rule)
        elif rule.rule_type == "completeness":
            return self.checker.check_completeness(df, rule)
        elif rule.rule_type == "freshness":
            return self.checker.check_freshness(df, rule)
        elif rule.rule_type == "custom":
            return self.checker.check_custom(df, rule)
        else:
            raise ValueError(f"Unsupported quality rule type: {rule.rule_type}")

class DataProfileOperator(BaseOperator):
    """Operator for data profiling and statistics"""
    
    @apply_defaults
    def __init__(
        self,
        data_source_task_id: str = None,
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data_source_task_id = data_source_task_id
        self.include_columns = include_columns
        self.exclude_columns = exclude_columns or []
    
    def execute(self, context):
        """Execute data profiling"""
        # Get data from upstream task or context
        if self.data_source_task_id:
            data = context['task_instance'].xcom_pull(task_ids=self.data_source_task_id)
        else:
            data = context.get('data')
        
        if data is None:
            raise AirflowException("No data provided for profiling")
        
        # Convert to DataFrame if needed
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise AirflowException(f"Unsupported data type for profiling: {type(data)}")
        
        if df.empty:
            return {"message": "No data to profile", "profile": {}}
        
        logger.info(f"Profiling dataset with {len(df)} rows and {len(df.columns)} columns")
        
        # Filter columns
        columns_to_profile = df.columns.tolist()
        
        if self.include_columns:
            columns_to_profile = [col for col in columns_to_profile if col in self.include_columns]
        
        columns_to_profile = [col for col in columns_to_profile if col not in self.exclude_columns]
        
        # Generate profile
        profile = {
            "dataset_info": {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "profiled_columns": len(columns_to_profile),
                "memory_usage": df.memory_usage(deep=True).sum(),
                "timestamp": datetime.now().isoformat()
            },
            "columns": {}
        }
        
        for column in columns_to_profile:
            profile["columns"][column] = self._profile_column(df[column])
        
        return profile
    
    def _profile_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile a single column"""
        column_profile = {
            "dtype": str(series.dtype),
            "count": len(series),
            "null_count": int(series.isnull().sum()),
            "null_percentage": (series.isnull().sum() / len(series) * 100),
            "unique_count": int(series.nunique()),
            "unique_percentage": (series.nunique() / len(series) * 100) if len(series) > 0 else 0
        }
        
        # Numeric column statistics
        if pd.api.types.is_numeric_dtype(series):
            non_null_series = series.dropna()
            if len(non_null_series) > 0:
                column_profile.update({
                    "min": float(non_null_series.min()),
                    "max": float(non_null_series.max()),
                    "mean": float(non_null_series.mean()),
                    "median": float(non_null_series.median()),
                    "std": float(non_null_series.std()) if len(non_null_series) > 1 else 0.0,
                    "quartiles": {
                        "q25": float(non_null_series.quantile(0.25)),
                        "q50": float(non_null_series.quantile(0.50)),
                        "q75": float(non_null_series.quantile(0.75))
                    }
                })
        
        # String column statistics
        elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
            non_null_series = series.dropna().astype(str)
            if len(non_null_series) > 0:
                lengths = non_null_series.str.len()
                column_profile.update({
                    "min_length": int(lengths.min()),
                    "max_length": int(lengths.max()),
                    "avg_length": float(lengths.mean()),
                    "most_common": non_null_series.value_counts().head(5).to_dict()
                })
        
        # Datetime column statistics
        elif pd.api.types.is_datetime64_any_dtype(series):
            non_null_series = series.dropna()
            if len(non_null_series) > 0:
                column_profile.update({
                    "min_date": non_null_series.min().isoformat(),
                    "max_date": non_null_series.max().isoformat(),
                    "date_range_days": (non_null_series.max() - non_null_series.min()).days
                })
        
        return column_profile

def create_data_quality_operator(
    task_id: str,
    quality_rules: List[DataQualityRule],
    data_source_task_id: str,
    dag,
    fail_on_error: bool = False,
    **kwargs
) -> DataQualityOperator:
    """Factory function to create data quality operator"""
    
    return DataQualityOperator(
        task_id=task_id,
        quality_rules=quality_rules,
        data_source_task_id=data_source_task_id,
        fail_on_error=fail_on_error,
        dag=dag,
        **kwargs
    )

def create_data_profile_operator(
    task_id: str,
    data_source_task_id: str,
    dag,
    include_columns: Optional[List[str]] = None,
    exclude_columns: Optional[List[str]] = None,
    **kwargs
) -> DataProfileOperator:
    """Factory function to create data profiling operator"""
    
    return DataProfileOperator(
        task_id=task_id,
        data_source_task_id=data_source_task_id,
        include_columns=include_columns,
        exclude_columns=exclude_columns,
        dag=dag,
        **kwargs
    )
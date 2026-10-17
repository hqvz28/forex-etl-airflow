"""
Unit tests for Apache Airflow DAG integrity, syntax validation, and best practices.
"""

import ast
import glob
import os
import unittest


class TestAirflowDAGIntegrity(unittest.TestCase):

    def setUp(self):
        self.dag_files = glob.glob("dags/*.py")

    def test_dag_files_exist(self):
        self.assertGreater(len(self.dag_files), 0, "No DAG python files found in dags/ folder.")

    def test_dag_python_syntax(self):
        """Verify that every python DAG file parses cleanly without syntax errors."""
        for dag_file in self.dag_files:
            with self.subTest(dag_file=dag_file):
                with open(dag_file, "r", encoding="utf-8") as f:
                    content = f.read()
                try:
                    ast.parse(content)
                except SyntaxError as e:
                    self.fail(f"Syntax error found in DAG file {dag_file}: {e}")

    def test_no_hardcoded_credentials_in_dags(self):
        """Assert that no secret keys or bot tokens are hardcoded in DAG files."""
        forbidden_patterns = ["AAHPfeOoyD4voAJqmWozyrd5x7LSMMBh5Ew", "S6sSSIezsruz8aOthFKyPaPGoHKRj4HS"]
        for dag_file in self.dag_files:
            with open(dag_file, "r", encoding="utf-8") as f:
                content = f.read()
            for pattern in forbidden_patterns:
                self.assertNotIn(
                    pattern, content,
                    f"Forbidden credential pattern detected in {dag_file}!"
                )

    def test_taskflow_dag_structure(self):
        """Verify TaskFlow DAG contains expected decorators and tasks."""
        taskflow_path = "dags/forex_etl_taskflow.py"
        if os.path.exists(taskflow_path):
            with open(taskflow_path, "r", encoding="utf-8") as f:
                content = f.read()
            self.assertIn("@dag(", content)
            self.assertIn("@task", content)
            self.assertIn("fetch_rates_with_fallback", content)
            self.assertIn("validate_data_quality", content)
            self.assertIn("transform_star_schema", content)


if __name__ == "__main__":
    unittest.main()

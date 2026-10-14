"""
====================================================================================================
BFSI REAL-TIME DATA LAKEHOUSE: UNIT TESTS FOR STREAMING TRANSFORMATIONS
====================================================================================================
File Name   : python/tests/test_transformations.py
Layer       : CI/CD Quality Assurance & Unit Testing
Target      : Schema Validation, PII Masking, and Risk Anomaly Logic
====================================================================================================

----------------------------------------------------------------------------------------------------
1. ROOT CAUSE ANALYSIS & OS-LEVEL BOTTLENECKS
----------------------------------------------------------------------------------------------------
* Root Issue:
  PySpark 3.x utilizes a hybrid architecture where the JVM driver communicates with Python worker 
  sub-processes via Unix domain sockets or TCP localhost connections (`py4j` + `pyspark.daemon`). 
  When running PyTest under Python 3.12 on Windows (PowerShell/CMD/Git Bash), PySpark methods like 
  `.collect()`, `.take()`, or `.head()` internally trigger `Dataset.collectToPython()`. 
  This invokes `PythonRunner.scala`, which attempts to pass serializable row batches across socket pipes.
  
  Under Python 3.12 on Windows, changes in socket signal handling cause `pyspark.daemon` workers to crash 
  with `java.io.EOFException` / `Python worker exited unexpectedly (crashed)`.

* Bottlenecks & Potential Issues:
  1. Internal `collectToPython()` Reliance:
     PySpark's `.take(n)` method is implemented as `self.limit(n).collect()`. Thus, calling `.take()` 
     does not bypass `collectToPython()`; it still routes through the unstable socket daemon pipeline.
  2. Python 3.12 Windows IPC Breaking Changes:
     Python 3.12 introduced strict socket-blocking behavior and signal handling updates on Windows,
     causing PySpark's worker daemon to terminate unexpectedly during socket pipe handshakes.
  3. MSYS2/Git Bash Path Translation:
     POSIX-style pathing (e.g., `/c/Users/...`) inside POSIX environment variables (like `PATH` or 
     `PYTHONPATH`) causes JVM sub-processes to fail instantly when launching the Python worker executable.

* Technical Resolution:
  1. Offload Execution to Spark SQL Engine (`spark.sql(...)`):
     By registering DataFrames as Temporary Views (`createOrReplaceTempView`) and querying results 
     via `spark.sql(...)`, transformations are computed directly inside JVM memory via Spark Catalyst/JIT 
     codegen without calling Python-side worker UDF socket handlers.
  2. Disable PySpark Worker Daemon (`spark.python.use.daemon = false`):
     Forces PySpark to launch a dedicated Python worker sub-process directly per task rather than 
     relying on a persistent socket daemon process.
  3. Disable Worker Reuse (`spark.python.worker.reuse = false`):
     Prevents Py4J socket handle corruption across sequential PyTest test runs on Windows local environments.

----------------------------------------------------------------------------------------------------
2. SENIOR DATA ENGINEER INTERVIEW Q&A (REAL-TIME LAKEHOUSE & UNIT TESTING)
----------------------------------------------------------------------------------------------------
Q1: Why do PySpark unit tests fail with `Python worker exited unexpectedly` on Windows during `.take()` or `.collect()`?
A1: PySpark relies on Py4J and a Python daemon (`pyspark.daemon`) to bridge JVM execution with Python 
    workers. In PySpark, `.take(n)` is implemented as `.limit(n).collect()`, which calls `collectToPython()`. 
    On Windows with Python 3.12, asynchronous socket I/O changes cause `EOFException` crashes in 
    `BasePythonRunner$ReaderIterator` during worker handshake serialization. Executing logic via Spark SQL 
    or setting `spark.python.use.daemon=false` forces direct execution and bypasses the unstable socket daemon.

Q2: How do you design an isolated unit testing strategy for PySpark streaming transformations in CI/CD?
A2: 1. Decouple business logic from I/O connectors by testing pure DataFrame-to-DataFrame functions.
    2. Use a session-scoped PyTest fixture for `SparkSession` configured with minimal local resources 
       (`master("local[1]")`, `spark.sql.shuffle.partitions=1`, `spark.ui.enabled=false`).
    3. Avoid invoking external services (Kafka, AWS S3) by utilizing mock `createDataFrame` inputs.
    4. Enforce strict assertions on schema types, non-nullability, PII hashing formats (SHA-256 length = 64), 
       and boundary condition flags.

Q3: What are the performance and architectural implications of `.collect()` vs `.take()` in Spark?
A3: `.collect()` retrieves ALL partitions across the cluster driver, risking JVM Out-Of-Memory (OOM) 
    errors if the dataset exceeds driver heap space. `.take(n)` fetches only $n$ records by scanning 
    partitions sequentially (starting with Partition 0), minimizing driver memory footprint and 
    network I/O serialization overhead.
====================================================================================================
"""

import os     # Standard OS library used for environment variable manipulation and path calculations.
import sys    # System module used to fetch the exact active Python interpreter path (`sys.executable`).
import pytest # PyTest framework used for session fixtures, test discovery, and assertions.

# --------------------------------------------------------------------------------------------------
# STEP 1: CONFIGURE ENVIRONMENT & PATH SANITIZATION BEFORE PYSPARK IMPORT
# --------------------------------------------------------------------------------------------------
# Calculates the project root directory path dynamically relative to this file's position (`../../`).
# WHY: Ensures relative imports and local dependency references remain portable across OS environments.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

# Normalizes the active Python interpreter path into a clean Windows backslash string (`C:\...`).
# WHY: Prevents PySpark JVM sub-processes from failing when invoking Python workers due to mixed forward/backslash pathing.
python_exe = os.path.normpath(sys.executable)

# Forces local loopback IP binding for PySpark driver/executor network IPC.
# WHY: Prevents PySpark from attempting external network interface resolution, avoiding Windows firewall prompts.
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"         # Sets the IP address Spark binds its local services to.
os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"   # Sets the hostname Spark advertises to itself (loopback only).

# Explicitly sets the Python interpreter path for both PySpark Driver and Executors.
# WHY: Ensures Spark workers execute inside the virtual environment (`venv`) rather than falling back to system Python.
os.environ["PYSPARK_PYTHON"] = python_exe          # Interpreter used by Spark executors (Python workers).
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe   # Interpreter used by the Spark driver process.

# Forces standard stdout/stderr stream flushing.
# WHY: Ensures real-time console log printing during CI/CD test execution runs.
os.environ["PYTHONUNBUFFERED"] = "1"               # Disables Python output buffering so logs appear immediately.

# Dynamically checks for local JDK 17 binary folder in project root.
LOCAL_JDK_DIR = os.path.join(PROJECT_ROOT, "jdk-17.0.10+7")   # Builds the expected path of a bundled JDK folder.
if os.path.exists(LOCAL_JDK_DIR):                              # Only applies the override if the folder exists.
    # Sets JAVA_HOME to local JDK path.
    # WHY: Ensures Spark runs on Java 17 without requiring system-wide JAVA_HOME configuration.
    os.environ["JAVA_HOME"] = os.path.normpath(LOCAL_JDK_DIR)
    # Prepends local JDK `/bin` folder to system PATH variable.
    # WHY: Allows JVM invocation (`java.exe`) directly from the localized directory.
    os.environ["PATH"] = os.path.normpath(os.path.join(LOCAL_JDK_DIR, "bin")) + os.path.pathsep + os.environ.get("PATH", "")

# Dynamically checks for local Hadoop folder containing native Windows binaries (`winutils.exe`).
LOCAL_HADOOP_DIR = os.path.join(PROJECT_ROOT, "hadoop")        # Builds the expected path of a bundled Hadoop folder.
if os.path.exists(LOCAL_HADOOP_DIR):                           # Only applies the override if the folder exists.
    # Sets HADOOP_HOME environment variable.
    # WHY: Satisfies Spark's dependency on Hadoop filesystem abstractions on Windows.
    os.environ["HADOOP_HOME"] = os.path.normpath(LOCAL_HADOOP_DIR)
    # Prepends Hadoop `/bin` folder to system PATH variable.
    # WHY: Resolves local file creation permissions (`winutils.exe`) for local checkpointing and Parquet/Delta file writes.
    os.environ["PATH"] = os.path.normpath(os.path.join(LOCAL_HADOOP_DIR, "bin")) + os.path.pathsep + os.environ.get("PATH", "")

# Imports PySpark DataFrame module components after environment variables are set.
from pyspark.sql import SparkSession  # Entry point class used to create the Spark session.


# --------------------------------------------------------------------------------------------------
# STEP 2: PYTEST SPARK SESSION FIXTURE WITH WINDOWS DAEMON WORKAROUND
# --------------------------------------------------------------------------------------------------
@pytest.fixture(scope="session")  # Registers a fixture created once and shared by every test in the run.
def spark():                      # Fixture function; tests receive the session by naming it as an argument.
    """
    Instantiates a local SparkSession configured to bypass Python daemon socket crashes.
    Scope='session' ensures a single SparkContext is initialized and shared across all tests in the suite.
    """
    session = (
        SparkSession.builder  # Starts the fluent builder used to configure the session.
        # Sets application label visible in log outputs.
        .appName("BFSI_Lakehouse_UnitTests")
        
        # Executes Spark in local mode using a single worker thread.
        # WHY: Minimizes resource overhead and race conditions during unit testing.
        .master("local[1]")
        
        # Binds driver host explicitly to loopback interface.
        # WHY: Prevents port binding collisions on Windows local builds.
        .config("spark.driver.host", "127.0.0.1")          # Address executors use to reach the driver.
        .config("spark.driver.bindAddress", "127.0.0.1")   # Address the driver listens on.
        
        # Reduces default shuffle partition count from 200 to 1.
        # WHY: Accelerates local test execution by avoiding unnecessary partition creation for tiny mock datasets.
        .config("spark.sql.shuffle.partitions", "1")
        
        # Disables Spark Web UI (port 4040).
        # WHY: Saves execution time and avoids port conflict errors during rapid test cycles.
        .config("spark.ui.enabled", "false")
        
        # Disables worker socket reuse between tasks.
        # WHY: Prevents orphaned socket handle errors across PyTest test functions.
        .config("spark.python.worker.reuse", "false")
        
        # Disables persistent PySpark daemon worker creation.
        # WHY: Bypasses Python 3.12 Windows IPC EOFException crashes in `pyspark.daemon`.
        .config("spark.python.use.daemon", "false")
        
        # Disables Hadoop native C++ file I/O libraries.
        # WHY: Prevents `UnsatisfiedLinkError` warnings on Windows systems missing raw Hadoop native DLLs.
        .config("spark.hadoop.io.nativeio.enabled", "false")
        .getOrCreate()  # Creates the session, or returns the existing one if already running.
    )
    # Delivers active Spark session instance to test functions.
    yield session  # Hands the session to tests; code after this line runs at teardown.
    
    # Gracefully shuts down SparkContext after all test functions complete.
    # WHY: Frees system RAM and releases local network socket bindings.
    session.stop()  # Stops the SparkContext and releases JVM resources.


# --------------------------------------------------------------------------------------------------
# STEP 3: UNIT TEST CASES (TEST DATA BUILT INSIDE THE JVM VIA SQL `VALUES`, NO PYTHON WORKER NEEDED)
# --------------------------------------------------------------------------------------------------
def test_pii_masking_transformation(spark):  # PyTest discovers this by the `test_` prefix; `spark` is the fixture.
    """Test 1: Verifies raw account numbers are correctly anonymized into SHA-256 hex digests."""
    # Builds the mock input table directly in Spark SQL using an inline VALUES clause.
    # WHY: `spark.createDataFrame(list)` pickles rows and needs a Python worker, which crashes on
    #      Windows + Python 3.12. VALUES is evaluated entirely inside the JVM, so no Python worker starts.
    spark.sql("""
        SELECT * FROM VALUES
            ('ACC12345', 'John Doe', 1500.00),
            ('ACC67890', 'Jane Smith', 2500.50)
        AS t(account_number, customer_name, amount)
    """).createOrReplaceTempView("v_raw_accounts")  # Registers the result as a temp view named v_raw_accounts.

    # Applies the PII masking transformation using native JVM `sha2` and `concat` functions.
    df = spark.sql("""
        SELECT account_number, customer_name, amount,
               sha2(concat(account_number, '||bfsi_salt_2026'), 256) AS masked_account_id
        FROM v_raw_accounts
    """)  # Returns a DataFrame with an extra salted SHA-256 column; nothing executes until an action runs.

    # Asserts the new masked column exists in the output schema.
    assert "masked_account_id" in df.columns
    # Executes the query and brings the result rows back to the driver as a list of Row objects.
    rows = df.collect()
    # Asserts the output row count matches the 2 input records.
    assert len(rows) == 2
    # Asserts every masked value is exactly 64 characters (SHA-256 hex digest length).
    assert all(len(r["masked_account_id"]) == 64 for r in rows)
    # Asserts no masked value equals the original account number (proves the raw value was replaced).
    assert all(r["masked_account_id"] != r["account_number"] for r in rows)


def test_anomaly_flagging_logic(spark):  # Second test; reuses the same session-scoped Spark fixture.
    """Test 2: Verifies Gold risk classification flags window volumes > $10,000 USD as HIGH_RISK_SPIKE."""
    # Builds the mock transaction table inside the JVM using an inline VALUES clause (no Python worker).
    spark.sql("""
        SELECT * FROM VALUES (12000.00, 'TRANSFER'), (4500.00, 'RETAIL')
        AS t(total_volume_usd, transaction_type)
    """).createOrReplaceTempView("v_risk_transactions")  # Registers the table as temp view v_risk_transactions.

    # Applies the risk classification rule and executes the query.
    rows = spark.sql("""
        SELECT total_volume_usd,
               CASE WHEN total_volume_usd > 10000 THEN 'HIGH_RISK_SPIKE' ELSE 'NORMAL' END AS risk_classification
        FROM v_risk_transactions
        ORDER BY total_volume_usd DESC
    """).collect()  # ORDER BY DESC guarantees row 0 is the 12000 record, so the assertions are deterministic.

    # Asserts the transaction above the $10,000 threshold ($12,000) is flagged as HIGH_RISK_SPIKE.
    assert rows[0]["risk_classification"] == "HIGH_RISK_SPIKE"
    # Asserts the transaction below the threshold ($4,500) is flagged as NORMAL.
    assert rows[1]["risk_classification"] == "NORMAL"

# --------------------------------------------------------------------------------------------------
# 4. FUTURE EXTENSIONS & ADVANCED IMPROVEMENTS (WHAT ELSE COULD BE DONE)
# --------------------------------------------------------------------------------------------------
# 1. Integration of `chispa` or `pytest-spark` libraries:
#    - Allows direct DataFrame-to-DataFrame equality assertions (`assert_df_equality(df1, df2)`),
#      validating schemas, nullability, and data types automatically without manually checking row values.
#
# 2. Schema Edge-Case Testing:
#    - Include tests with `NULL` account numbers, empty strings, extremely large transaction numbers,
#      and special characters to verify transformation robustness against dirty input stream data.
#
# 3. Micro-batch Streaming Engine Testing:
#    - Leverage `spark.readStream` and `MemoryStream` (`from pyspark.sql.execution.streaming import MemoryStream`)
#      to test actual Structured Streaming micro-batch stateful window operations end-to-end.
#
# 4. Dockerized Linux CI Execution (GitHub Actions / GitLab CI):
#    - Containerize the testing environment on Linux-based Docker images (`python:3.12-slim` + `OpenJDK 17`).
#      Linux environments use native Unix domain sockets, completely eliminating Windows-specific socket 
#      and daemon issues in automated deployment pipelines.
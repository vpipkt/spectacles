from dataclasses import dataclass
from typing import Union, Dict, Any, List, Optional
import time
from tabulate import tabulate
from spectacles.validators.validator import Validator, Test
from spectacles.client import LookerClient
from spectacles.lookml import Dimension, Explore
from spectacles.types import QueryMode
from spectacles.exceptions import SpectaclesException, SqlError
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.printer import print_header


@dataclass
class SqlTest(Test):
    query_id: int
    lookml_ref: Union[Dimension, Explore]
    explore_url: str
    sql: str
    query_task_id: Optional[str] = None
    status: Optional[str] = None
    runtime: Optional[float] = None
    result: Optional[SqlError] = None

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            return self.sql == other.sql
        else:
            return False

    def __hash__(self) -> int:
        return self.sql.__hash__()

    def get_profile_result(self) -> List:
        return [
            self.lookml_ref.__class__.__name__.lower(),
            self.lookml_ref.name,
            self.runtime,
            self.query_id,
            self.explore_url,
        ]


@dataclass
class QueryResult:
    """Stores ID, query status, and error details for a completed query task"""

    query_task_id: str
    status: str
    runtime: Optional[float] = None
    error: Optional[Dict[str, Any]] = None


class SqlValidator(Validator):
    """Runs and validates the SQL for each selected LookML dimension.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.
        concurrency: The number of simultaneous queries to run.
        runtime_threshold: When profiling, only display queries lasting longer
            than this.

    Attributes:
        project: LookML project object representation.
        query_tasks: Mapping of query task IDs to LookML objects

    """

    def __init__(
        self,
        client: LookerClient,
        project: str,
        concurrency: int = 10,
        runtime_threshold: int = 5,
    ):
        super().__init__(client, project)
        self.query_slots = concurrency
        self.runtime_threshold = runtime_threshold
        self._running_tests: List[SqlTest] = []
        # Lookup used to retrieve the LookML object
        self._test_by_task_id: Dict[str, SqlTest] = {}
        self._long_running_tests: List[SqlTest] = []

    def build_project(
        self,
        selectors: Optional[List[str]] = None,
        exclusions: Optional[List[str]] = None,
        build_dimensions: bool = True,
    ) -> None:
        super().build_project(selectors, exclusions, build_dimensions)

    def generate_tests(self, mode: QueryMode = "batch") -> List[SqlTest]:
        tests: List[SqlTest] = []
        for model in self.project.models:
            for explore in model.explores:
                if mode == "batch" or (mode == "hybrid" and not explore.queried):
                    test = self._create_explore_test(explore, model.name)
                    tests.append(test)
                elif mode == "single" or (mode == "hybrid" and explore.errored):
                    dimension_tests = self._create_dimension_tests(explore, model.name)
                    tests.extend(dimension_tests)
        return tests

    def run_tests(self, tests: List[SqlTest], profile: bool = False):
        self._test_by_task_id = {}
        try:
            self._run_tests(tests)
        except KeyboardInterrupt:
            logger.info(
                "\n\n" + "Please wait, asking Looker to cancel any running queries..."
            )
            query_tasks = self._get_running_query_tasks()
            self._cancel_queries(query_tasks)
            if query_tasks:
                message = (
                    f"Attempted to cancel {len(query_tasks)} running "
                    f"{'query' if len(query_tasks) == 1 else 'queries'}."
                )
            else:
                message = (
                    "No queries were running at the time so nothing was cancelled."
                )
            raise SpectaclesException(
                name="validation-keyboard-interrupt",
                title="SQL validation was manually interrupted.",
                detail=message,
            )

        if profile:
            self._print_profile_results()

    def _get_running_query_tasks(self) -> List[str]:
        return [
            test.query_task_id for test in self._running_tests if test.query_task_id
        ]

    def _create_explore_test(self, explore: Explore, model_name: str) -> SqlTest:
        """Creates a SqlTest to query all dimensions in an explore"""
        dimensions = [dimension.name for dimension in explore.dimensions]
        query = self.client.create_query(
            model_name, explore.name, dimensions, fields=["id", "share_url"]
        )
        sql = self.client.run_query(query["id"])
        return SqlTest(
            query["id"], lookml_ref=explore, explore_url=query["share_url"], sql=sql
        )

    def _create_dimension_tests(
        self, explore: Explore, model_name: str
    ) -> List[SqlTest]:
        """Creates individual queries for each dimension in an explore"""
        tests: List[SqlTest] = []
        for dimension in explore.dimensions:
            query = self.client.create_query(
                model_name, explore.name, [dimension.name], fields=["id", "share_url"]
            )
            sql = self.client.run_query(query["id"])
            test = SqlTest(
                query["id"],
                lookml_ref=dimension,
                explore_url=query["share_url"],
                sql=sql,
            )
            tests.append(test)
        return tests

    def _run_tests(self, tests: List[SqlTest]) -> None:
        """Creates and runs tests with a maximum concurrency defined by query slots"""
        QUERY_TASK_LIMIT = 250

        while tests or self._running_tests:
            if tests:
                logger.debug(f"Starting a new loop, {len(tests)} tests queued")
                self._fill_query_slots(tests)
            query_tasks = self._get_running_query_tasks()[:QUERY_TASK_LIMIT]
            logger.debug(f"Checking for results of {len(query_tasks)} query tasks")
            for query_result in self._get_query_results(query_tasks):
                if query_result.status in ("complete", "error"):
                    self._handle_query_result(query_result)
            time.sleep(0.5)

    def _fill_query_slots(self, tests: List[SqlTest]) -> None:
        """Creates query tasks until all slots are used or all tests are running"""
        while tests and self.query_slots > 0:
            logger.debug(
                f"{self.query_slots} available query slots, creating query task"
            )
            test = tests.pop(0)
            query_task_id = self.client.create_query_task(test.query_id)
            self.query_slots -= 1
            test.query_task_id = query_task_id
            self._test_by_task_id[query_task_id] = test
            self._running_tests.append(test)

    def _get_query_results(self, query_task_ids: List[str]) -> List[QueryResult]:
        """Returns ID, status, and error message for all query tasks"""
        query_results = []
        results = self.client.get_query_task_multi_results(query_task_ids)
        for query_task_id, result in results.items():
            status = result["status"]
            if status not in ("complete", "error", "running", "added", "expired"):
                raise SpectaclesException(
                    name="unexpected-query-result-status",
                    title="Encountered an unexpected query result status.",
                    detail=(
                        f"Query result status '{status}' was returned "
                        "by the Looker API."
                    ),
                )
            logger.debug(f"Query task {query_task_id} status is: {status}")

            try:
                runtime: Optional[float] = float(result["data"]["runtime"])
            except KeyError:
                runtime = None

            query_result = QueryResult(query_task_id, status, runtime)
            if status == "error":
                try:
                    error_details = self._extract_error_details(result)
                except (KeyError, TypeError, IndexError) as error:
                    logger.debug(
                        f"Exiting because of unexpected query result format: {result}"
                    )
                    raise SpectaclesException(
                        name="unexpected-query-result-format",
                        title="Encountered an unexpected query result format.",
                        detail="Unable to extract error details. The unexpected result has been logged.",
                    ) from error
                else:
                    query_result.error = error_details
            query_results.append(query_result)
        return query_results

    def _handle_query_result(self, result: QueryResult) -> None:
        test = self._test_by_task_id[result.query_task_id]
        self._running_tests.remove(test)
        self.query_slots += 1
        test.status = result.status
        test.runtime = result.runtime
        lookml_object = test.lookml_ref
        lookml_object.queried = True

        if result.runtime and result.runtime >= self.runtime_threshold:
            self._long_running_tests.append(test)

        if result.status == "error" and result.error:
            model_name = lookml_object.model_name
            dimension_name: Optional[str] = None
            if isinstance(lookml_object, Dimension):
                explore_name = lookml_object.explore_name
                dimension_name = lookml_object.name
            else:
                explore_name = lookml_object.name

            sql_error = SqlError(
                model=model_name,
                explore=explore_name,
                dimension=dimension_name,
                explore_url=test.explore_url,
                lookml_url=getattr(lookml_object, "url", None),
                **result.error,
            )
            test.result = sql_error
            lookml_object.errors.append(sql_error)

    @staticmethod
    def _extract_error_details(query_result: Dict) -> Optional[Dict]:
        """Extracts the relevant error fields from a Looker API response"""
        data = query_result["data"]
        if isinstance(data, dict):
            errors = data.get("errors") or [data.get("error")]
            try:
                first_error = next(
                    error
                    for error in errors
                    if error.get("message")
                    != "Note: This query contains derived tables with conditional SQL for Development Mode. "
                    "Query results in Production Mode might be different."
                )
            except StopIteration:
                return None
            message = " ".join(
                filter(
                    None,
                    [first_error.get("message"), first_error.get("message_details")],
                )
            )
            sql = data.get("sql")
            error_loc = first_error.get("sql_error_loc")
            if error_loc:
                line_number = error_loc.get("line")
            else:
                line_number = None
        elif isinstance(data, list):
            message = data[0]
            line_number = None
            sql = None
        else:
            raise TypeError(
                "Unexpected error response type. "
                "Expected a dict or a list, "
                f"received type {type(data)}: {data}"
            )

        return {"message": message, "sql": sql, "line_number": line_number}

    def _cancel_queries(self, query_task_ids: List[str]) -> None:
        """Asks the Looker API to cancel specified queries"""
        for query_task_id in query_task_ids:
            self.client.cancel_query_task(query_task_id)

    def _print_profile_results(self) -> None:
        HEADER_CHAR = "."
        print_header("Query profiler results", char=HEADER_CHAR, leading_newline=False)
        if self._long_running_tests:
            tests_by_runtime = sorted(
                self._long_running_tests,
                key=lambda x: x.runtime if x.runtime is not None else -1,
                reverse=True,
            )
            profile_results = [test.get_profile_result() for test in tests_by_runtime]
            output = tabulate(
                profile_results,
                headers=[
                    "Type",
                    "Name",
                    "Runtime (s)",
                    "Query ID",
                    "Explore From Here",
                ],
                tablefmt="github",
                numalign="left",
                floatfmt=".1f",
            )
        else:
            output = (
                f"All queries completed in less than {self.runtime_threshold} "
                "seconds."
            )
        logger.info(output)
        print_header(HEADER_CHAR, char=HEADER_CHAR)

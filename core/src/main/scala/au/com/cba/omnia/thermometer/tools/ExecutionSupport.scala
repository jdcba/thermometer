//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.thermometer.tools

import scala.util.{Try, Success, Failure}

import scalaz._, Scalaz._

import com.twitter.scalding._

import org.apache.hadoop.mapred.JobConf

import org.apache.log4j.LogManager

import org.specs2.Specification
import org.specs2.matcher.MatchResult
import org.specs2.execute.{Result, Success => SpecsSuccess, Failure => SpecsFailure, FailureException}
import org.specs2.execute.StandardResults.success

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.fact.Fact

/** Adds testing support for scalding execution monad by setting up a test `Config` and `Mode`.*/
trait ExecutionSupport extends FieldConversions with HadoopSupport { self: Specification =>
  /**
   * Executes the provided execution.
   *
   * The provided `Execution` is run in HDFS mode, using supplied arguments and optional additional
   * configuration. The method returns a `Try[T]` containing the result after waiting for the
   * execution to finish.
   *
   * `extraConfig` should be used with caution since it can potentially override important
   * parameters of the `Execution`, such as the mode and arguments. A useful purpose for
   * `extraConfig` is to add custom serialization for the execution.
   *
   * @tparam T type of the execution (the type of its result)
   * @param execution execution to execute
   * @param args arguments passed to `Execution.run` (placed in the Scalding `Args`
   *        variable of the `Config`)
   * @param extraConfig extra configuration passed to `Execution.run` (appended to the
   *        `Config` that would otherwise be used using the `++` method)
   * @return `Try[T]` containing the result of waiting for the execution to complete
   */
  def execute[T](
    execution: Execution[T],
    args: Map[String, List[String]] = Map.empty,
    extraConfig: Config = Config.empty
  ): Try[T] = {
    val log = LogManager.getLogger(getClass)
    log.info("")
    log.info("")
    log.info(s"============================   Running execution test with work directory <$dir>  ============================")
    log.info("")
    log.info("")

    val mode   = Hdfs(false, jobConf)
    val a      = Mode.putMode(mode, new Args(args + (("hdfs", List.empty))))
    val config =
      Config
        .hadoopWithDefaults(jobConf)
        .setArgs(a)
        .++(extraConfig)

    Executions.runExecution[T](config, mode, execution)
  }

  /**
   * Checks that the provided execution executed successfully.
   *
   * The provided `Execution` is run in HDFS mode, using supplied arguments and optional additional
   * configuration. Any return value from the execution is discarded.
   *
   * `extraConfig` should be used with caution since it can potentially override important
   * parameters of the `Execution`, such as the mode and arguments. A useful purpose for
   * `extraConfig` is to add custom serialization for the execution.
   *
   * @tparam T type of the execution (the type of its result)
   * @param execution execution to execute
   * @param args arguments passed to `Execution.run` (placed in the Scalding `Args`
   *        variable of the `Config`)
   * @param extraConfig extra configuration passed to `Execution.run` (appended to the
   *        `Config` that would otherwise be used using the `++` method)
   * @return `Result` of running the execution (discarding the value produced)
   */
  def executesOk(
    execution: Execution[_],
    args: Map[String, List[String]] = Map.empty,
    extraConfig: Config = Config.empty
  ): Result = {
    execute(execution, args, extraConfig) match {
      case Success(x) => SpecsSuccess()
      case Failure(t) => {
        val stackTrace = Errors.renderWithStack(t)
        throw FailureException(SpecsFailure(
          s"Execution failed: ${t.toString}\n$stackTrace", t.getMessage, t.getStackTrace.toList
        ))
      }
    }
  }

  /**
   * Checks that the provided execution executed successfully and returns the value produced.
   *
   * The provided `Execution` is run in HDFS mode, using supplied arguments and optional additional
   * configuration. The method returns the value of the execution if it was successful, and throws a
   * `FailureException` if it faield.
   *
   * `extraConfig` should be used with caution since it can potentially override important
   * parameters of the `Execution`, such as the mode and arguments. A useful purpose for
   * `extraConfig` is to add custom serialization for the execution.
   *
   * @tparam T type of the execution (the type of its result value)
   * @param execution execution to execute
   * @param args arguments passed to `Execution.run` (placed in the Scalding `Args`
   *        variable of the `Config`)
   * @param extraConfig extra configuration passed to `Execution.run` (appended to the
   *        `Config` that would otherwise be used using the `++` method)
   * @return value produced by the execution
   */
  def executesSuccessfully[T](
    execution: Execution[T],
    args: Map[String, List[String]] = Map.empty,
    extraConfig: Config = Config.empty
  ): T = {
    execute(execution, args, extraConfig) match {
      case Success(x) => x
      case Failure(t) =>
        throw FailureException(SpecsFailure(s"Execution failed: ${t.toString}", "", t.getStackTrace.toList))
    }
  }

  /**
   * Check that an execution fails in a way matching a pattern (i.e., matching a partial function).
   *
   * The provided `Execution` is run in HDFS mode, using supplied arguments and optional additional
   * configuration. Any return value from the execution is discarded if it succeeds.
   *
   * `extraConfig` should be used with caution since it can potentially override important
   * parameters of the `Execution`, such as the mode and arguments. A useful purpose for
   * `extraConfig` is to add custom serialization for the execution.
   *
   * @tparam T type of the execution (the type of its result)
   * @param execution execution to execute
   * @param exceptionPattern pattern that the failure exception should match
   * @param args arguments passed to `Execution.run` (placed in the Scalding `Args`
   *        variable of the `Config`)
   * @param extraConfig extra configuration passed to `Execution.run` (appended to the
   *        `Config` that would otherwise be used using the `++` method)
   * @return `Result` of running the execution
   */
  def executesWithFailurePattern[T](
    executeJob: Execution[T],
    exceptionPattern: PartialFunction[Throwable, MatchResult[Any]],
    args: Map[String, List[String]] = Map.empty,
    extraConfig: Config = Config.empty
  ): Result =
    execute(executeJob, args, extraConfig) must beLike { case Failure(exception) => exceptionPattern(exception) }

  /** Runs the provided pipe and returns [[Try]] with the iterable of the result. */
  def run[T](pipe: TypedPipe[T]): Try[Iterable[T]] = execute(pipe.toIterableExecution)

  /** Runs the provided [[Grouped]] and returns [[Try]] with the iterable of the result. */
  def run[K, V](grouped: Grouped[K, V]): Try[Iterable[(K, V)]] =
    execute(grouped.toTypedPipe.toIterableExecution)

  /** Checks that the provided pipe runs successfully and returns an iterable of the content. */
  def runsSuccessfully[T](pipe: TypedPipe[T]): Iterable[T] =
    executesSuccessfully(pipe.toIterableExecution)

  /** Checks that the provided [[Grouped]] runs successfully and returns an iterable of the content. */
  def runsSuccessfully[K, V](grouped: Grouped[K, V]): Iterable[(K, V)] =
    executesSuccessfully(grouped.toTypedPipe.toIterableExecution)

  /** Verifies the provided list of Facts in the context of this test. */
  def facts(facts: Fact*): Result =
    facts.toList.map(fact => fact.run(Context(jobConf))).suml(Result.ResultMonoid)

  /** Verifies the provided list of expections in the context of this test. */
  def expectations(f: Context => Result): Result = {
    f(Context(jobConf))
  }
}

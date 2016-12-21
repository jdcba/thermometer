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

package au.com.cba.omnia.thermometer
package tools

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration.Inf
import scala.util.Try

import java.util.concurrent.{Executors, RejectedExecutionHandler, ThreadPoolExecutor}

import com.twitter.scalding.{Config, Execution, Mode}

object Executions {
  /** Run the specified execution context, returns an optional error state (None indicates success). */
  def runExecution[T](config: Config, mode: Mode, execution: Execution[T]): Try[T] = {
    val es     = Executors.newCachedThreadPool()
    // Occasionally (when the system is under load), RejectedExecutionException has been observed
    // due to an execution request submitted to the ThreadPoolExecutor after shutdown has begun.
    // The consequences of this are unclear; often the test still succeeds, on the other hand
    // it has been the last output received from some (but not all) of the coppersmith Travis builds
    // which have intermittently stalled (see CommBank/RedMetalSquad#135).
    // In a continuing attempt to diagnose this problem, replace the executor's default
    // handler (AbortPolicy) with a custom handler that ensures all requests are executed.
    es.asInstanceOf[ThreadPoolExecutor].setRejectedExecutionHandler(RejectedExecutionHandler)
    val cec    = ExecutionContext.fromExecutorService(es)
    val result = Try(Await.result(execution.run(config, mode)(cec), Inf))
    es.shutdown()
    result
  }

  // Handler that runs the task in the caller's thread, with verbose logging to stderr.
  // (Choosing to bypass the logging framework, as this is unexpected behaviour and should never be silenced).
  object RejectedExecutionHandler extends RejectedExecutionHandler {
    def rejectedExecution(r: Runnable, es: ThreadPoolExecutor) {
      System.err.println(s"Execution request rejected by ${es}")
      val t = Thread.currentThread
      System.err.println(s"Executing ${r} in caller thread instead (name=${t.getName}, id=${t.getId})")
      r.run
      System.err.println(s"Execution of ${r} complete")
    }
  }
}

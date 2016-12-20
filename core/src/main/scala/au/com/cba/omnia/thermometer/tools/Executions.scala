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
    es.asInstanceOf[ThreadPoolExecutor].setRejectedExecutionHandler(Handler)
    val cec    = ExecutionContext.fromExecutorService(es)
    val result = Try(Await.result(execution.run(config, mode)(cec), Inf))
    es.shutdown()
    result
  }

  object Handler extends RejectedExecutionHandler {
    def rejectedExecution(r: Runnable, es: ThreadPoolExecutor) {
      println(s"Execution request rejected by ${es} (isShutdown=${es.isShutdown}, isTerminating=${es.isTerminating}, isTerminated=${es.isTerminated}")
      println(s"Running ${r} in caller thread instead...")
      r.run
    }
  }
}

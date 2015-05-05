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
package core

import java.io.File

import cascading.pipe.Pipe

import com.twitter.scalding.{Job, TypedPipe, Args}

import org.apache.commons.io.FileUtils

import org.apache.log4j.LogManager

import org.apache.hadoop.fs.{FileSystem, Path}

import org.specs2._
import org.specs2.execute.{Result, Failure, FailureException}
import org.specs2.matcher.ThrownExpectations
import org.specs2.specification.core.SpecStructure

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.fact.Fact
import au.com.cba.omnia.thermometer.tools.{Errors, ExecutionSupport}
import au.com.cba.omnia.thermometer.core.Thermometer._

/** Adds functionality that makes testing scalding flows and jobs nicer.*/
abstract class ThermometerSpec extends Specification
    with ThrownExpectations
    with ScalaCheck
    with ExecutionSupport {

  /** Ensures that each of the tests is run sequentially and isolated. */
  override def map(struct: SpecStructure) =
    sequential ^ isolated ^ isolate(struct)

  def isolate[A](thunk: => A): A = {
    FileSystem.closeAll()
    thunk
  }
  
  /** Run the test with sourceEnv being on the local hadoop path of the test.*/
  def withEnvironment(sourceEnv: Path)(test: => Result): Result = {
    val (sourceDir, targetDir) = (new File(sourceEnv.toUri.getPath), new File((dir </> "user").toUri.getPath))
    FileUtils.forceMkdir(targetDir)
    FileUtils.copyDirectory(sourceDir, targetDir)
    test
  }
}

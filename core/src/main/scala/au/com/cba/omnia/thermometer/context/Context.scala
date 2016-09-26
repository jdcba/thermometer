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

package au.com.cba.omnia.thermometer.context

import scala.util.control.NonFatal
import scala.collection.JavaConverters

import java.io.{InputStreamReader, BufferedReader}

import org.specs2.execute._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.tools._

/**
 * A `Context` is a HDFS wrapper that sacrifices all safety, composability
 * and sanity in order to give the illusion that you may be able to test a
 * scalding job. Methods on thermometer return raw values that are appropriate
 * to use with specs2 matchers.
 */
case class Context(config: Configuration) {
  def withConfig[A](f: Configuration => A)(message: String): A =
    try f(config)
    catch {
      case NonFatal(t) =>
        val detail =
          s"""This test is failing because an error has occurred trying to perform an HDFS operation.
            |
            |This usually (but not always) indicates the scalding code has run to completion, but
            |there is an issue within the assertions being made.
            |
            |${Errors.renderWithStack(t)}"""
        throw new FailureException(Failure(message, detail, t.getStackTrace.toList))
    }

  def withFileSystem[A](f: FileSystem => A)(message: String): A =
    withConfig(config => f(FileSystem.get(config)))(message)

  def withFiles[A](paths: List[Path], f: (FileSystem, Path) => A)(message: String): List[A] =
    withFileSystem(system =>
      paths.flatMap(pattern =>
        glob(pattern).map(file =>
          f(system, file))))(message)

  def exists(paths: Path*): Boolean =
    withFileSystem(system =>
      paths.forall(system.exists)
    )(s"""exists(${paths.mkString(", ")})""")

  def stat(path: String): FileStatus =
    stat(path.toPath)

  def stat(path: Path): FileStatus =
    withFileSystem(system =>
      system.getFileStatus(path)
    )(s"""stat(${path})""")

  def stats(paths: Path*): List[FileStatus] =
    paths.toList.map(stat)

  def lines(paths: Path*): List[String] =
    withFiles(paths.toList, (system, file) => {
      val in = system.open(file)
      try Streams.read(in).lines.toList
      finally in.close
    })(s"""lines(${paths.mkString(", ")})""").flatten

  def withLines[A](paths: Path*)(z: A)(f: (A, String) => A): A =
    withFileSystem(system => {
      var state = z
      paths.flatMap(pattern =>
        glob(pattern).map(file => {
          val in = system.open(file)
          try {
            val reader = new BufferedReader(new InputStreamReader(in))
            var line = ""
            while ({line = reader.readLine; line != null})
              state = f(state, line)
          } finally in.close}))
       state
    })(s"""withLines(${paths.mkString(", ")})""")

  def forLines(paths: Path*)(f: String => Unit): Unit =
    withLines[Unit](paths:_*)(())((_, line) => f(line))

  def content(paths: Path*): String =
    withFiles(paths.toList, (system, file) => {
      val in = system.open(file)
      try Streams.read(in)
      finally in.close
    })(s"""content(${paths.mkString(", ")})""").mkString("\n")

  def data(paths: Path*): List[Array[Byte]] =
    withFiles(paths.toList, (system, file) => {
      val in = system.open(file)
      try Streams.bytes(in)
      finally in.close
    })(s"""data(${paths.mkString(", ")})""")

  def glob(pattern: Path): List[Path] =
    withFileSystem(system =>
      system.globStatus(pattern).toList.map(_.getPath)
    )(s"""glob(${pattern})""")
}

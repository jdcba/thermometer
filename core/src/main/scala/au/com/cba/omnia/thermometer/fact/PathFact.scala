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

package au.com.cba.omnia.thermometer.fact

import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}

import org.specs2.execute.Result
import org.specs2.matcher.MustThrownExpectations
import org.specs2.matcher.MustMatchers._

import scalaz._, Scalaz._

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerRecordReader

case class PathFact(path: Path) {
  def apply(factoid: PathFactoid*): Fact =
    Fact(thermometer =>
      factoid.toList.map(f => f.run(thermometer, path)).suml(Result.ResultMonoid))
}

case class PathFactoid(run: (Context, Path) => Result)

object PathFactoids extends MustThrownExpectations {
  def conditional(cond: (Context, Path) => Boolean)(message: Path => String): PathFactoid =
    PathFactoid((context, path) => if (cond(context, path)) ok.toResult else failure(message(path)))

  def exists: PathFactoid =
    conditional(_.exists(_))(path => s"Path <${path}> does not exist when it should.")

  def missing: PathFactoid =
    conditional(!_.exists(_))(path => s"Path <${path}> exists when it should not.")

  def count(n: Int): PathFactoid =
    PathFactoid((context, path) => {
      val count = context.lines(path).size
      if (count == n) ok.toResult else failure(s"Path <${path}> exists but it contains ${count} records where we expected ${n}.")
    })

  def records[A](reader: ThermometerRecordReader[A], data: List[A]): PathFactoid =
    records(reader, data, identity[A] _)

  def records[A, B](reader: ThermometerRecordReader[A], data: List[A], adapter: A => B): PathFactoid =
    PathFactoid((context, path) => {
      val paths = context.glob(path)
      if (paths.isEmpty)
        failure(s"No files found under <${path}>.")
      else {
        val records = paths.flatMap(p =>
          reader.read(context.config, p).unsafePerformIO)
        checkEquality(
          records,
          data,
          adapter,
          s"""Path <${path}> exists but it contains records that don't match. Expected [${data.mkString(", ")}], got [${records.mkString(", ")}]."""
        )
      }
    })

  def checkEquality[A, B](actual: List[A], expected: List[A], adapter: A => B, koMessage: String): Result
    = actual.map(adapter).toSet.must(beTypedEqualTo(expected.map(adapter).toSet)).updateMessage( _ => koMessage)


  /**
   * Create a factoid for comparing records loaded from thermometer record readers for the fact path and an expected path.
   *
   * This allows you to use different file formats for the actual output of the job and the 
   * expected output of the job. Eg: Parquet files for the job output, but a readable CSV for the reference data.
   *
   * @param actualReader reads records from the fact's path, the paths should be the actual output of the job.
   *        If fact path's are relative it is to the job's working directory.
   * @param expectedReader reads records from the expectedPath.
   * @param expectedPath path that points to the reference data. If path is relative it is to the job's working
   *        directory.
   */
  def records[A](actualReader: ThermometerRecordReader[A], expectedReader: ThermometerRecordReader[A], expectedPath: Path): PathFactoid =
    records(actualReader, expectedReader, expectedPath, identity[A] _)

  /**
   * Create a factoid for comparing records loaded from thermometer record readers for the fact path and an expected path.
   *
   * This allows you to use different file formats for the actual output of the job and the 
   * expected output of the job. Eg: Parquet files for the job output, but a readable CSV for the reference data.
   *
   * @param actualReader reads records from the fact's path, the paths should be the actual output of the job.
   *        If fact path's are relative it is to the job's working directory.
   * @param expectedReader reads records from the expectedPath.
   * @param expectedPath path that points to the reference data. If path is relative it is to the job's working
   *        directory.
   * @param adapter function adapts each of record from the expected as well as actual record set before the records
   *        are compared. If not specified, the identity function is used.
   */
  def records[A, B](actualReader: ThermometerRecordReader[A], expectedReader: ThermometerRecordReader[A], expectedPath: Path, adapter: A => B): PathFactoid = {
    PathFactoid((context, actualPath) => {
      def get(reader: ThermometerRecordReader[A], paths: List[Path]) = {
        paths.flatMap(p => reader.read(context.config, p).unsafePerformIO)
      }

      val expectedPaths = context.glob(expectedPath)
      val actualPaths = context.glob(actualPath)
      if (actualPaths.isEmpty)
        failure(s"Path <${actualPath}> does not exist.")
      else if (expectedPaths.isEmpty)
        failure(s"Path <${expectedPath}> for reference data does not exist.")
      else {
        val actual = get(actualReader, actualPaths)
        val expected = get(expectedReader, expectedPaths)
        checkEquality(
          actual,
          expected,
          adapter,
          s"""Path <${actualPath}> exists but it contains records that don't match. Expected [${expected.mkString(", ")}], got [${actual.mkString(", ")}]. Expected Path <${expectedPath}>"""
        )
      }
    })
  }

  /**
   * Create a factoid for comparing records loaded from thermometer record readers for the tree of directories beneath
   * the fact path and the tree of directories of the expected path. The directory structure for each root must be the
   * same and the records loaded from each subdirectory must be the same.
   *
   * @param actualReader reads records from the fact's path, the path should be a root directory of actual output of
   *        the job. If fact path's are relative it is to the job's working directory.
   * @param expectedReader reads records from the expectedPath.
   * @param expectedPath path that points to the root directory of the reference data. If path is relative it is to the
   *        job's working directory.
   */
  def recordsByDirectory[A](actualReader: ThermometerRecordReader[A], expectedReader: ThermometerRecordReader[A], expectedPath: Path): PathFactoid =
    recordsByDirectory(actualReader, expectedReader, expectedPath, identity[A] _)

  /**
   * Create a factoid for comparing records loaded from thermometer record readers for the tree of directories beneath
   * the fact path and the tree of directories of the expected path. The directory structure for each root must be the
   * same and the records loaded from each subdirectory must be the same.
   *
   * @param actualReader reads records from the fact's path, the path should be a root directory of actual output of
   *        the job. If fact path's are relative it is to the job's working directory.
   * @param expectedReader reads records from the expectedPath.
   * @param expectedPath path that points to the root directory of the reference data. If path is relative it is to the
   *        job's working directory.
   * @param adapter function adapts each of record from the expected as well as actual record set before the records
   *        are compared. If not specified, the identity function is used.
   * @param fileGlob pattern which helps to specify the files to be considered while comparison. The pattern should be
   *        the one supported for globStatus in org.apache.hadoop.fs.FileSystem. By default the files starting with `_`
   *        will be ignored.
   */
  def recordsByDirectory[A, B](actualReader: ThermometerRecordReader[A], expectedReader: ThermometerRecordReader[A], expectedPath: Path, adapter: A => B, fileGlob: String = "[^_]*"): PathFactoid = {
    PathFactoid((context, actualPath) => {
      val system: FileSystem = FileSystem.get(context.config)

      case class RemoteIter[FileStatus](iter: RemoteIterator[FileStatus]) extends Iterator[FileStatus] {
        def hasNext = iter.hasNext
        def next = iter.next()
      }
      
      def getRelativeSubdirs(p: Path) = {
        val absoluteRoot = system.resolvePath(p).toString()
        val pattern = s"${absoluteRoot}/(.*)".r

        RemoteIter(system.listFiles(p, true))
          .filterNot(_.isDirectory)
          .map(_.getPath.getParent().toString)
          .map(s => s match {
            case pattern(subdir) => subdir
          })
          .filter(_ != "")
          .map(path(_))
          /* Filtering out directories containing only hidden files */
          .filterNot(subdir => context.glob(p </> subdir </> fileGlob).isEmpty)
          .toSet
      }
      val actualSubdirs = getRelativeSubdirs(actualPath)
      val expectedSubdirs = getRelativeSubdirs(expectedPath)
      
      if (actualSubdirs != expectedSubdirs)
        failure(s"""Actual output Paths <${actualSubdirs}> do not match expected output Paths ${expectedSubdirs}.""")
      else if (actualSubdirs.size == 0) {
        failure(s"""No subdirectories found beneath Path <${actualPath}>.""")
      } else {
        actualSubdirs.map(subdir => {
          /* Filtering out the files which starts with _ similar to Hadoop framework */
          records(actualReader, expectedReader, expectedPath </> subdir </> fileGlob, adapter).run(context, actualPath </> subdir </> fileGlob)
        }).reduce((a, b) => if (a.isFailure) a else b)
      }
    })
  }

  def recordCount[A](reader: ThermometerRecordReader[A], n: Int): PathFactoid =
    PathFactoid((context, path) => {
      val paths = context.glob(path)
      if (paths.isEmpty)
        failure(s"No files found under <${path}>.")
      else {
        val records = paths.flatMap(p =>
          reader.read(context.config, p).unsafePerformIO)
        if (records.size == n)
          ok.toResult
        else
          failure(s"""Path <${path}> exists but it contains ${records.size} records and we expected ${n}.""")
      }
    })

  def lines(expected: List[String]): PathFactoid =
    PathFactoid((context, path) => {
      val paths = context.glob(path)
      if (paths.isEmpty)
        failure(s"No files found under <${path}>.")
      else {
        val actual = context.lines(paths: _*)
        if (actual.toSet == expected.toSet)
          ok.toResult
        else
          failure(s"""Path <${path}> exists but it contains records that don't match. Expected [${expected.mkString(", ")}], got [${actual.mkString(", ")}].""")
      }
    })

}

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

import au.com.cba.omnia.thermometer.fact._
import org.apache.hadoop.fs.Path

object Thermometer {
  def path(s: String): Path =
    new Path(s)

  def paths(ss: List[String]): List[Path] =
    ss.map(path)

  implicit class ThermometerStringSyntax(s: String) {
    def toPath: Path = path(s)
    def </>(path: String): Path = new Path(s, path)
    def </>(path: Path): Path = new Path(s, path)
    def ==> : PathFact = PathFact(toPath)
  }

  implicit class ThermometerPathSyntax(p: Path) {
    def </>(path: String): Path = new Path(p, path)
    def </>(path: Path): Path = new Path(p, path)
    def ==> : PathFact = PathFact(p)
  }
}

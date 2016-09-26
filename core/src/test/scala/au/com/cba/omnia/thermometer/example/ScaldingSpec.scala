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

package au.com.cba.omnia.thermometer.example

import com.twitter.scalding.typed.IterablePipe

import org.apache.hadoop.mapred.JobConf

import au.com.cba.omnia.thermometer.core.ThermometerSpec

object ScaldingSpec extends ThermometerSpec { def is = s2"""

Demonstration of ThermometerSpec on Scalding pipe
=================================================

  Verify typed pipe $pipe
  Verify grouped    $grouped

"""

  val data = List(
    Car("Canyonero", 1999),
    Car("Batmobile", 1966)
  )

  def f(c: Car) = (c.model, c.year, c.purchaseDate)

  val pipeline =
    IterablePipe[Car](data)
      .map(f)

  def pipe =
    runsSuccessfully(pipeline) must_== data.map(f)

  def grouped =
    runsSuccessfully(pipeline.groupBy(_._1)) must_== data.map(c => (c.model, f(c)))
}

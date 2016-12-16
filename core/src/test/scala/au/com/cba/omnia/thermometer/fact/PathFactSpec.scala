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

import au.com.cba.omnia.thermometer.core.ThermometerSpec

object PathFactoidsSpec extends ThermometerSpec { def is = s2"""
PathFactoids
============

  `checkEquality` correctly handles test failures where multiple expectations are checked
    This actually tests that the test fails so we use pending until fixed
    to turn it into a pending. If the test passes it will actually be turned
    into a failure.                                                            ${thrownExpectations.pendingUntilFixed}

"""

  def thrownExpectations = {
    PathFactoids.checkEquality(List(1, 2), List(3, 4), identity[Int], "error message")

    2 must_== 2
  }
}

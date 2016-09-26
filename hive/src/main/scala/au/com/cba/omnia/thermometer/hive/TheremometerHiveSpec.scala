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

package au.com.cba.omnia.thermometer.hive

import au.com.cba.omnia.thermometer.core.ThermometerSpec

/**
  * Adds functionality that makes testing Scalding and Hive nicer.
  *
  * This combines [[ThermometerSpec]] and [[HiveSupport]] and resolves the conflicting inheritance
  * of `before`.
  */
abstract class ThermometerHiveSpec extends ThermometerSpec with HiveSupport {
  /** Combines the `before` from [[ThermometerSpec]] and [[HiveSupport]]. */
  override def before = {
    super[ThermometerSpec].before
    super[HiveSupport].before
  }
}

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

import org.apache.hadoop.hive.metastore.{HiveMetaHookLoader, HiveMetaStoreClient, RetryingMetaStoreClient}
import org.apache.hadoop.hive.metastore.api.Table

object HiveSpec extends ThermometerHiveSpec { def is = s2"""

Example hive test
=================

  Can run hive test $test

"""

  def test = {
    val client = RetryingMetaStoreClient.getProxy(
      hiveConf,
      new HiveMetaHookLoader() {
        override def getHook(tbl: Table) = null
      },
      classOf[HiveMetaStoreClient].getName()
    )

    client.getAllDatabases.size must_== 1

  }
}

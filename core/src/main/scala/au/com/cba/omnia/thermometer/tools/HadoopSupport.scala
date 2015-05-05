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

package au.com.cba.omnia.thermometer.tools

import scalaz._, Scalaz._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf

/** Adds testing support for Hadoop by creating a `JobConf` with a temporary path.*/
trait HadoopSupport {
  lazy val name: String =
    s"test-${java.util.UUID.randomUUID}"

  lazy val dir: String =
    s"/tmp/hadoop/${name}/mapred"

  lazy val jobConf: JobConf = new JobConf <| (conf => {
    new java.io.File(dir, "data").mkdirs()
    conf.set("user.dir", s"${dir}/user")
    conf.set("jobclient.completion.poll.interval", "10")
    conf.set("cascading.flow.job.pollinginterval", "2")
    conf.set("mapred.local.dir", s"${dir}/local")
    conf.set("mapred.system.dir", s"${dir}/system")
    conf.set("hadoop.log.dir", s"${dir}/log")
    conf.set("fs.defaultFS", s"file://${dir}/data")
  })
}

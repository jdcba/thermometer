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

import sbt._, Keys._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._

object build extends Build {
  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    uniformDependencySettings ++
    strictDependencySettings ++
    uniform.docSettings("https://github.com/CommBank/thermometer") ++ Seq(
      parallelExecution in Test := false,
      updateOptions := updateOptions.value.withCachedResolution(true),
      fork in Test := true
    )

  lazy val all = Project(
    id = "all",
    base = file("."),
    settings =
      standardSettings
        ++ uniform.project("thermometer-all", "au.com.cba.omnia.thermometer.all")
        ++ uniform.ghsettings
        ++ Seq(
          publishArtifact := false
        ),
    aggregate = Seq(core, hive)
  )

  lazy val core = Project(
    id = "core",
    base = file("core"),
    settings =
      standardSettings
        ++ uniform.project("thermometer", "au.com.cba.omnia.thermometer")
        ++ Seq(
             libraryDependencies ++=
                  depend.hadoopClasspath
               ++ depend.hadoop()
               ++ depend.scalding()
               ++ depend.testing(configuration = "compile")
               ++ depend.time().map(_ % "test")
           )
  )

  lazy val hive = Project(
    id = "hive",
    base = file("hive"),
    settings =
      standardSettings
        ++ uniform.project("thermometer-hive", "au.com.cba.omnia.thermometer.hive")
        ++ Seq(
          libraryDependencies ++= depend.hadoopClasspath ++ depend.hadoop() ++ depend.hive()
        )
  ).dependsOn(core)
}

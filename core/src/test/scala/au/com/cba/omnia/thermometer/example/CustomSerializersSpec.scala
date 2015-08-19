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

import scala.collection.immutable._

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat}

import com.esotericsoftware.kryo.Serializer

import com.twitter.algebird.{Aggregator, Monoid}

import com.twitter.chill._
import com.twitter.chill.config.{Config => ChillConfig,
                                 ConfiguredInstantiator => ChillConfiguredInstantiator}

import com.twitter.scalding.{Config, TypedPsv}
import com.twitter.scalding.typed.IterablePipe
import com.twitter.scalding.serialization.KryoHadoop

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerSpec

/**
 * Demonstrates custom serializers attached to a Thermometer specification.
 * 
 * Ordinarily, JodaTime `DateTime` objects cannot be serialized by Scalding. It is possible to
 * serialize them with a custom Kryo serializer. This example illustrates how this can be
 * achieved, by passing a custom serializer as part of the job configuration.
 */
class CustomSerializersSpec extends ThermometerSpec { def is = s2"""

Demonstration of an execution-based ThermometerSpec that relies on a custom Kryo serializer
===========================================================================================

  Verify that a custom serializer can participate in a test $usingCustomSerializer

"""

  import CustomSerializerSpec._

  def usingCustomSerializer = {

    // pass the register of extra serialziers to the execution job
    val extraConfig =
      Config
        .empty
        .setSerialization(Right(classOf[CustomSerializerRegistration]))

    // test pipe of DateTime objects, which can't be serialized properly without a custom
    // serializer
    val times: IterablePipe[DateTime] = IterablePipe(List(
      yyyyMMdd.parseDateTime("20150101"),
      yyyyMMdd.parseDateTime("20150602")
    ))

    // the execution just converts the DateTime objects back to Strings
    val exec =
      times
        .map(yyyyMMdd.print)
        .writeExecution(TypedPsv("customers"))

    executesOk(exec, extraConfig = extraConfig)
    expectations(
      _.lines("customers" </> "part-*") must contain(allOf("20150101", "20150602"))
    )
  }

}

object CustomSerializerSpec {

  val yyyyMMdd = DateTimeFormat.forPattern("yyyyMMdd")

  // Custom serializer for Joda DateTime objects
  class DateTimeSerializer extends Serializer[DateTime] {
    def write(kryo: Kryo, o: Output, d: DateTime): Unit = {
      o.writeLong(d.getMillis, true)
      () // intentionally discard non-Unit value
    }
    def read(kryo: Kryo, i: Input, `type`: Class[DateTime]): DateTime = {
      val millis = i.readLong(true)
      new DateTime(millis, DateTimeZone.forID("Australia/Sydney"))
    }
  }

  // Register the custom serializer
  class CustomSerializerRegistration(config: ChillConfig) extends KryoHadoop(config) {
    override def newKryo() = {
      val kryo = super.newKryo()
      kryo.register(classOf[DateTime], new DateTimeSerializer)
      kryo
    }
  }

}

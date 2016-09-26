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
package tools

import java.io.{InputStream, OutputStream, ByteArrayOutputStream, PrintStream}

object Streams {
  val DefaultBufferSize = 4096

  def read(in: InputStream, encoding: String = "UTF-8", bufferSize: Int = DefaultBufferSize) = {
    val data = bytes(in, bufferSize)
    new String(data, encoding)
  }

  def bytes(in: InputStream, bufferSize: Int = DefaultBufferSize) = {
    val buffer = Array.ofDim[Byte](bufferSize)
    val out = new ByteArrayOutputStream
    var size = 0
    while ({ size = in.read(buffer); size != -1 })
      out.write(buffer, 0, size)
    out.toByteArray
  }

  def write(out: OutputStream, content: String, encoding: String = "UTF-8") = {
    val writer = new PrintStream(out, false, encoding);
    try writer.print(content)
    finally writer.close
  }
}

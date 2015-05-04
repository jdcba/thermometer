thermometer
===========

[![Build Status](https://travis-ci.org/CommBank/thermometer.svg?branch=master)](https://travis-ci.org/CommBank/thermometer)
[![Gitter chat](https://badges.gitter.im/CommBank.png)](https://gitter.im/CommBank)

```
A micro-test framework for scalding pipes to make sure you don't get burnt
```

The `thermometer` library has a few goals:
 - Be explicit in expected outcomes, whilst not being concerned with irrelevant details.
 - Provide exceptional feedback in the face of failure:
   - Good error messages.
   - Clear mapping into actual files on disk where appropriate.
 - To allow testing of end-to-end pipelines which is impossible with `JobTest`.
 - To just work (no race-conditions, data to clean-up etc...), and work fast.


Thermometer tests can be declared in two ways, as `facts` or as traditional specs2
checks. The `facts` api should be preferred, it generally provides better contextual
error messages and composition for lower effort.

[Scaladoc](https://commbank.github.io/thermometer/latest/api/index.html)

Usage
-----

See https://commbank.github.io/thermometer/index.html.

getting started
---------------

Import everything:

```scala
import com.twitter.scalding._
import com.twitter.scalding.typed.IterablePipe

import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._
```

Then create a spec that extends `ThermometerSpec`. This sets up appropriate scalding,
cascading and hadoop related things as well as ensuring that specs2 is run in a
way that won't break hadoop.


thermometer facts
-----------------

Facts can be asserted on cascading `Pipe` objects or scalding `TypedPipe` objects.

To verify some pipeline, you add a facts call. For example:

```scala
def test {
  val exec =
    IterablePipe(List("hello", "world"))
      .map(c => (c, "really" + c + "!"))
      .writeExecution(TypedPsv[(String, String)]("output"))

  executesOk(exec)
  facts(
    "output" </> "_ERROR"      ==> missing
  , "output" </> "_SUCCESS"    ==> exists
  , "output" </> "part-00000"  ==> (exists, count(2))  /* 2 items */
  )
}
```

Breaking this down, `facts` takes a sequence of `Fact`s, these
can be construted in a number of ways, the most supported form are `PathFact`s,
which are built using the `==>` operation added to hdfs `Path`s and `String`s.
The right hand side of `==>` specifies a sequences of facts that should hold
true given the specified path.


thermometer expectations
------------------------

Thermometer expectations allow you to fall back to specs2, this may be because
of missing functionality from the facts api, or for optimisation of special
cases.

To verify some pipeline, you add a expectations call. For example:

```scala
def test = {
  val exec =
    IterablePipe(List("hello", "world"))
      .map(c => (c, "really" + c + "!"))
      .writeExecution(TypedPsv[(String, String)]("output"))

  executesOk(exec)
  expectations { context =>
    context.exists("output" </> "_SUCCESS") must beTrue
    context.lines("output" </> "part-*").toSet must_== Set(
      "hello" -> "really hello!",
       "world" -> "really world!"
    )
  }
}
```

Breaking this down, `expectations` takes a function `Context => Unit`.
`Context` is a primitive (unsafe) API over hdfs operations that will allow you
to make assertions. The `Context` handles unexpected failures by failing the
test with a nice error message, but there is no way to do manual error handling
at this point.

using thermometer from scalacheck properties
--------------------------------------------

The hackery that thermometer uses to handle the _mutable_, _global_, _implicit_ state that
HDFS uses (yes shake your head now) needs to be reset for each run. To do this use an
`isolate {} block inside the property`. (https://github.com/CommBank/thermometer/issues/41)

For example:

```scala
def test = prop((data: List[String]) => isolate {
    val exec =
      IterablePipe(data)
        .map(c => (c, "really " + c + "!"))
        .writeExecution(TypedPsv[(String, String)]("output"))

    executesOk(exec)
    facts("output" </> "_SUCCESS"    ==> exists)
})
```

dependent pipelines
-------------------

Use `flatMap` or `zip` from the Execution API to create dependencies.

hive
----

Mix in the `HiveSupport` trait in `import au.com.cba.omnia.thermometer.hive.HiveSupport` to add
support for hive and, in particular, set up a separate warehouse directory and metadata database per
test and provide the right `HiveConf`.

log verbosity
-------------

The verbosity of the logging messages from Thermometer and Hadoop can be configured by adding a
`log4j.properties` file. Please find an example in
[core/src/main/resources/log4j.properties](core/src/main/resources/log4j.properties).

ongoing work
------------

 - Built-in support for more facts
   - Support for streaming comparisons
   - Support for statistical comparisons
  - Support for re-running tests with different scalding modes
 - Add the ability for Context to not depend on hdfs, via some
   sort of in memory representation for assertions.

# Table of contents
- [Introduction](#introduction)
- [Usage](#usage)
- [Operators](#operators)
  - [DistinctKey](#distinctKey)
  - [FilterConsecutives](#filterConsecutives)
  - [CaptureMaterializedValues](#captureMaterializedValues)
  - [WeightedWindow](#weightedWindow)
  - [WeightedSlidingWindow](#weightedSlidingWindow)
  - [TimedWindow](#timedWindow)
  - [TimedSlidingWindow](#timedSlidingWindow)
  - [Window](#window)
  - [SlidingWindow](#window)
  - [Reorder](#reorder)
  - [Flattening operators](#flattening-operators)
    - [Concatenate](#concatenate)
    - [MapConcatenate](#mapConcatenate)
    - [Switch](#switch)
    - [MapSwitch](#mapSwitch)
    - [Join](#join)
    - [MapJoin](#mapJoin)
    - [JoinFairly](#joinFairly)
    - [MapJoinFairly](#mapJoinFairly)
    - [JoinWithPriorities](#joinWithPriorities)
    - [MapJoinWithPriorities](#mapJoinWithPriorities)
  - [Diverging operators](#diverging-operators)
    - [Anycast](#anycast)
    - [AnycastWithPriorities](#anycastWithPriorities)
    - [Broadcast](#broadcast)
    - [Balance](#balance)
    - [Partition](#partition)

# Introduction
This library provides a set of operators for akka-stream. Some of them come as an improvement of the standard operators packed with akka-stream whereas others implement new functionalities.

# Usage
The library is available on [maven central](https://mvnrepository.com/artifact/be.broij/akka-stream-utils). To use it in a project, a dependency to the chosen version of akka-stream must be setup. Note that each release was tested on one particular version of akka-stream, which is given in the table here below.

Release | Tested version
--------|--------------------
0.0.0|2.6.10

To use the operators, one can import the provided implicit conversions and use them on the sources/flows of his choice.
```
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import be.broij.akka.stream.SourceExtensions._
import be.broij.akka.stream.FlowExtensions._

implicit val system = ActorSystem()

Source(List(Source(List("h", "el", "lo")), Source.empty, Source.single("w"), Source(List("orl", "d"))))
  .concatenate
  .runForeach(System.out.print)
```

Some operators, namely [window](#window) and [slidingWindow](#slidingWindow), don't have any implicit conversions available and can only be instanciated via their factory methods. This is by design: these operators were conceived as basic blocks that can be used to ease the process of creating new types of operators. The other operators for which implicit conversions are available can also be instanciated that way.
```
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import be.broij.akka.stream.operators.flatten.Concatenate

implicit val system = ActorSystem()

Source(List(Source(List("h", "el", "lo")), Source.empty, Source.single("w"), Source(List("orl", "d"))))
  .via(Concatenate.apply)
  .runForeach(System.out.print)
```

# Operators
The specification of each operator provided by the library is given in the following sections.

## DistinctKey
`DistinctKey[T, K](keyOf: T => K): Flow[T, T, NotUsed]`

Creates a flow filtering out the elements whose key is identical to the one of their preceding element. The function _keyOf_ is used to extract the keys of the elements.

![DistinctKeyExample](/images/example-distinctKey.png)
*An example of the stream which is produced by applying the distinctKey operator to a stream of integers where the keyOf function is the identity function.*

## FilterConsecutives
`FilterConsecutives[T](shouldFilter: (Option[T], T) => Boolean): Flow[T, T, NotUsed]`

Creates a flow filtering out the elements invalidating the shouldFilter predicate. The function shouldFilter is a predicate taking as parameters an Option wrapping the latest element emitted (None if no element was emitted yet) and the current element to be tested.

![FilterConsecutivesExample](/images/example-filterConsecutives.png)
*An example of the stream which is produced by applying the filterConsecutives operator to a stream of integers. The predicate makes sure the integer are emitted in ascending order.*

## CaptureMaterializedValues
```
CaptureMaterializedValues[T, M](
  implicit materializer: Materializer
): Flow[Source[T, M], Source[T, NotUsed], Source[M, NotUsed]]
```

Creates a flow working with streams of streams. Let us refer to the streams embedded in a stream as substreams. The flow erases the materialized value of each substream. Its materialized value is another stream where the n<sup>th</sup> element gives the materialized value of the n<sup>th</sup> substream. The _materializer_ is used to pre-materialize each substream in order to access their own materialized value.

## WeightedWindow
`WeightedWindow[T, W: Numeric](maxWeight: W, weightOf: T => W): Flow[T, Seq[T], NotUsed]`

Creates a flow working on streams where each element has an associated weight obtained with the function _weightOf_. Let us call w<sub>n</sub> the weight associated to the n<sup>th</sup> element of such a stream. The flow turns such a stream of elements into a stream of windows. Each window is the longest sequence of consecutive elements, kept in emission order, that starts with a given element and whose cumulative weight doesn't exceed _maxWeight_. The first window starts with the first element of the stream. Let l<sub>n</sub> be the index of the last element of the n<sup>th</sup> window. The index of the first element of the n<sup>th</sup> window is f<sub>n</sub> = l<sub>n-1</sub> + 1.

## WeightedSlidingWindow
`WeightedSlidingWindow[T, W: Numeric](maxWeight: W, weightOf: T => W): Flow[T, Seq[T], NotUsed]`

Creates a flow working on streams where each element has an associated weight obtained with the function _weightOf_. Let us call w<sub>n</sub> the weight associated to the n<sup>th</sup> element of such a stream. The flow turns such a stream of elements into a stream of windows. Each window is the longest sequence of consecutive elements, kept in emission order, that starts with a given element and whose cumulative weight doesn't exceed _maxWeight_. The first window starts with the first element of the stream. Let l<sub>n</sub> be the index of the last element of the n<sup>th</sup> window. The n<sup>th</sup> + 1 window is fit to include the l<sub>n</sub> + 1 <sup>th</sup> element while repeating as many elements from the n<sup>th</sup> window as possible.

## TimedWindow
`TimedWindow[T](maxPeriod: FiniteDuration, timeOf: T => ZonedDateTime): Flow[T, Seq[T], NotUsed]`

Creates a flow working on streams where each element has an associated timestamp obtained with the function _timeOf_. Let us call t<sub>n</sub> the timestamp associated to the n<sup>th</sup> element of such a stream. The flow assumes the elements are emitted in the order dictated by their timestamps: ![prec](https://render.githubusercontent.com/render/math?math=%5Cforall%20n%20%3C%20m%20%3A%20t_n%20%5Cleq%20t_m). It turns such a stream of elements into a stream of windows. Each window is a sequence of timestamp-ordered elements giving the set of elements whose timestamps are included in a given time interval. Let l<sub>n</sub> be the index of the last element of the n<sup>th</sup> window. The index of the first element of the n<sup>th</sup> window is f<sub>n</sub> = l<sub>n-1</sub> + 1. The first window starts with the first element of the stream: f<sub>0</sub> = 0. The n<sup>th</sup> window contains the elements that occurred in the [t<sub>f<sub>n</sub></sub>, t<sub>f<sub>n</sub></sub> + maxPeriod] time interval. The _maxPeriod_ parameter defines the duration of the time intervals of each window.

## TimedSlidingWindow
`TimedSlidingWindow[T](maxPeriod: FiniteDuration, timeOf: T => ZonedDateTime): Flow[T, Seq[T], NotUsed]`

Creates a flow working on streams where each element has an associated timestamp obtained with the function _timeOf_. Let us call t<sub>n</sub> the timestamp associated to the n<sup>th</sup> element of such a stream. The flow assumes the elements are emitted in the order dictated by their timestamps: ![prec](https://render.githubusercontent.com/render/math?math=%5Cforall%20n%20%3C%20m%20%3A%20t_n%20%5Cleq%20t_m). It turns such a stream of elements into a stream of windows. Each window is a sequence of timestamp-ordered elements giving the set of elements whose timestamps are included in a given time interval. The first window starts with the first element of the stream. Let f<sub>n</sub> be the index of the first element of the n<sup>th</sup> window. Such a window contains the elements that occurred in the [t<sub>f<sub>n</sub></sub>, t<sub>f<sub>n</sub></sub> + maxPeriod] time interval. The _maxPeriod_ parameter defines the duration of the time intervals of each window. Let l<sub>n</sub> be the index of the last element of the n<sup>th</sup> window. The n<sup>th</sup> + 1 window is fit to include the l<sub>n</sub> + 1 <sup>th</sup> element while repeating as many elements from the n<sup>th</sup> window as possible.

## Window
`Window[T, F <: Frame[T]](implicit frameFactory: FrameFactory[T, F]): Flow[T, Seq[T], NotUsed]`

Creates a flow turning streams of elements into streams of windows. Each window is a sequence of elements. The flow uses an implementation of the trait `Frame[T]` that has to be provided by the user. An instance of the trait `FrameFactory[T, F <: Frame[T]]` must also be provided by the user. A frame represents a window being assembled. It defines several methods:
- `canAdd(item: T): Boolean` False if the item can’t be added to the frame, true otherwise;
- `add(item: T): Frame[T]` Creates a new frame containing all the items in the frame plus the given item;
- `nonEmpty: Boolean` True if the frame is not empty, false otherwise;
- `payloadSeq: Seq[T]` Gives the content of the frame as a sequence of elements.

The frame factory also defines several methods:
- `apply(): F` Creates an empty frame;
- `apply(item: T): F` Creates a frame containing the given item.

To build the windows, the flow consumes the elements one after the others. It starts with an empty frame. It tries to add each element it consumes to that frame. If an element can’t be added to the frame, the window it represents is emitted, and a new frame containing that element is created to pursue the process of windowing the stream.

## SlidingWindow
`SlidingWindow[T, F <: Frame[T]](implicit frameFactory: FrameFactory[T, F]): Flow[T, Seq[T], NotUsed]`

Creates a flow turning streams of elements into streams of windows. Each window is a sequence of elements. The flow uses an implementation of the trait `Frame[T]` that has to be provided by the user. An instance of the trait `FrameFactory[T, F <: Frame[T]]` must also be provided by the user. A frame represents a window being assembled. It defines several methods:
- `canAdd(item: T): Boolean` False if the item can’t be added to the frame, true otherwise;
- `add(item: T): Frame[T]` Creates a new frame containing all the items in the frame plus the given item;
- `shrink(item: T): Frame[T]` Creates a new frame containing as many items as possible from the frame plus the given item.
- `nonEmpty: Boolean` True if the frame is not empty, false otherwise;
- `payloadSeq: Seq[T]` Gives the content of the frame as a sequence of elements.

The frame factory also defines several methods:
- `apply(): F` Creates an empty frame;

To build the windows, the flow consumes the elements one after the others. It starts with an empty frame. It tries to add each element it consumes to that frame. If an element can’t be added to the frame, the window it represents is emitted, and the frame is then fit to contain that element to pursue the process of windowing the stream.

## Reorder
```
Reorder[T: Ordering, F <: Frame[T]](implicit frameFactory: FrameFactory[T, F]): Flow[T, T, NotUsed]
Reorder[T: Ordering, W: Numeric](maxWeight: W, weightOf: T => W): Flow[T, T, NotUsed]
Reorder[T: Ordering](maxPeriod: FiniteDuration, timeOf: T => ZonedDateTime): Flow[T, T, NotUsed]
```

Creates a flow working on streams of elements for which an [Ordering](https://www.scala-lang.org/api/2.12.3/scala/math/Ordering.html) implementation is provided by the user. Referring to that ordering implementation, it tries to make sure the elements are emitted in ascending order. Since a stream is an infinite sequence, it cannot be fully sorted. Instead, the flow splits the stream into windows of elements, sorts each of them and concatenates their content in order. The ordering is thus guaranteed per window. To sort each window, the flow uses the default scala sorting algorithm, which uses [java.util.Arrays.sort](https://docs.oracle.com/en/java/javase/12/docs/api/java.base/java/util/Arrays.html#sort(T%5B%5D,java.util.Comparator)). One can refer respectively to the [Window](#window), [WeightedWindow](#weightedWindow) or [TimedWindow](#timedWindow) specifications to get more information about the role of each parameter of the builders whose specifications were given here above.

## Flattening operators
These operators flatten streams: they turn streams of streams of a given abstraction into streams of that given abstraction. In the following subsections, we will refer to the streams embedded in a stream as substreams.

### Concatenate
`Concatenate[T]: Flow[Source[T, NotUsed], T, NotUsed]`

Creates a flattening operator. The result is a flow taking the first substream and emitting all of its elements one by one in order. Then, when the substream completes, the flow takes the next substream and repeats that process. The flow completes when the stream completes and all of its substreams have been processed. It fails when the stream fails or the substream being processed fails.

#### MapConcatenate
`MapConcatenate[T, U](mapper: T => Source[U, NotUsed]): Flow[T, U, NotUsed]`

Creates a flow using the _mapper_ function to turn each element of a stream in a substream and then flattening all of these substreams via the concatenate operator.

### Switch
`Switch[T](implicit materializer: Materializer): Flow[Source[T, NotUsed], T, NotUsed]`

Creates a flattening operator. The result is a flow taking the first substream and emitting all of its elements one by one in order. As soon as the next substream is available, the flow stops the substream currently being processed. When the substream being processed completes, the flow takes the next substream and repeats that process. The flow completes when the stream completes and all of its substreams have been processed. It fails when the stream fails or the substream being processed fails. The _materializer_ is used to pre-materialize each substream.

#### MapSwitch
```
MapSwitch[T, U](mapper: T => Source[U, NotUsed])
               (implicit materializer: Materializer): Flow[T, U, NotUsed]
```

Creates a flow using the _mapper_ function to turn each element of a stream in a substream and then flattening all of these substreams via the switch operator. The _materializer_ is used to pre-materialize each substream.

### Join
`Join[T](breadth: Option[BigInt]): Flow[Source[T, NotUsed], T, NotUsed]`

Creates a flattening operator. The result is a flow taking the first N substreams and joining them by emitting all of their elements one by one in a FIFO fashion. When one of the substreams being joined completes, the flow takes the next substream and continues its process with that substream added to the set of substreams it joins. The flow completes when the stream completes and all of its substreams have been processed. It fails when the stream fails or one of the substreams being processed fails. The number of substreams to join at the same time is provided by the user as an optional value called _breadth_. When that optional value is set _None_, there is no limit on the maximum number of substreams to process simultaneously.

#### MapJoin
`MapJoin[T, U](mapper: T => Source[U, NotUsed], breadth: Option[BigInt]): Flow[T, U, NotUsed]`

Creates a flow using the _mapper_ function to turn each element of a stream in a substream and then flattening all of these substreams via the join operator. The number of substreams to join at the same time is provided by the user as an optional value called _breadth_. When that optional value is set to _None_, there is no limit on the maximum number of substreams to process simultaneously.

### JoinFairly
`JoinFairly[T](n: BigInt, breadth: Option[BigInt]): Flow[Source[T, NotUsed], T, NotUsed]`

Creates a flattening operator. The result is a flow taking the first M substreams and joining them by emitting the next _n_ elements from the first substream being joined followed by the next _n_ elements from the second substream being joined, and so on so forth. When one of the substreams being joined completes, the flow takes the next substream and continues its process with that substream added to the set of substreams it joins. The flow completes when the stream completes and all of its substreams have been processed. It fails when the stream fails or one of the substreams being processed fails. The number of substreams to join at the same time is provided by the user as an optional value called _breadth_. When that optional value is set _None_, there is no limit on the maximum number of substreams to process simultaneously.

#### MapJoinFairly
`MapJoinFairly[T, U](n: BigInt, mapper: T => Source[U, NotUsed], breadth: Option[BigInt]): Flow[T, U, NotUsed]`

Creates a flow using the _mapper_ function to turn each element of a stream in a substream and then flattening all of these substreams via the joinFairly operator. The number of substreams to join at the same time is provided by the user as an optional value called _breadth_. When that optional value is set to _None_, there is no limit on the maximum number of substreams to process simultaneously.

### JoinWithPriorities
```
JoinWithPriorities[T, P: Ordering](
  priorityOf: T => P, breadth: Option[BigInt]
): Flow[Source[T, NotUsed], T, NotUsed]
```

Creates a flattening operator. The result is a flow taking the first N substreams and joining them by emitting all of their elements. The function _priorityOf_ is used to attribute a priority to each element and when several are available for emission, the one with the highest priority is emitted. When one of the substreams being joined completes, the flow takes the next substream and continues its process with that substream added to the set of substreams it joins. The flow completes when the stream completes and all of its substreams have been processed. It fails when the stream fails or one of the substreams being processed fails. The number of substreams to join at the same time is provided by the user as an optional value called _breadth_. When that optional value is set _None_, there is no limit on the maximum number of substreams to process simultaneously.

#### MapJoinWithPriorities
```
MapJoinWithPriorities[T, U, P: Ordering](
  mapper: T => Source[U, NotUsed], priorityOf: U => P, breadth: Option[BigInt]
): Flow[T, U, NotUsed]
```

Creates a flow using the _mapper_ function to turn each element of a stream in a substream and then flattening all of these substreams via the joinWithPriorities operator. The number of substreams to join at the same time is provided by the user as an optional value called _breadth_. When that optional value is set to _None_, there is no limit on the maximum number of substreams to process simultaneously.

## Diverging operators
These operators enable to build streaming scenarios where a dynamic set of consumers are connected to the same producer. They provide sources whose materializations are consumers emitting the elements they receive from a shared producer they register to. The job of the producer is to emit the elements of a wrapped source to the registered consumers. It manages a dynamic group of consumers that grows or shrinks as consumers register and unregister. In each of these operators, a special flag called _restartSource_ allows specifying how the producer should react when there are no more consumers. If set to _true_, the producer will stop the wrapped source and restart it from the beginning when some new consumer register. If set to _false_, it will let the wrapped source continue to execute. In each diverging operators, if an element sent to a consumer isn't acknowledged to the producer before a _FiniteDuration_ called _baseTimeoutDelay_, which is provided by the user, the element is sent again to that consumer. This duration is increased exponentially by a power of two each time the same element is sent again to the same consumer. Note that this mechanism is completely transparent for the final user:
- nothing more than providing that _baseTimeoutDelay_ is expected to be done by the user;
- when an element is received several time by the same consumer due to retransmissions, it will appear only once in the corresponding stream.

### Anycast
```
Anycast[T](source: Source[T, NotUsed], restartSource: Boolean, baseTimeoutDelay: FiniteDuration)
          (implicit actorSystem: ActorSystem): Source[T, NotUsed]
```

Creates a diverging operator. The result is a source whose materializations are consumers registered to the same producer. The producer emits the elements of the source it wraps one after the others. Each element is sent to one of the consumers, taking the first one available or using a FIFO policy when several consumers are available. The source completes when the producer completes. It fails when the producer fails.

### AnycastWithPriorities
```
AnycastWithPriorities[T, P: Ordering](source: Source[T, NotUsed], restartSource: Boolean,
                                      baseTimeoutDelay: FiniteDuration)
                                     (implicit actorSystem: ActorSystem): AnycastWithPriorities[T, P]
```

Creates a diverging operator. The result is an object allowing to create sources whose materializations are consumers registered to the same producer. The producer emits the elements of the source it wraps one after the others. The sources can be created via the method:
```
AnycastWithPriorities.withPriority(priority: P): Source[T, NotUsed]
```

Each consumer is bound to the priority that is given when the source it materializes is created. Each element is sent to one of the consumers, taking the first one available or the one with the highest priority when several are available. The created sources complete when the producer completes. They fail when the producer or any of its consumers fail.

### Broadcast
```
Broadcast[T](source: Source[T, NotUsed], restartSource: Boolean,
             waitSlowest: Boolean, bufferSize: Int, baseTimeoutDelay: FiniteDuration)
            (implicit actorSystem: ActorSystem): Source[T, NotUsed]
```

Creates a diverging operator. The result is a source whose materializations are consumers registered to the same producer. The producer emits the elements of the source it wraps one after the others. Each element is sent to all of its consumers. A buffer allows the fastest consumers to advance without having to wait for the slowest ones to consume the current element. The size of the buffer is finite and provided by the user with the _bufferSize_ parameter. Two behaviors are available when the buffer is full: wait the slowest consumers when the user sets the flag _waitSlowest_ to _true_ or follow the pace of the fastest consumers (which means that some consumers may skip some elements) when the user sets this flag to _false_. The source completes when the producer completes. It fails when the producer fails.

### Balance
```
Balance[T](source: Source[T, NotUsed], n: BigInt, restartSource: Boolean,
           baseTimeoutDelay: FiniteDuration)
          (implicit actorSystem: ActorSystem): Source[T, NotUsed]
```

Creates a diverging operator. The result is a source whose materializations are consumers registered to the same producer. The producer emits the elements of the source it wraps one after the others. Each element is sent to one of its consumers. The _n_ first elements are sent to the first consumer, then the _n_ next elements are sent to the second consumer and so on so forth in a circular fashion. The constant _n_ is a parameter giving the amount of elements to send to an individual consumer before switching to the next. The source completes when the producer completes. It fails when the producer fails.

### Partition
```
Partition[T, P](source: Source[T, NotUsed], partitionOf: T => P, restartSource: Boolean,
                waitSlowest: Boolean, bufferSize: Int, baseTimeoutDelay: FiniteDuration)
               (implicit actorSystem: ActorSystem): P => Source[T, NotUsed]
```

Creates a diverging operator. The result is a function allowing to create sources whose materializations are consumers registered to the same producer. The producer emits the elements of the source it wraps one after the others. Each consumer is bound to a partition P that is given when the source it materializes is created. Each element is attributed to a given partition using a partitioning function, _partitionOf_. Each consumer subscribes to the partition it is bound to and receives all the elements attributed to this partition. A buffer allows the fastest consumers to advance without having to wait for the slowest ones to consume the current element. This buffer is shared by all consumers no matter the partition they are subscribed to. The size of the buffer is finite and provided by the user with the _bufferSize_ parameter. Two behaviors are available when the buffer is full: wait the slowest consumers when the user sets the flag _waitSlowest_ to _true_ or follow the pace of the fastest consumers (which means that the slowest consumers may skip some elements) when the user sets this flag to _false_. The source completes when the producer completes. It fails when the producer fails.

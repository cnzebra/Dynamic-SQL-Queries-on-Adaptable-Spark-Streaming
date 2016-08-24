# SPARK CEP using Stateful Operator

**C**omplex **E**vent **P**rocessing implementation that analyzes stream of events coming from various sources and launches alerts when specific matching pattern occurs.

### Problem
Each rack of servers periodically sends its **temperature** (including **time** and **id**).

```
1471431614103|3|40.32
1471431615103|3|41.20
```
If during the last **X seconds** >2 events of **high temperature** (>40.0) occured on a rack, an alert is launched. This alert contains **time** and **events** that triggered the alert.

```
{"time":1471431617134,"events":[{"temp":40.32,"time":1471431614103,"rackId":3},{"temp":41.20,"time":1471431615103,"rackId":3}]}
```

### Implementation overview
Events are sent to a given *Kafka* topic which are later consumed and processedd by *Flink*, generating **alerts** that are then sent to another *Kafka* topic.

A state is mantained for each key (**rack id**) using **ValueState\<T\>** to keep a set of past events over time.

Implementation has the following structure (which is easily adaptable to other CEP solutions)

```scala

var partitionedStream = ...  // DStream[(K, Event)] K -type of the key over which state will be mantained (rackId in our case)

//mapWithState function
val updateState = (batchTime: Time, key: Int, value: Option[Event], state: State[(Option[Long], Set[Event])]) => {

  if (!state.exists) state.update((None, Set.empty))

  var updatedSet = Set[Event](value.get)

  //exclude non-relevant events
  state.get()._2.foreach( (tempEvent) => {
    if (...) updatedSet.add(tempEvent)
  } )

  var lastAlertTime = state.get()._1

  if (...) {     //verify if alert situation occurs and last alert time >= Y seconds

    lastAlertTime = Some(System.currentTimeMillis())

    //send alert to Kafka
  }

  state.update((lastAlertTime, updatedSet))

  Some((key, updatedSet)) // mapped value

}

val spec = StateSpec.function(updateState)
events.mapWithState(spec)

```







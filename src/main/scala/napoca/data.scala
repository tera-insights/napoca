package com.terainsights.napoca

import java.time.Instant

/**
  * We assume that all IDs are coercible to a string type. We thought about adding
  * type parameters, but this doesn't seem like a critical feature now and adds
  * less benefit than, say, writing tests and coming up with a simpler API.
  *
  * We also calculate the priority for you.
  */
case class BatchJob(id: String, brickID: String,
                    requiredIntervals: Int, requiredHosts: Int,
                    inserted: Instant,
                    adminSetPriority: Long)

case class ScheduledBatchJob(hostToNumInstances: Map[String, Int],
                             startInterval: Int,
                             b: BatchJob)

case class RunningBatchJob(vms: Set[String], s: ScheduledBatchJob)

case class BatchSchedule(jobs: List[BatchJob], currentInterval: Int)

/** Abstraction over the resources required to run a vm */
trait HostResources {
  val vcpus: Int
  val memory: Long
  val devices: Map[String, Int]
  def + (added: HostResources): HostResources
  def - (removed: HostResources): HostResources
}

trait HostResourcesCreator {
  def apply(vcpus: Int, memory: Long, devices: Map[String, Int]): HostResources

  def empty: HostResources
}

/**
  capabilities = hostResources potential if there were no running vms
  */
case class Host(capabilities: HostResources, id: String)

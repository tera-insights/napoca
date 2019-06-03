package com.terainsights.napoca

import java.time.Instant

/**
  * We assume that all IDs are coercible to a string type. We thought about adding
  * type parameters, but this doesn't seem like a critical feature now and adds
  * less benefit than, say, writing tests and coming up with a simpler API.
  *
  * We also calculate the priority for you.
  */
case class BatchJob(brickID: String,
                    requiredIntervals: Int, requiredHosts: Int,
                    inserted: Instant,
                    adminSetPriority: Long)

case class ScheduledBatchJob(
  hostToNumInstances: Map[String, Int],
  startInterval: Int,
  b: BatchJob
) {
  /**
   * Returns the first interval at which the job is no longer scheduled to run.
   * This is one past the last interval in which the job is running.
   */
  def endInterval: Int = startInterval + b.requiredIntervals

  /**
   * Returns the last interval in which the job is scheduled to run.
   */
  def lastInterval: Int = endInterval - 1
}

case class RunningBatchJob(vms: Set[String], s: ScheduledBatchJob) {
  def startInterval: Int = s.startInterval
  def endInterval: Int = s.endInterval
  def lastInterval: Int = s.lastInterval
}

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

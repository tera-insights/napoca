package com.terainsights.napoca.data

import java.util.UUID
import java.time.Instant

case class BatchJob(id: String, brickID: String,
                    requiredIntervals: Int, schedulingUserID: String,
                    requiredHosts: Int, inserted: Instant,
                    adminSetPriority: Long)

case class ScheduledBatchJob(hostToNumInstances: Map[String, Int],
                             startInterval: Int,
                             b: BatchJob)

case class RunningBatchJob(vms: Set[UUID], s: ScheduledBatchJob)

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

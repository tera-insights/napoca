package scheduler

import java.util.UUID
import java.time.Instant

sealed trait JobState
case object ToSchedule extends JobState
case class Scheduled(hostToNumInstances: Map[String, Int], startInterval: Int) extends JobState
case class Running(scheduled: Scheduled, vms: Set[UUID]) extends JobState

case class BatchJob(id: String, brickID: String, state: JobState,
                    requiredIntervals: Int, schedulingUserID: String,
                    requiredHosts: Int, inserted: Instant,
                    adminSetPriority: Long)

case class BatchSchedule(jobs: List[BatchJob], currentInterval: Int)

/** Abstraction over the resources required to run a vm */
trait HostUsage {
  val vcpus: Int
  val memory: Long
  val devices: Map[String, Int]
  def + (added: HostUsage): HostUsage
  def - (removed: HostUsage): HostUsage
}

trait HostUsageCreator = {
  def apply(vcpus: Int, memory: Long, devices: Map[String, Int]): HostUsage
}
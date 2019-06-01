package com.terainsights.napoca

import data._

import scala.concurrent.{ ExecutionContext, Future }
import java.time.Instant

object Scheduler {
  case class HostWithCurrentUsage(host: Host, usage: HostResources)

  case class SchedulerInfo(hosts: Set[Host], brickIDToUsage: Map[String, HostResources],
                           runningJobs: List[RunningBatchJob], intervalsToLookAt: Range,
                           jobsToSchedule: List[BatchJob],
                           hostResCreator: HostResourcesCreator,
                           costToSchedule: HostWithCurrentUsage => Long)

  private case class PotentialMatch(h: Host, toSchedule: BatchJob, interval: Int,
                            alreadyScheduledThisRound: List[ScheduledBatchJob])


  private def getTotalResourceUsage(p: PotentialMatch, s: SchedulerInfo) = {
    val relevantBrickIDNumInstances = s.runningJobs.foldLeft(
      List[(String, Int)]()){ (l, job) =>
      val inBounds = job.s.startInterval + job.s.b.requiredIntervals > p.interval
      job.s.hostToNumInstances.get(p.h.id) match {
        case Some(count) if inBounds => (job.s.b.brickID, count) :: l
        case _ => l
      }
    } ++ p.alreadyScheduledThisRound.foldLeft(List[(String, Int)]()){ (l, job) =>
      val inBounds =
        (job.startInterval + job.b.requiredIntervals > p.interval ||
           p.interval + p.toSchedule.requiredIntervals > job.startInterval)
      job.hostToNumInstances.get(p.h.id) match {
        case Some(count) if inBounds => (job.b.brickID, count) :: l
        case _ => l
      }
    }
    relevantBrickIDNumInstances.foldLeft(
      s.hostResCreator.empty){
      (accum, tuple) =>
      val (brickID, cnt) = tuple
      // safe b/c we generate this map
      val hostUsage = s.brickIDToUsage(brickID)
      Range.inclusive(1, cnt).foldLeft(accum)((x, y) => x + hostUsage)
    }
  }

  /**
    analysis:
    assumptions about the rest of the propram:
    1) assumes that hosts will never be scheduled in groups s.t. they HAVE to be
    scheduled in groups; i.e., if a job required its hosts to be scheduled in groups
    of 2s, this function could still return 3; this is maybe misleading since we pass
    the whole =toSchedule= object in, which may contain lots of metadata
    2) schedule; e.g., we could have a mandatory cleanup round and want to denote this;
    or we may want to add a round that can't be shut down; maybe it's only in this round for
    some amount of time? wildcard match on the job state created this assumption
    
    
    retval = number of jobs of this type that are schedulable on this host at this time
    assumes that the host already contains the information about "its schedule"
    "its schedule" = the jobs that are running on it both now and in the future
    O(jobs + devices)
    
    */
  private def numHostsSchedulableHere(s: SchedulerInfo, p: PotentialMatch): Int = {
    val resourcesUsed: HostResources = getTotalResourceUsage(p, s)
    val resourcesLeft: HostResources = p.h.capabilities - resourcesUsed
    val jobResourcesOneInstance = s.brickIDToUsage(p.toSchedule.brickID)
    val nCPUs = Math.floor(resourcesLeft.vcpus / jobResourcesOneInstance.vcpus)
    val mem = Math.floor(resourcesLeft.memory / jobResourcesOneInstance.memory)
    val dev = resourcesLeft.devices.foldLeft(Integer.MAX_VALUE){
      (max, tuple) =>
      val (name, cnt) = tuple
      jobResourcesOneInstance.devices.get(name) match {
        case None => max
        case Some(required) =>
          Math.min(max, Math.floor(cnt / required).toInt)
      }
    }
    Math.min(Math.min(nCPUs.toInt, mem.toInt), dev)
  }

  private def getCostHere(s: SchedulerInfo, p: PotentialMatch) = {
    val resourcesAtTime = getTotalResourceUsage(p, s)
    s.costToSchedule(HostWithCurrentUsage(p.h, resourcesAtTime))
  }

  /** runtime: O(times * times * hosts) */
  private def scheduleJobThatTakesOneHost(s: SchedulerInfo, job: BatchJob,
                                          alreadyScheduledThisRound: List[ScheduledBatchJob]) = {
    var outerHostID: Option[String] = None // host to schedule on
    val firstInterval: Option[Int] = s.intervalsToLookAt.dropWhile{ time =>
      s.hosts.foldLeft(Option.empty[(String, Long)]){ case (accum, host) =>
        val p = PotentialMatch(host, job, time,
                               alreadyScheduledThisRound)
        if (0 < numHostsSchedulableHere(s, p)) {
          val costHere = getCostHere(s, p)
          accum match {
            case Some((_, prevMinCost)) if prevMinCost > costHere =>
              Some((host.id, costHere))
            case _ => accum
          }
        } else accum
      } match {
        case Some((minCostHost, _)) =>
          outerHostID = Some(minCostHost)
          false
        case _ => true
      }
    }.headOption
    firstInterval map { x =>
      ScheduledBatchJob(hostToNumInstances = Map(outerHostID.get.toString -> 1),
                        startInterval = x, job) }
  }

  private def scheduleJobThatTakesMultipleHosts(
    s: SchedulerInfo,
    job: BatchJob, alreadyScheduledThisRound: List[ScheduledBatchJob]) = {
    import scala.collection.mutable.PriorityQueue
    implicit val ord = Ordering.fromLessThan[HostAllocInfo]((h1, h2) => h1.cost > h2.cost) // tested in console
    case class HostAllocInfo(cost: Long, nsched: Int, hostID: String)
    var mcHosts: Option[PriorityQueue[HostAllocInfo]] = None
    val firstInterval: Option[Int] = s.intervalsToLookAt.dropWhile { time =>
      // should be a minheap ordered by cost, but contains tuples (cost, num_schedulable)
      val lowestCostHosts = PriorityQueue.empty[HostAllocInfo]
      s.hosts.foreach{host =>
        val p = PotentialMatch(host, job, time, alreadyScheduledThisRound)
        val num_schedulable: Int = numHostsSchedulableHere(s, p)
        if (num_schedulable > 0) {
          lowestCostHosts.enqueue(HostAllocInfo(cost = getCostHere(s,p),
                                                nsched = num_schedulable, hostID = host.id))
        }
      }
      val hostsAvailable = lowestCostHosts.foldLeft(0)((accum, x) => accum + x.nsched)
      if (hostsAvailable >= job.requiredHosts) {
        mcHosts = Some(lowestCostHosts.clone())
        false
      } else true
    }.headOption

    firstInterval map { x =>
      var hostToNumInstances: Map[String, Int] = Map()
      mcHosts.get.iterator.takeWhile{ hai =>
        val total = hostToNumInstances.foldLeft(0){(accum, tuple) =>
          val (k, v) = tuple
          accum + v}
        Integer.compare(total, job.requiredHosts) match {
          case -1 => false
          case 0 => false
          case _ =>
            val nWanted = job.requiredHosts - total
            val nTaken = Math.max(nWanted, hai.nsched)
            hostToNumInstances = hostToNumInstances + (hai.hostID.toString -> nTaken)
            true
        }
      }

      ScheduledBatchJob(hostToNumInstances = hostToNumInstances, startInterval = x, job)
    }
  }

  def runSchedulingAlgorithm(s: SchedulerInfo)(implicit ec: ExecutionContext): Future[List[ScheduledBatchJob]] = Future {
    val orderedJobsToSchedule =
      s.jobsToSchedule.sortWith((l, r) =>
        // true iff l goes before r
        l.priority(PriorityInfo(Instant.now)) > r.priority(PriorityInfo(Instant.now))
      )
    var result: List[ScheduledBatchJob] = List.empty
    orderedJobsToSchedule.iterator.takeWhile{ job =>
      val schedulingResult = job.requiredHosts match {
        case 1 => scheduleJobThatTakesOneHost(s, job, result)
        case _ => scheduleJobThatTakesMultipleHosts(s, job, result)
      }
      schedulingResult match {
        case None => false
        case Some(s) =>
          s :: result
          true
      }
    }
    result
  }
}

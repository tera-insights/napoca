package com.terainsights.napoca

import data._

import scala.concurrent.{ ExecutionContext, Future }

case class HostWithCurrentUsage(host: Host, usage: HostResources)

class Scheduler(hosts: Set[Host], brickIDToUsage: Map[String, HostResources],
                old: BatchSchedule, intervalsToLookAt: Range,
                hostResCreator: HostResourcesCreator,
                costToSchedule: HostWithCurrentUsage => Long) {

  private def getTotalResourceUsage(h: Host, toSchedule: BatchJob,
                                    alreadyScheduledThisRound: List[BatchJob],
                                    interval: Int): HostResources = {
    val relevantBrickIDNumInstances: List[(String, Int)] =
      old.jobs.foldLeft(List[(String, Int)]())( (l, job) =>
        job.state match {
          case r: Running =>
            val inBounds = r.scheduled.startInterval + job.requiredIntervals > interval
            r.scheduled.hostToNumInstances.get(h.id) match {
              case Some(count) if inBounds => (job.brickID, count) :: l
              case _ => l
            }
          case _ => l
        }) ++
        alreadyScheduledThisRound.foldLeft(List[(String, Int)]())( (l, job) =>
          job.state match {
            case s: Scheduled =>
              val inBounds =
                (s.startInterval + job.requiredIntervals > interval ||
                   interval + toSchedule.requiredIntervals > s.startInterval)
              s.hostToNumInstances.get(h.id) match {
                case Some(count) if inBounds => (job.brickID, count) :: l
                case _ => l
              }
            case r: Running =>
              throw new RuntimeException("Jobs shouldn't be scheduled to the running state")
            case _ => l
          })
    relevantBrickIDNumInstances.foldLeft(
      hostResCreator.empty){
      (accum, tuple) =>
      val (brickID, cnt) = tuple
      // safe b/c we generate this map
      val hostUsage = brickIDToUsage(brickID)
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
    2) that the brick IDs are strings (generics might help here; would that be hard in scala?)
    3) that there are no other states, besides =running= we need to consider when looking at the previous
    schedule; e.g., we could have a mandatory cleanup round and want to denote this;
    or we may want to add a round that can't be shut down; maybe it's only in this round for
    some amount of time? wildcard match on the job state created this assumption


    retval = number of jobs of this type that are schedulable on this host at this time
    assumes that the host already contains the information about "its schedule"
    "its schedule" = the jobs that are running on it both now and in the future
    O(jobs + devices)

    */
  private def numHostsSchedulableHere(
    toSchedule: BatchJob,
    alreadyScheduledThisRound: List[BatchJob],
    h: Host, interval: Int): Int = {
    val resourcesUsed: HostResources = getTotalResourceUsage(h, toSchedule,
                                                             alreadyScheduledThisRound,
                                                             interval)
    val resourcesLeft: HostResources = h.capabilities - resourcesUsed
    val jobResourcesOneInstance = brickIDToUsage(toSchedule.brickID)
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

  private def getCostHere(interval: Int, h: Host,
                       alreadyScheduledThisRound: List[BatchJob], job: BatchJob) =
  {
    val resourcesAtTime = getTotalResourceUsage(h, job,
                                                alreadyScheduledThisRound, interval)
    costToSchedule(HostWithCurrentUsage(h, resourcesAtTime))
  }

  /** runtime: O(times * times * hosts) */
  private def scheduleJobThatTakesOneHost(job: BatchJob,
                                          alreadyScheduledThisRound: List[BatchJob]):
      Option[Scheduled] = {
    var outerHostID: Option[String] = None // host to schedule on
    val firstInterval: Option[Int] = intervalsToLookAt.dropWhile{ time =>
      hosts.foldLeft(Option.empty[(String, Long)]){ case (accum, host) =>
        if (0 < numHostsSchedulableHere(job, alreadyScheduledThisRound, host, time)) {
          val costHere = getCostHere(time, host, alreadyScheduledThisRound, job)
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
      Scheduled(hostToNumInstances = Map(outerHostID.get.toString -> 1),
                startInterval = x) }
  }

  private def scheduleJobThatTakesMultipleHosts(
    job: BatchJob, alreadyScheduledThisRound: List[BatchJob]): Option[Scheduled] = {
    import scala.collection.mutable.PriorityQueue
    implicit val ord = Ordering.fromLessThan[HostAllocInfo]((h1, h2) => h1.cost > h2.cost) // tested in console
    case class HostAllocInfo(cost: Long, nsched: Int, hostID: String)
    var mcHosts: Option[PriorityQueue[HostAllocInfo]] = None
    val firstInterval: Option[Int] = intervalsToLookAt.dropWhile { time =>
      // should be a minheap ordered by cost, but contains tuples (cost, num_schedulable)
      val lowestCostHosts = PriorityQueue.empty[HostAllocInfo]
      hosts.foreach{host =>
        val num_schedulable: Int = numHostsSchedulableHere(job, alreadyScheduledThisRound,                                                             host, time)
        if (num_schedulable > 0) {
          lowestCostHosts.enqueue(HostAllocInfo(cost = getCostHere(
                                                  time, host,
                                                  alreadyScheduledThisRound,
                                                  job),
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

      Scheduled(hostToNumInstances = hostToNumInstances, startInterval = x)
    }
  }

  /**
   produces a set of those jobs that were scheduled
    if running this for the first time, can simply create a dummy batch schedule with an empty
    job list

    perhaps this should return a Future[List[Scheduled]]?
   */
  def runSchedulingAlgorithm()(implicit ec: ExecutionContext): Future[List[BatchJob]] = Future {
    val orderedJobsToSchedule =
      old.jobs.filter(x => x.state == ToSchedule).sortWith((l, r) =>
        // true iff l goes before r
        l.inserted.isBefore(r.inserted) &&
          l.adminSetPriority > r.adminSetPriority
      )
    var result: List[BatchJob] = List.empty
    orderedJobsToSchedule.iterator.takeWhile{ job =>
      val schedulingResult = job.requiredHosts match {
        case 1 => scheduleJobThatTakesOneHost(job, result)
        case _ => scheduleJobThatTakesMultipleHosts(job, result)
      }
      schedulingResult match {
        case None => false
        case Some(sched) =>
          job.copy(state = sched) :: result
          true
      }
    }
    result
  }
}


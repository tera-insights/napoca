package scheduler // perhaps this should be renamed to reduce the change of collisions

object Scheduler {
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
    toSchedule: BatchJob, old: BatchSchedule,
    alreadyScheduledThisRound: List[BatchJob],
    h: LibvirtHost, interval: Int, brickIDToUsage: Map[String, HostUsage],
    hostUsageCreator: HostUsageCreator): Int = {
    // running job; note this job could be on multiple hosts so that "multihost" fact has to
    // be taken into acct. with the neat operators below
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
    val resourcesUsed: HostUsage = relevantBrickIDNumInstances.foldLeft(
      HostUsage.empty){
      (accum, tuple) =>
      val (brickID, cnt) = tuple
      // safe b/c we generate this map
      val hostUsage = idToBrick(brickID).usage
      Range.inclusive(1, cnt).foldLeft(accum)((x, y) => x + hostUsage)
    }
    val allResources: HostHardwareProfile = config.profiles(h.profileID)
    val resourcesLeft: HostUsage = // i dont think we care about `vms`
      HostUsageCreator(vcpus = allResources.numCores,
                memory = allResources.memoryBytes,
                devices =
                  allResources.devices.foldLeft(Map[String, Int]()){(m, d) =>
                    val old = m.getOrElse(d.deviceType, 0)
                      m + (d.deviceType -> (old + 1))
                  }) - resourcesUsed
    resourcesLeft match {
      // matching to prevent hidden coupling when new fields are added to the host
      case HostUsage(_, vcpus, memory, devices) =>
        val jobResourcesOneInstance = idToBrick(toSchedule.brickID).usage
        val nCPUs = Math.floor(vcpus / jobResourcesOneInstance.vcpus)
        val mem = Math.floor(memory / jobResourcesOneInstance.memory)
        val dev = devices.foldLeft(Integer.MAX_VALUE){
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
  }




  private sealed trait ScheduledStatus
  case object FailedToSchedule extends ScheduledStatus
  case object SuccessfulSchedule(s: Scheduled) extends ScheduledStatus

  /** runtime: O(times * times * hosts) */
  private def scheduleJobThatTakesOneHost(): ScheduledStatus = {
    var outerHostID: Option[String] = None // host to schedule on
    val firstInterval: Option[Int] = intervalsToLookAt.dropWhile{ time =>
      h.foldLeft(Option.empty[(String, Long)]){ case (accum, host) =>
        if (0 < numHostsSchedulableHere(job, old, result, host, time, idToBrick)) {
          val costHere: Long = cost_to_schedule(time, host, job)
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
    firstInterval match {
      case None => FailedToSchedule
      case Some(x) =>
          Scheduled(hostToNumInstances = Map(outerHostID.get.toString -> 1),
                    startInterval = x)
  }

  private def scheduleJobThatTakesMultipleHosts = ???

  // produces a set of those jobs that were scheduled
  def runSchedulingAlgorithm(h: Set[LibvirtHost], idToBrick: Map[String, Brick], old: BatchSchedule, intervalsToLookAt: Range): Future[List[BatchJob]] = Future {
    val orderedJobsToSchedule =
      old.jobs.filter(x => x.state == ToSchedule).sortWith((l, r) =>
        // true iff l goes before r
        l.inserted.isBefore(r.inserted) &&
          l.adminSetPriority > r.adminSetPriority
      )
    var result: List[BatchJob] = List.empty
    orderedJobsToSchedule.iterator.takeWhile{ job =>
      if (job.requiredHosts == 1) {
      } else {
        import scala.collection.mutable.PriorityQueue
        implicit val ord = Ordering.fromLessThan[HostAllocInfo]((h1, h2) => h1.cost > h2.cost) // tested in console
        case class HostAllocInfo(cost: Long, nsched: Int, hostID: String)
        var mcHosts: Option[PriorityQueue[HostAllocInfo]] = None
        val firstInterval: Option[Int] = intervalsToLookAt.dropWhile { time =>
          // should be a minheap ordered by cost, but contains tuples (cost, num_schedulable)
          val lowestCostHosts = PriorityQueue.empty[HostAllocInfo]
          h.foreach{host =>
            val num_schedulable: Int = numHostsSchedulableHere(job, old, result, host, time, idToBrick)
            if (num_schedulable > 0) {
              lowestCostHosts.enqueue(HostAllocInfo(cost = getCost(job, host, time), nsched = num_schedulable, hostID = host.id))
              }
          }
          val hostsAvailable = lowestCostHosts.foldLeft(0)((accum, x) => accum + x.nsched)
          if (hostsAvailable >= job.requiredHosts) {
            mcHosts = Some(lowestCostHosts.clone())
            false
          } else true
        }.headOption

        firstInterval match {
          case Some(x) =>
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
            result = job.copy(
              state = Scheduled(hostToNumInstances = hostToNumInstances,
                                startInterval = x)) :: result
            true
          case None =>
            false // couldnt schedule this so we're done
        }
      }
    }
    result
  }
}


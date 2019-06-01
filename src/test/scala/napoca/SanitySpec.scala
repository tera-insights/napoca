package com.terainsights.napoca

import org.scalatest._
import scalaz._
import Scalaz._
import scala.concurrent._
import ExecutionContext.Implicits.global
import java.time.Instant

import com.terainsights.napoca._

case class SimpleHostResources(vcpus: Int, memory: Long, devices: Map[String, Int]) extends HostResources {
  def +(added: HostResources) = SimpleHostResources(vcpus = this.vcpus + added.vcpus,
                                           memory = memory + added.memory,
                                           devices = devices |+| added.devices)

  def -(other: HostResources) = {
    val inv = other.devices.map{case (k, v) => (k, -1 * v)}
    val d = (this.devices |+| inv).filter{case (k, v) => v > 0}
    this.copy(vcpus = vcpus - other.vcpus,
              memory = memory - other.memory, d)
  }
}

object BaseCreator extends HostResourcesCreator {
  def empty = SimpleHostResources(0, 0L, Map())

  def apply(vcpus: Int, memory: Long, devices: Map[String, Int]) =
    SimpleHostResources(vcpus, memory, devices)
}


class SanitySpec extends FlatSpec with Matchers {
  val base = Scheduler.SchedulerInfo(Set(), Map(), List(),
                                     Range.inclusive(50, 100), Map(), BaseCreator,
                                     { _ => 0L })

  val cpuBrickID = "cpu"
  val memBrickID = "mem"
  val simpleCPURes = SimpleHostResources(1, 0, Map())
  val simpleMemRes = SimpleHostResources(0, 1, Map())


  "The scheduler" should "should stop when it can't schedule more jobs" in {
    val hosts = Set(Host(simpleCPURes, "a"), Host(simpleCPURes, "b"),
                    Host(simpleMemRes, "c"))
    val brickIDToUsage = Map(cpuBrickID -> simpleCPURes, memBrickID -> simpleMemRes)
    val now = Instant.now
    val mine = base.copy(
      hosts = hosts, toSchedule = Map(
        "a" -> BatchJob(inserted = now, brickID = cpuBrickID,
                 requiredIntervals = 50, requiredHosts = 1,

                 adminSetPriority = 100L ),

        "b" -> BatchJob(inserted = now, brickID = cpuBrickID,
                 requiredIntervals = 50, requiredHosts = 1,
                 adminSetPriority = 90L ),

        "c" -> BatchJob(inserted = now, brickID = cpuBrickID,
                 requiredIntervals = 50, requiredHosts = 1,
                 adminSetPriority = 80L ),

        "d" -> BatchJob(inserted = now, brickID = memBrickID,
                 requiredIntervals = 50, requiredHosts = 1,
                 adminSetPriority = 70L ))
    )
    for {
      res <- Scheduler.runSchedulingAlgorithm(mine)
    } yield {
      res.keySet shouldEqual Set("a", "b")
    }
  }
}

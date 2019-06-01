package com.terainsights.napoca

import org.scalatest._
import scalaz._
import Scalaz._
import scala.concurrent._
import ExecutionContext.Implicits.global

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
                                     Range.inclusive(50, 100), List(), BaseCreator,
                                     { _ => 0L })

  val cpuBrickID = "cpu"
  val memBrickID = "mem"
  val simpleCPURes = SimpleHostResources(1, 0, Map())
  val simpleMemRes = SimpleHostResources(0, 1, Map())


  "The scheduler" should "should stop when it can't schedule more jobs" in {
    val hosts = Set(Host(simpleCPURes, "a"), Host(simpleCPURes, "b"),
                    Host(simpleMemRes, "c"))
    val brickIDToUsage = Map(cpuBrickID -> simpleCPURes, memBrickID -> simpleMemRes)
    val mine = base.copy(
      hosts = hosts, toSchedule = List(
        BatchJob("a", brickID = cpuBrickID, requiredIntervals = 50, requiredHosts = 1,
                 priority = { _ => 100L }),

        BatchJob("b", brickID = cpuBrickID, requiredIntervals = 50, requiredHosts = 1,
                 priority = { _ => 90L }),

        BatchJob("c", brickID = cpuBrickID, requiredIntervals = 50, requiredHosts = 1,
                 priority = { _ => 80L }),

        BatchJob("d", brickID = memBrickID, requiredIntervals = 50, requiredHosts = 1,
                 priority = { _ => 70L }))
    )
    for {
      res <- Scheduler.runSchedulingAlgorithm(mine)
    } yield {
      res.map(_.b.id) shouldEqual List("a", "b")
    }
  }
}

package kafka.controllerV2

import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.AsyncCallback.DataCallback
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

object ZookeeperReadPerformance {
  def main(args: Array[String]): Unit = {
    val zkConnect = args(0)
    val count = Integer.parseInt(args(1))
    println(s"running ZookeeperReadPerformance at $zkConnect with count: $count")
    val zookeeperReadPerformance = new ZookeeperReadPerformance(zkConnect, count)
    zookeeperReadPerformance.runAll()
  }
}

class ZookeeperReadPerformance(zkConnect: String, count: Int) extends Watcher {
  val zookeeper = new ZooKeeper(zkConnect, 30000, this)

  def path(x: Int) = s"/$x"
  val checkPath = path(Int.MaxValue)

  def setup: Unit = {
    (1 to count).foreach { x =>
      try {
        zookeeper.create(path(x), Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      } catch {
        case e: KeeperException.NodeExistsException =>
      }
    }
    try {
      zookeeper.create(checkPath, "".getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    } catch {
      case e: KeeperException.NodeExistsException =>
    }
  }

  def teardown: Unit = {
    (1 to count).foreach { x =>
      try {
        zookeeper.delete(path(x), -1)
      } catch {
        case e: KeeperException.NoNodeException =>
      }
    }
    try {
      zookeeper.delete(checkPath, -1)
    } catch {
      case e: KeeperException.NoNodeException =>
    }
  }

  def time(f: () => Unit): (Long, Option[Exception]) = {
    val start = System.currentTimeMillis()
    try {
      f()
      val end = System.currentTimeMillis()
      (end - start, None)
    } catch {
      case e: Exception =>
        val end = System.currentTimeMillis()
        (end - start, Option(e))
    }
  }

  def test(name: String, f: () => Unit): Unit = {
    val (duration, exceptionOpt) = time(f)
    if (exceptionOpt.isEmpty) println(s"$name completed after $duration ms")
    else println(s"$name failed after $duration ms with exception: ${exceptionOpt.get}")
  }

  def sync(): Unit = {
    (1 to count).foreach(x => zookeeper.getData(path(x), false, null))
  }

  def async(): Unit = {
    val countDownLatch = new CountDownLatch(count)
    (1 to count).foreach(x => zookeeper.getData(path(x), false, new DataCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat) = {
        val code = Code.get(rc)
        countDownLatch.countDown()
        if (code != Code.OK) throw new RuntimeException(s"error code: $code")
      }
    }, null))
    countDownLatch.await()
  }

  def runAll(): Unit = {
    println("setting up")
    setup
    test("sync", sync)
    test("async", async)
    println("tearing down")
    teardown
    zookeeper.close()
  }

  override def process(event: WatchedEvent): Unit = {}
}
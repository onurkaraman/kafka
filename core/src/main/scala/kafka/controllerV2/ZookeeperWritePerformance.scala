package kafka.controllerV2

import java.util
import java.util.UUID
import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.AsyncCallback.{MultiCallback, StatCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, KeeperException, Op, OpResult, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

object ZookeeperWritePerformance {
  def main(args: Array[String]): Unit = {
    val zkConnect = args(0)
    val count = Integer.parseInt(args(1))
    println(s"running ZookeeperWritePerformance at $zkConnect with count: $count")
    val zookeeperWritePerformance = new ZookeeperWritePerformance(zkConnect, count)
    zookeeperWritePerformance.runAll()
  }
}

class ZookeeperWritePerformance(zkConnect: String, count: Int) extends Watcher {
  val zookeeper = new ZooKeeper(zkConnect, 30000, this)

  def bytes = UUID.randomUUID().toString.getBytes

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
    val data = bytes
    (1 to count).foreach(x => zookeeper.setData(path(x), data, -1))
  }

  def async(): Unit = {
    val countDownLatch = new CountDownLatch(count)
    val data = bytes
    (1 to count).foreach(x => zookeeper.setData(path(x), data, -1, new StatCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, stat: Stat): Unit = {
        val code = Code.get(rc)
        countDownLatch.countDown()
        if (code != Code.OK) throw new RuntimeException(s"error code: $code")
      }
    }, null))
    countDownLatch.await()
  }

  def multi(): Unit = {
    val ops = new util.ArrayList[Op]()
    val data = bytes
    (1 to count).foreach(x => ops.add(Op.setData(path(x), data, -1)))
    zookeeper.multi(ops)
  }

  def batchedMulti(size: Int): Unit = {
    val data = bytes
    val ops = (1 to count).map(x => Op.setData(path(x), data, -1)).toArray
    val batches = ops.grouped(size)
    val jBatches = batches.map { batch =>
      val list = new util.ArrayList[Op]()
      batch.foreach(list.add)
      list
    }
    jBatches.foreach(zookeeper.multi)
  }

  def asyncMultiCheckAndSetData(): Unit = {
    val countDownLatch = new CountDownLatch(count)
    val data = bytes
    val checkStat = new Stat()
    zookeeper.getData(checkPath, false, checkStat)
    (1 to count).foreach { x =>
      val ops = new util.ArrayList[Op]()
      ops.add(Op.check(checkPath, checkStat.getVersion))
      ops.add(Op.setData(path(x), data, -1))
      zookeeper.multi(ops, new MultiCallback {
        override def processResult(rc: Int, path: String, ctx: scala.Any, opResults: util.List[OpResult]) = {
          val code = Code.get(rc)
          countDownLatch.countDown()
          if (code != Code.OK) throw new RuntimeException(s"error code: $code")
        }
      }, null)
    }
    countDownLatch.await()
  }

  def asyncMultiCheckAndBatchSetData(size: Int): Unit = {
    val data = bytes
    val checkStat = new Stat()
    zookeeper.getData(checkPath, false, checkStat)
    val ops = (1 to count).map(x => Op.setData(path(x), data, -1)).toArray
    val batches = ops.grouped(size)
    val opsets = batches.map { ops =>
      val l = new util.ArrayList[Op]()
      l.add(Op.check(checkPath, checkStat.getVersion))
      ops.foreach(op => l.add(op))
      l
    }.toList
    val countDownLatch = new CountDownLatch(opsets.size)
    opsets.foreach { opset =>
      zookeeper.multi(opset, new MultiCallback {
        override def processResult(rc: Int, path: String, ctx: scala.Any, opResults: util.List[OpResult]) = {
          val code = Code.get(rc)
          countDownLatch.countDown()
          if (code != Code.OK) throw new RuntimeException(s"error code: $code")
        }
      }, null)
    }
    countDownLatch.await()
  }


  def runAll(): Unit = {
    println("setting up")
    setup
    test("sync", sync)
    test("async", async)
    test("batched multi size 5000", () => batchedMulti(5000))
    test("batched multi size 1000", () => batchedMulti(1000))
    test("batched multi size 500", () => batchedMulti(500))
    test("batched multi size 100", () => batchedMulti(100))
    test("batched multi size 50", () => batchedMulti(50))
    test("batched multi size 10", () => batchedMulti(10))
    test("async multi check and setData", asyncMultiCheckAndSetData)
    test("async multi check and batch 5000 setData", () => asyncMultiCheckAndBatchSetData(5000))
    test("async multi check and batch 1000 setData", () => asyncMultiCheckAndBatchSetData(1000))
    test("async multi check and batch 500 setData", () => asyncMultiCheckAndBatchSetData(500))
    test("async multi check and batch 100 setData", () => asyncMultiCheckAndBatchSetData(100))
    test("async multi check and batch 50 setData", () => asyncMultiCheckAndBatchSetData(50))
    test("async multi check and batch 10 setData", () => asyncMultiCheckAndBatchSetData(10))
    test("multi", multi)
    println("tearing down")
    teardown
    zookeeper.close()
  }

  override def process(event: WatchedEvent): Unit = {}
}
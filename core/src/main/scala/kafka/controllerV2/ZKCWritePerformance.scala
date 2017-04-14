package kafka.controllerV2

import java.util
import java.util.UUID
import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.AsyncCallback.{MultiCallback, StatCallback, StringCallback, VoidCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, Op, OpResult, ZooDefs}

object ZKCWritePerformance {
  def main(args: Array[String]): Unit = {
    val zkConnect = args(0)
    val count = Integer.parseInt(args(1))
    println(s"running ZKCWritePerformance at $zkConnect with count: $count")
    val zKCWritePerformance = new ZKCWritePerformance(zkConnect, count)
    zKCWritePerformance.runAll()
  }
}

class ZKCWritePerformance(zkConnect: String, count: Int) {
  val eventHandler = new EventHandler
  val zkc = new ZKC(zkConnect, 30000, 30000, eventHandler)
  eventHandler.start()
  zkc.reinitSession()

  def bytes = UUID.randomUUID().toString.getBytes

  def path(x: Int) = s"/$x"
  val checkPath = path(Int.MaxValue)

  def setup: Unit = {
    val countDownLatch = new CountDownLatch(count + 1);
    (1 to count).foreach { x =>
      zkc.create(path(x), Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new StringCallback {
        override def processResult(rc: Int, path: String, ctx: scala.Any, name: String) = {
          countDownLatch.countDown()
        }
      }, null)
    }
    zkc.create(checkPath, "".getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new StringCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, name: String) = {
        countDownLatch.countDown()
      }
    }, null)
  }

  def teardown: Unit = {
    val countDownLatch = new CountDownLatch(count + 1);
    (1 to count).foreach { x =>
        zkc.delete(path(x), -1, new VoidCallback {
          override def processResult(rc: Int, path: String, ctx: scala.Any) = {
            countDownLatch.countDown()
          }
        }, null)
    }
    zkc.delete(checkPath, -1, new VoidCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any) = {
        countDownLatch.countDown()
      }
    }, null)
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

  def async(): Unit = {
    val countDownLatch = new CountDownLatch(count)
    val data = bytes
    (1 to count).foreach(x => zkc.setData(path(x), data, -1, new StatCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, stat: Stat): Unit = {
        val code = Code.get(rc)
        countDownLatch.countDown()
        if (code != Code.OK) throw new RuntimeException(s"error code: $code")
      }
    }, null))
    countDownLatch.await()
  }

  def asyncMultiCheckAndSetData(): Unit = {
    val countDownLatch = new CountDownLatch(count)
    val data = bytes
    zkc.exists(checkPath, new StatCallback {
      override def processResult(rc: Int, p: String, ctx: scala.Any, stat: Stat) = {
        (1 to count).foreach { x =>
          val ops = new util.ArrayList[Op]()
          ops.add(Op.check(checkPath, stat.getVersion))
          ops.add(Op.setData(path(x), data, -1))
          zkc.multi(ops, new MultiCallback {
            override def processResult(rc: Int, path: String, ctx: scala.Any, opResults: util.List[OpResult]) = {
              val code = Code.get(rc)
              countDownLatch.countDown()
              if (code != Code.OK) throw new RuntimeException(s"error code: $code")
            }
          }, null)
        }
      }
    }, null)
    countDownLatch.await()
  }

  def asyncMultiCheckAndBatchSetData(size: Int): Unit = {
    val data = bytes
    val countDownLatch = new CountDownLatch((1 to count).grouped(size).size)
    zkc.exists(checkPath, new StatCallback {
      override def processResult(rc: Int, p: String, ctx: scala.Any, stat: Stat): Unit = {
        val ops = (1 to count).map(x => Op.setData(path(x), data, -1)).toArray
        val batches = ops.grouped(size)
        val opsets = batches.map { ops =>
          val l = new util.ArrayList[Op]()
          l.add(Op.check(checkPath, stat.getVersion))
          ops.foreach(op => l.add(op))
          l
        }.toList
        opsets.foreach { opset =>
          zkc.multi(opset, new MultiCallback {
            override def processResult(rc: Int, path: String, ctx: scala.Any, opResults: util.List[OpResult]) = {
              val code = Code.get(rc)
              countDownLatch.countDown()
              if (code != Code.OK) throw new RuntimeException(s"error code: $code")
            }
          }, null)
        }

      }
    }, null)
    countDownLatch.await()
  }

  def runAll(): Unit = {
    println("setting up")
    setup
    test("async", async)
    test("async multi check and setData", asyncMultiCheckAndSetData)
    test("async multi check and batch 5000 setData", () => asyncMultiCheckAndBatchSetData(5000))
    test("async multi check and batch 1000 setData", () => asyncMultiCheckAndBatchSetData(1000))
    test("async multi check and batch 500 setData", () => asyncMultiCheckAndBatchSetData(500))
    test("async multi check and batch 100 setData", () => asyncMultiCheckAndBatchSetData(100))
    test("async multi check and batch 50 setData", () => asyncMultiCheckAndBatchSetData(50))
    test("async multi check and batch 10 setData", () => asyncMultiCheckAndBatchSetData(10))
    println("tearing down")
    teardown
    zkc.close()
  }

}
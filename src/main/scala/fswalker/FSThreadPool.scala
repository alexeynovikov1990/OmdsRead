package fswalker

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

class FSThreadPool( val threadCount: Int) {

  def addTask(task: Runnable): Unit = {
    tasks.put(task)
  }

  def start(): Unit = {
    threads.clear()
    for (i <- 0 until threadCount) {
      val t: Thread = new Thread(new Worker())
      t.start()
      threads += t
    }
  }

  def isTasksCompleted(): Boolean = {
    var rslt:Boolean = true
    for (idx <- 0 to (threads.length - 1)){
        if (threads(idx).getState != Thread.State.WAITING) {
          rslt = false
        }
    }
    //Если все потоки находятся в состоянии ожидания,
    //то они должны быть завершены т.к. обход закончен
    if (rslt){
      for (idx <- 0 to (threads.length - 1)){
        threads(idx).interrupt()
      }
    }
    rslt
  }

  private def getNextTask(): Runnable = tasks.take()

  private class Worker extends Runnable {
    override def run(): Unit = {
      try while (true) FSThreadPool.this.getNextTask.run()
      catch {
        case ex: InterruptedException => None
      }
    }
  }

  private val tasks: LinkedBlockingQueue[Runnable] = new LinkedBlockingQueue()
  private val threads: ArrayBuffer[Thread] = new ArrayBuffer()

}

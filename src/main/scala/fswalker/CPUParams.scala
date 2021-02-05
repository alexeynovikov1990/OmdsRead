package fswalker

import java.nio.ByteOrder

class CPUParams {

  def getInfo(): Unit = {
    coresCount = Runtime.getRuntime.availableProcessors
    bo = ByteOrder.nativeOrder
  }
  def getCoresCount: Int = coresCount
  def getByteOrder: ByteOrder = bo

  private var coresCount = 0;
  private var bo:ByteOrder = _
}

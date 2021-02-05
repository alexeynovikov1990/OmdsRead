package fswalker

object SystemSettings {
  def Init(): Unit = {
    cpu.getInfo
  }
  val cpu: CPUParams = new CPUParams()
}

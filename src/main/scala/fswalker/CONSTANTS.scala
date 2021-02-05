package fswalker

object CONSTANTS {
  object SYSTEMCALLS64 {
    //https://github.com/torvalds/linux/blob/master/arch/x86/entry/syscalls/syscall_64.tbl
    val getdents64: Int = 217
  }
  //Чтение наиболее эффективно если размер буфера кратен как размеру страницы в памяти, так и размеру блока файловой системы
  val cacheBufferSize: Int = 4096 * 16
  val maxFileNameLegth: Int = 256
  val completionTimeout = 50

  val O_RDONLY = 0
  val INVALID_FD: Int = -1
  val DT_DIR = 4
  val DT_REG = 8
}

package fswalker

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import com.sun.jna.Pointer
import scala.collection.mutable.ArrayBuffer

object LowLevelUtils {

  object GETDENTS64 {

    def ReadDirEntries(buffer: Array[Byte]): ArrayBuffer[LowLevelStructures.LinuxDirent64] = {
      val entries: ArrayBuffer[LowLevelStructures.LinuxDirent64] = new ArrayBuffer[LowLevelStructures.LinuxDirent64]()
      val buf: ByteBuffer = ByteBuffer.wrap(buffer, 0, buffer.length)
      buf.order(SystemSettings.cpu.getByteOrder)
      while (buf.remaining() > 0) {
        val entry: LowLevelStructures.LinuxDirent64 = new LowLevelStructures.LinuxDirent64()
        if (entry.readFromBuffer(buf)) entries += entry
      }
      entries
    }

    def readFilesInfo(dirPath: String): Option[Array[Byte]] = {
      var content: Option[Array[Byte]] = None
      val jnaLib: JNA = JNA.INSTANCE
      val hDir: Int = JNA.INSTANCE.open(dirPath, CONSTANTS.O_RDONLY.asInstanceOf[Object])
      if (hDir != CONSTANTS.INVALID_FD) {
        try {
          val buffer: Array[Byte] = Array.ofDim[Byte](CONSTANTS.cacheBufferSize)
          val baos: ByteArrayOutputStream = new ByteArrayOutputStream()
          var len: Int = JNA.INSTANCE.syscall(CONSTANTS.SYSTEMCALLS64.getdents64, hDir.asInstanceOf[Object],
                                              buffer.asInstanceOf[Object], CONSTANTS.cacheBufferSize.asInstanceOf[Object])
          while (len  > 0) {
            baos.write(buffer, 0, len)
            len = JNA.INSTANCE.syscall(CONSTANTS.SYSTEMCALLS64.getdents64, hDir.asInstanceOf[Object],
                                        buffer.asInstanceOf[Object], CONSTANTS.cacheBufferSize.asInstanceOf[Object])
          }
          content = Option.apply(baos.toByteArray())
        } catch {
          case ex: Exception => ex.printStackTrace()
        }
        JNA.INSTANCE.close(hDir)
      }
      content
    }

  }

  object READDIR {

    def ReadDirEntries(dirPath: String): ArrayBuffer[LowLevelStructures.LinuxDirent64] = {
      val entries: ArrayBuffer[LowLevelStructures.LinuxDirent64] = new ArrayBuffer[LowLevelStructures.LinuxDirent64]()
      val dir: Pointer = JNA.INSTANCE.opendir(dirPath)
      if (dir != null) {
        var dirent: Pointer = null
        val header: Array[Byte] = Array.ofDim[Byte](LowLevelStructures.LinuxDirent64.HEADER_SIZE)

        dirent = JNA.INSTANCE.readdir(dir)
        while (dirent != null) try {
          val entry: LowLevelStructures.LinuxDirent64 = new LowLevelStructures.LinuxDirent64()
          dirent.read(
            LowLevelStructures.LinuxDirent64.SKIPED_FIELD_SIZE,
            header,
            0,
            LowLevelStructures.LinuxDirent64.HEADER_SIZE - LowLevelStructures.LinuxDirent64.SKIPED_FIELD_SIZE
          )
          var buf: ByteBuffer = ByteBuffer.wrap(header)
          buf.order(SystemSettings.cpu.getByteOrder)
          val entrysize: Short = buf.getShort
          buf = ByteBuffer.wrap(dirent.getByteArray(0, entrysize), 0, entrysize)
          buf.order(SystemSettings.cpu.getByteOrder)
          if (entry.readFromBuffer(buf)) entries += entry
          dirent = JNA.INSTANCE.readdir(dir)
        } catch {
          case ex: Exception =>
            System.out.printf("Exception %s\n", ex.toString)
        }
        JNA.INSTANCE.closedir(dir)
      }
      entries
    }
  }
}
package fswalker

import java.io.UnsupportedEncodingException
import java.nio.ByteBuffer

object LowLevelStructures {

  /*На x64-платформах форматы структур для вызовов getdents64 и readdir совпадают
       struct linux_dirent64 {
           u64     d_ino;
           s64     d_off;
           unsigned short  d_reclen;
           unsigned char   d_type;
           char        d_name[0];
      };
      */

  object LinuxDirent64 {
    val SKIPED_FIELD_SIZE: Int = 16
    val HEADER_SIZE: Int = SKIPED_FIELD_SIZE + 2 + 1
    val MAX_DIRENT_LEGTH: Int = HEADER_SIZE + CONSTANTS.maxFileNameLegth
  }

  class LinuxDirent64 {

    def readFromBuffer(buffer: ByteBuffer): Boolean =
      (if (readHeader(buffer)) readFileName(buffer) else false)

    private def readHeader(buffer: ByteBuffer): Boolean =
      if (buffer.remaining() > LinuxDirent64.HEADER_SIZE) {
        buffer.position(buffer.position() + LinuxDirent64.SKIPED_FIELD_SIZE)
        d_reclen = buffer.getShort
        d_type = buffer.get
        true
      } else false

    private def readFileName(buffer: ByteBuffer): Boolean =
      if (buffer.remaining() >= d_reclen - LinuxDirent64.HEADER_SIZE) {
        try {
          val namebuf: Array[Byte] = Array.ofDim[Byte](d_reclen - LinuxDirent64.HEADER_SIZE)
          buffer.get(namebuf)
          var len: Int = 0
          while (namebuf(len) != 0) {
            len += 1
          }
          d_name = new String(namebuf, 0, len, "UTF-8")
          true
        } catch {
          case ex: UnsupportedEncodingException => false
        }
      } else false

    def getFileName(): String = d_name
    def getFileType(): Short = d_type
    def isDirectory(): Boolean = (d_type == CONSTANTS.DT_DIR)

    //размер записи
    private var d_reclen: Short = _
    //тип файла
    private var d_type: Byte = _
    //имя
    private var d_name: String = _

  }

}


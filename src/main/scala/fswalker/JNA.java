package fswalker;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface JNA extends Library {
    JNA INSTANCE = (JNA) Native.loadLibrary(("c"), JNA.class);

    int open(String path, Object... args);
    int close(int fd);
    int syscall(long number, Object... args);
    Pointer opendir(String dirname);
    Pointer readdir(Pointer dirp);
    int closedir(Pointer dirp);
}
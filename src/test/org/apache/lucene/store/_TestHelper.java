package org.apache.lucene.store;
import java.io.RandomAccessFile;
import java.io.IOException;

/** This class provides access to package-level features defined in the
 *  store package. It is used for testing only.
 */
public class _TestHelper {

    /** Returns true if the instance of the provided input stream is actually
     *  an FSInputStream.
     */
    public static boolean isFSInputStream(InputStream is) {
        return is instanceof FSInputStream;
    }

    /** Returns true if the provided input stream is an FSInputStream and
     *  is a clone, that is it does not own its underlying file descriptor.
     */
    public static boolean isFSInputStreamClone(InputStream is) {
        if (isFSInputStream(is)) {
            return ((FSInputStream) is).isClone;
        } else {
            return false;
        }
    }

    /** Given an instance of FSDirectory.FSInputStream, this method returns
     *  true if the underlying file descriptor is valid, and false otherwise.
     *  This can be used to determine if the OS file has been closed.
     *  The descriptor becomes invalid when the non-clone instance of the
     *  FSInputStream that owns this descriptor is closed. However, the
     *  descriptor may possibly become invalid in other ways as well.
     */
    public static boolean isFSInputStreamOpen(InputStream is)
    throws IOException
    {
        if (isFSInputStream(is)) {
            FSInputStream fis = (FSInputStream) is;
            return fis.isFDValid();
        } else {
            return false;
        }
    }

}

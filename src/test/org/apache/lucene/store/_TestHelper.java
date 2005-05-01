package org.apache.lucene.store;

import java.io.IOException;

/** This class provides access to package-level features defined in the
 *  store package. It is used for testing only.
 */
public class _TestHelper {

    /** Returns true if the instance of the provided input stream is actually
     *  an FSIndexInput.
     */
    public static boolean isFSIndexInput(IndexInput is) {
        return is instanceof FSIndexInput;
    }

    /** Returns true if the provided input stream is an FSIndexInput and
     *  is a clone, that is it does not own its underlying file descriptor.
     */
    public static boolean isFSIndexInputClone(IndexInput is) {
        if (isFSIndexInput(is)) {
            return ((FSIndexInput) is).isClone;
        } else {
            return false;
        }
    }

    /** Given an instance of FSDirectory.FSIndexInput, this method returns
     *  true if the underlying file descriptor is valid, and false otherwise.
     *  This can be used to determine if the OS file has been closed.
     *  The descriptor becomes invalid when the non-clone instance of the
     *  FSIndexInput that owns this descriptor is closed. However, the
     *  descriptor may possibly become invalid in other ways as well.
     */
    public static boolean isFSIndexInputOpen(IndexInput is)
    throws IOException
    {
        if (isFSIndexInput(is)) {
            FSIndexInput fis = (FSIndexInput) is;
            return fis.isFDValid();
        } else {
            return false;
        }
    }

}

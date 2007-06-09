package org.apache.lucene.store;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.store.FSDirectory.FSIndexInput;

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

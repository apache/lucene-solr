/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.util.compress;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.UnsatisfiedLinkError;

public enum Native {
;
    private static final String libnameShort = "zstd-jni";
    private static final String libname = "lib" + libnameShort;
    private static final String errorMsg = "Unsupported OS/arch, cannot find " +
            resourceName() + " or load " + libnameShort + " from system libraries. Please " +
            "try building from source the jar or providing " + libname + " in your system.";

    private static String osName() {
        String os = System.getProperty("os.name").toLowerCase().replace(' ', '_');
        if (os.startsWith("win")){
            return "win";
        } else if (os.startsWith("mac")) {
            return "darwin";
        } else {
            return os;
        }
    }

    private static String osArch() {
        return System.getProperty("os.arch");
    }

    private static String libExtension() {
        if (osName().contains("os_x") || osName().contains("darwin")) {
            return "dylib";
        } else if (osName().contains("win")) {
            return "dll";
        } else {
            return "so";
        }
    }

    private static String resourceName() {
        return "/" + osName() + "/" + osArch() + "/" + libname + "." + libExtension();
    }

    private static boolean loaded = false;

    public static synchronized boolean isLoaded() {
        return loaded;
    }

    public static synchronized void load() {
        load(null);
    }

    public static synchronized void load(final File tempFolder) {
        if (loaded) {
            return;
        }

        String resourceName = resourceName();
        InputStream is = Native.class.getClassLoader().getResourceAsStream(resourceName);
        if (is == null) {
            // fall-back to loading the zstd-jni from the system library path.
            // It also cover loading on Android.
            try {
                System.loadLibrary(libnameShort);
                loaded = true;
                return;
            } catch (UnsatisfiedLinkError e) {
                UnsatisfiedLinkError err = new UnsatisfiedLinkError(e.getMessage() + "\n" + errorMsg);
                err.setStackTrace(e.getStackTrace());
                throw err;
            }
        }
        File tempLib = null;
        FileOutputStream out = null;
        try {
            tempLib = File.createTempFile(libname, "." + libExtension(), tempFolder);
            // try to delete on exit, does not work on Windows
            tempLib.deleteOnExit();
            // copy to tempLib
            out = new FileOutputStream(tempLib);
            byte[] buf = new byte[4096];
            while (true) {
                int read = is.read(buf);
                if (read == -1) {
                    break;
                }
                out.write(buf, 0, read);
            }
            try {
                out.flush();
                out.close();
                out = null;
            } catch (IOException e) {
                // ignore
            }
            try {
                System.load(tempLib.getAbsolutePath());
            } catch (UnsatisfiedLinkError e) {
                // fall-back to loading the zstd-jni from the system library path
                try {
                    System.loadLibrary(libnameShort);
                } catch (UnsatisfiedLinkError e1) {
                    // display error in case problem with loading from temp folder
                    // and from system library path - concatenate both messages
                    UnsatisfiedLinkError err = new UnsatisfiedLinkError(
                            e.getMessage() + "\n" +
                                    e1.getMessage() + "\n"+
                                    errorMsg);
                    err.setStackTrace(e1.getStackTrace());
                    throw err;
                }
            }
            loaded = true;
        } catch (IOException e) {
            // IO errors in extacting and writing the shared object in the temp dir
            ExceptionInInitializerError err = new ExceptionInInitializerError(
                    "Cannot unpack " + libname + ": " + e.getMessage());
            err.setStackTrace(e.getStackTrace());
            throw err;
        }
        finally {
            try {
                is.close();
                if (out != null) {
                    out.close();
                }
                if (tempLib != null && tempLib.exists()) {
                    tempLib.delete();
                }
            } catch (IOException e) {
                // ignore
            }
        }
    }
}

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.github.luben.zstd;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public enum Native {
    ;
    private static final String libnameShort = "zstd-jni";
    private static final String libname = "libzstd-jni";
    private static final String errorMsg = "Unsupported OS/arch, cannot find " + resourceName() + " or load " + "zstd-jni" + " from system libraries. Please try building from source the jar or providing " + "libzstd-jni" + " in your system.";
    private static boolean loaded = false;

    private Native() {
    }

    private static String osName() {
        String var0 = System.getProperty("os.name").toLowerCase().replace(' ', '_');
        if (var0.startsWith("win")) {
            return "win";
        } else {
            return var0.startsWith("mac") ? "darwin" : var0;
        }
    }

    private static String osArch() {
        return System.getProperty("os.arch");
    }

    private static String libExtension() {
        if (!osName().contains("os_x") && !osName().contains("darwin")) {
            return osName().contains("win") ? "dll" : "so";
        } else {
            return "dylib";
        }
    }

    private static String resourceName() {
        return "/" + osName() + "/" + osArch() + "/" + "libzstd-jni" + "." + libExtension();
    }

    public static synchronized boolean isLoaded() {
        return loaded;
    }

    public static synchronized void load() {
        load((File)null);
    }

    public static synchronized void load(File var0) {
        if (!loaded) {
            String var1 = resourceName();
            InputStream var2 = Native.class.getResourceAsStream(var1);
            if (var2 == null) {
                try {
                    System.loadLibrary("zstd-jni");
                    loaded = true;
                } catch (UnsatisfiedLinkError var20) {
                    UnsatisfiedLinkError var26 = new UnsatisfiedLinkError(var20.getMessage() + "\n" + errorMsg);
                    var26.setStackTrace(var20.getStackTrace());
                    throw var26;
                }
            } else {
                File var3 = null;
                FileOutputStream var4 = null;

                try {
                    var3 = File.createTempFile("libzstd-jni", "." + libExtension(), var0);
                    var3.deleteOnExit();
                    var4 = new FileOutputStream(var3);
                    byte[] var5 = new byte[4096];

                    while(true) {
                        int var27 = var2.read(var5);
                        if (var27 == -1) {
                            try {
                                var4.flush();
                                var4.close();
                                var4 = null;
                            } catch (IOException var23) {
                            }

                            try {
                                System.load(var3.getAbsolutePath());
                            } catch (UnsatisfiedLinkError var22) {
                                try {
                                    System.loadLibrary("zstd-jni");
                                } catch (UnsatisfiedLinkError var21) {
                                    UnsatisfiedLinkError var8 = new UnsatisfiedLinkError(var22.getMessage() + "\n" + var21.getMessage() + "\n" + errorMsg);
                                    var8.setStackTrace(var21.getStackTrace());
                                    throw var8;
                                }
                            }

                            loaded = true;
                            return;
                        }

                        var4.write(var5, 0, var27);
                    }
                } catch (IOException var24) {
                    ExceptionInInitializerError var6 = new ExceptionInInitializerError("Cannot unpack libzstd-jni: " + var24.getMessage());
                    var6.setStackTrace(var24.getStackTrace());
                    throw var6;
                } finally {
                    try {
                        var2.close();
                        if (var4 != null) {
                            var4.close();
                        }

                        if (var3 != null && var3.exists()) {
                            var3.delete();
                        }
                    } catch (IOException var19) {
                    }

                }
            }
        }
    }
}

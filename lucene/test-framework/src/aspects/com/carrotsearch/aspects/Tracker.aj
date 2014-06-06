package com.carrotsearch.aspects;

import java.util.*;

public class Tracker {
  public static class TrackingInfo {
    public final Object target;
    public volatile boolean closed;

    TrackingInfo(Object target, boolean closed) {
      this.target = target;
      this.closed = closed;
    }
    
    @Override
    public String toString() {
      return "FH: " + target + " [" + (closed ? "closed" : "open") + "]";
    }
  }

  public static class TrackingStats {
    public final Map<Object, TrackingInfo> open = new HashMap<>();
    public final Map<Object, TrackingInfo> closed = new HashMap<>();
  }

  private static final Object lock = new Object();

  /** Guarded by: {@link #lock}. */
  private static boolean tracking;
  private static IdentityHashMap<Object, TrackingInfo> trackpool = new IdentityHashMap<>();

  /**
   * Start tracking resources.
   */
  public static void startTracking() {
    synchronized (lock) {
      tracking = true;
    }
  }

  /**
   * Stop tracking resources.
   */
  public static void endTracking() {
    synchronized (lock) {
      tracking = false;
    }
  }

  /**
   * Clear currently tracked resources.
   */
  public static void reset() {
    synchronized (lock) {
      trackpool.clear();
    }
  }

  public static TrackingStats snapshot() {
    synchronized (lock) {
      TrackingStats stats = new TrackingStats();
      for (TrackingInfo ti : trackpool.values()) {
        if (ti.closed) {
          stats.closed.put(ti.target, ti);
        } else {
          stats.open.put(ti.target, ti);
        }
      }
      return stats;
    }
  }

  /**
   * Are we tracking open/ close calls?
   */
  static final boolean isTracking() {
    return tracking;
  }

  /**
   * Start tracking object (instantiated).
   */
  static final <T> T track(T target) {
    synchronized (lock) {
      assert !trackpool.containsKey(target);
      trackpool.put(target, new TrackingInfo(target, false));
    }
    return target;
  }

  /**
   * Tracked object's close method called.
   */
  static final void close(Object target) {
    synchronized (lock) {
      final TrackingInfo trackingInfo = trackpool.get(target);
      if (trackingInfo != null) {
        trackingInfo.closed = true;
      }
    }
  }
}

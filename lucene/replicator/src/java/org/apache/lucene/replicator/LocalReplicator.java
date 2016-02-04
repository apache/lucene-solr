/*
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
package org.apache.lucene.replicator;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.AlreadyClosedException;

/**
 * A {@link Replicator} implementation for use by the side that publishes
 * {@link Revision}s, as well for clients to {@link #checkForUpdate(String)
 * check for updates}. When a client needs to be updated, it is returned a
 * {@link SessionToken} through which it can
 * {@link #obtainFile(String, String, String) obtain} the files of that
 * revision. As long as a revision is being replicated, this replicator
 * guarantees that it will not be {@link Revision#release() released}.
 * <p>
 * Replication sessions expire by default after
 * {@link #DEFAULT_SESSION_EXPIRATION_THRESHOLD}, and the threshold can be
 * configured through {@link #setExpirationThreshold(long)}.
 * 
 * @lucene.experimental
 */
public class LocalReplicator implements Replicator {
  
  private static class RefCountedRevision {
    private final AtomicInteger refCount = new AtomicInteger(1);
    public final Revision revision;
    
    public RefCountedRevision(Revision revision) {
      this.revision = revision;
    }
    
    public void decRef() throws IOException {
      if (refCount.get() <= 0) {
        throw new IllegalStateException("this revision is already released");
      }
      
      final int rc = refCount.decrementAndGet();
      if (rc == 0) {
        boolean success = false;
        try {
          revision.release();
          success = true;
        } finally {
          if (!success) {
            // Put reference back on failure
            refCount.incrementAndGet();
          }
        }
      } else if (rc < 0) {
        throw new IllegalStateException("too many decRef calls: refCount is " + rc + " after decrement");
      }
    }
    
    public void incRef() {
      refCount.incrementAndGet();
    }
    
  }
  
  private static class ReplicationSession {
    public final SessionToken session;
    public final RefCountedRevision revision;
    private volatile long lastAccessTime;
    
    ReplicationSession(SessionToken session, RefCountedRevision revision) {
      this.session = session;
      this.revision = revision;
      lastAccessTime = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    }
    
    boolean isExpired(long expirationThreshold) {
      return lastAccessTime < (TimeUnit.MILLISECONDS.convert(System.nanoTime(), 
          TimeUnit.NANOSECONDS) - expirationThreshold);
    }
    
    void markAccessed() {
      lastAccessTime = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    }
  }
  
  /** Threshold for expiring inactive sessions. Defaults to 30 minutes. */
  public static final long DEFAULT_SESSION_EXPIRATION_THRESHOLD = 1000 * 60 * 30;
  
  private long expirationThresholdMilllis = LocalReplicator.DEFAULT_SESSION_EXPIRATION_THRESHOLD;
  
  private volatile RefCountedRevision currentRevision;
  private volatile boolean closed = false;
  
  private final AtomicInteger sessionToken = new AtomicInteger(0);
  private final Map<String, ReplicationSession> sessions = new HashMap<>();
  
  private void checkExpiredSessions() throws IOException {
    // make a "to-delete" list so we don't risk deleting from the map while iterating it
    final ArrayList<ReplicationSession> toExpire = new ArrayList<>();
    for (ReplicationSession token : sessions.values()) {
      if (token.isExpired(expirationThresholdMilllis)) {
        toExpire.add(token);
      }
    }
    for (ReplicationSession token : toExpire) {
      releaseSession(token.session.id);
    }
  }
  
  private void releaseSession(String sessionID) throws IOException {
    ReplicationSession session = sessions.remove(sessionID);
    // if we're called concurrently by close() and release(), could be that one
    // thread beats the other to release the session.
    if (session != null) {
      session.revision.decRef();
    }
  }
  
  /** Ensure that replicator is still open, or throw {@link AlreadyClosedException} otherwise. */
  protected final synchronized void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException("This replicator has already been closed");
    }
  }
  
  @Override
  public synchronized SessionToken checkForUpdate(String currentVersion) {
    ensureOpen();
    if (currentRevision == null) { // no published revisions yet
      return null;
    }
    
    if (currentVersion != null && currentRevision.revision.compareTo(currentVersion) <= 0) {
      // currentVersion is newer or equal to latest published revision
      return null;
    }
    
    // currentVersion is either null or older than latest published revision
    currentRevision.incRef();
    final String sessionID = Integer.toString(sessionToken.incrementAndGet());
    final SessionToken sessionToken = new SessionToken(sessionID, currentRevision.revision);
    final ReplicationSession timedSessionToken = new ReplicationSession(sessionToken, currentRevision);
    sessions.put(sessionID, timedSessionToken);
    return sessionToken;
  }
  
  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      // release all managed revisions
      for (ReplicationSession session : sessions.values()) {
        session.revision.decRef();
      }
      sessions.clear();
      closed = true;
    }
  }
  
  /**
   * Returns the expiration threshold.
   * 
   * @see #setExpirationThreshold(long)
   */
  public long getExpirationThreshold() {
    return expirationThresholdMilllis;
  }
  
  @Override
  public synchronized InputStream obtainFile(String sessionID, String source, String fileName) throws IOException {
    ensureOpen();
    ReplicationSession session = sessions.get(sessionID);
    if (session != null && session.isExpired(expirationThresholdMilllis)) {
      releaseSession(sessionID);
      session = null;
    }
    // session either previously expired, or we just expired it
    if (session == null) {
      throw new SessionExpiredException("session (" + sessionID + ") expired while obtaining file: source=" + source
          + " file=" + fileName);
    }
    sessions.get(sessionID).markAccessed();
    return session.revision.revision.open(source, fileName);
  }
  
  @Override
  public synchronized void publish(Revision revision) throws IOException {
    ensureOpen();
    if (currentRevision != null) {
      int compare = revision.compareTo(currentRevision.revision);
      if (compare == 0) {
        // same revision published again, ignore but release it
        revision.release();
        return;
      }
      
      if (compare < 0) {
        revision.release();
        throw new IllegalArgumentException("Cannot publish an older revision: rev=" + revision + " current="
            + currentRevision);
      } 
    }
    
    // swap revisions
    final RefCountedRevision oldRevision = currentRevision;
    currentRevision = new RefCountedRevision(revision);
    if (oldRevision != null) {
      oldRevision.decRef();
    }
    
    // check for expired sessions
    checkExpiredSessions();
  }
  
  @Override
  public synchronized void release(String sessionID) throws IOException {
    ensureOpen();
    releaseSession(sessionID);
  }
  
  /**
   * Modify session expiration time - if a replication session is inactive that
   * long it is automatically expired, and further attempts to operate within
   * this session will throw a {@link SessionExpiredException}.
   */
  public synchronized void setExpirationThreshold(long expirationThreshold) throws IOException {
    ensureOpen();
    this.expirationThresholdMilllis = expirationThreshold;
    checkExpiredSessions();
  }
  
}

package org.apache.lucene.index;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.DocumentsWriterPerThread.FlushedSegment;


/**
 * @lucene.internal 
 */
class DocumentsWriterFlushQueue {
  private final Queue<FlushTicket> queue = new LinkedList<FlushTicket>();
  // we track tickets separately since count must be present even before the ticket is
  // constructed ie. queue.size would not reflect it.
  private final AtomicInteger ticketCount = new AtomicInteger();
  private final ReentrantLock purgeLock = new ReentrantLock();

  void addDeletesAndPurge(DocumentsWriter writer,
      DocumentsWriterDeleteQueue deleteQueue) throws IOException {
    synchronized (this) {
      incTickets();// first inc the ticket count - freeze opens
                   // a window for #anyChanges to fail
      boolean success = false;
      try {
        queue.add(new GlobalDeletesTicket(deleteQueue.freezeGlobalBuffer(null)));
        success = true;
      } finally {
        if (!success) {
          decTickets();
        }
      }
    }
    // don't hold the lock on the FlushQueue when forcing the purge - this blocks and deadlocks 
    // if we hold the lock.
    forcePurge(writer);
  }
  
  private void incTickets() {
    int numTickets = ticketCount.incrementAndGet();
    assert numTickets > 0;
  }
  
  private void decTickets() {
    int numTickets = ticketCount.decrementAndGet();
    assert numTickets >= 0;
  }

  synchronized SegmentFlushTicket addFlushTicket(DocumentsWriterPerThread dwpt) {
    // Each flush is assigned a ticket in the order they acquire the ticketQueue
    // lock
    incTickets();
    boolean success = false;
    try {
      // prepare flush freezes the global deletes - do in synced block!
      final SegmentFlushTicket ticket = new SegmentFlushTicket(dwpt.prepareFlush());
      queue.add(ticket);
      success = true;
      return ticket;
    } finally {
      if (!success) {
        decTickets();
      }
    }
  }
  
  synchronized void addSegment(SegmentFlushTicket ticket, FlushedSegment segment) {
    // the actual flush is done asynchronously and once done the FlushedSegment
    // is passed to the flush ticket
    ticket.setSegment(segment);
  }

  synchronized void markTicketFailed(SegmentFlushTicket ticket) {
    // to free the queue we mark tickets as failed just to clean up the queue.
    ticket.setFailed();
  }

  boolean hasTickets() {
    assert ticketCount.get() >= 0 : "ticketCount should be >= 0 but was: " + ticketCount.get();
    return ticketCount.get() != 0;
  }

  private void innerPurge(DocumentsWriter writer) throws IOException {
    assert purgeLock.isHeldByCurrentThread();
    while (true) {
      final FlushTicket head;
      final boolean canPublish;
      synchronized (this) {
        head = queue.peek();
        canPublish = head != null && head.canPublish(); // do this synced 
      }
      if (canPublish) {
        try {
          /*
           * if we block on publish -> lock IW -> lock BufferedDeletes we don't block
           * concurrent segment flushes just because they want to append to the queue.
           * the downside is that we need to force a purge on fullFlush since ther could
           * be a ticket still in the queue. 
           */
          head.publish(writer);
        } finally {
          synchronized (this) {
            // finally remove the published ticket from the queue
            final FlushTicket poll = queue.poll();
            ticketCount.decrementAndGet();
            assert poll == head;
          }
        }
      } else {
        break;
      }
    }
  }

  void forcePurge(DocumentsWriter writer) throws IOException {
    assert !Thread.holdsLock(this);
    purgeLock.lock();
    try {
      innerPurge(writer);
    } finally {
      purgeLock.unlock();
    }
  }

  void tryPurge(DocumentsWriter writer) throws IOException {
    assert !Thread.holdsLock(this);
    if (purgeLock.tryLock()) {
      try {
        innerPurge(writer);
      } finally {
        purgeLock.unlock();
      }
    }
  }

  public int getTicketCount() {
    return ticketCount.get();
  }

  synchronized void clear() {
    queue.clear();
    ticketCount.set(0);
  }

  static abstract class FlushTicket {
    protected FrozenBufferedDeletes frozenDeletes;
    protected boolean published = false;

    protected FlushTicket(FrozenBufferedDeletes frozenDeletes) {
      assert frozenDeletes != null;
      this.frozenDeletes = frozenDeletes;
    }

    protected abstract void publish(DocumentsWriter writer) throws IOException;
    protected abstract boolean canPublish();
  }
  
  static final class GlobalDeletesTicket extends FlushTicket {

    protected GlobalDeletesTicket(FrozenBufferedDeletes frozenDeletes) {
      super(frozenDeletes);
    }
    @Override
    protected void publish(DocumentsWriter writer) throws IOException {
      assert !published : "ticket was already publised - can not publish twice";
      published = true;
      // its a global ticket - no segment to publish
      writer.finishFlush(null, frozenDeletes);
    }

    @Override
    protected boolean canPublish() {
      return true;
    }
  }

  static final class SegmentFlushTicket extends FlushTicket {
    private FlushedSegment segment;
    private boolean failed = false;
    
    protected SegmentFlushTicket(FrozenBufferedDeletes frozenDeletes) {
      super(frozenDeletes);
    }
    
    @Override
    protected void publish(DocumentsWriter writer) throws IOException {
      assert !published : "ticket was already publised - can not publish twice";
      published = true;
      writer.finishFlush(segment, frozenDeletes);
    }
    
    protected void setSegment(FlushedSegment segment) {
      assert !failed;
      this.segment = segment;
    }
    
    protected void setFailed() {
      assert segment == null;
      failed = true;
    }

    @Override
    protected boolean canPublish() {
      return segment != null || failed;
    }
  }
}
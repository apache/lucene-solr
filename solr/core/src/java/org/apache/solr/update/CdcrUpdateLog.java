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
package org.apache.solr.update;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of the {@link org.apache.solr.update.UpdateLog} for the CDCR scenario.<br>
 * Compared to the original update log implementation, transaction logs are removed based on
 * pointers instead of a fixed size limit. Pointers are created by the CDC replicators and
 * correspond to replication checkpoints. If all pointers are ahead of a transaction log,
 * this transaction log is removed.<br>
 * Given that the number of transaction logs can become considerable if some pointers are
 * lagging behind, the {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader} provides
 * a {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader#seek(long)} method to
 * efficiently lookup a particular transaction log file given a version number.
 */
public class CdcrUpdateLog extends UpdateLog {

  protected final Map<CdcrLogReader, CdcrLogPointer> logPointers = new ConcurrentHashMap<>();

  /**
   * A reader that will be used as toggle to turn on/off the buffering of tlogs
   */
  private CdcrLogReader bufferToggle;

  public static String LOG_FILENAME_PATTERN = "%s.%019d.%1d";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean debug = log.isDebugEnabled();

  @Override
  public void init(UpdateHandler uhandler, SolrCore core) {
    // remove dangling readers
    for (CdcrLogReader reader : logPointers.keySet()) {
      reader.close();
    }
    logPointers.clear();

    // init
    super.init(uhandler, core);
  }

  @Override
  public TransactionLog newTransactionLog(File tlogFile, Collection<String> globalStrings, boolean openExisting) {
    return new CdcrTransactionLog(tlogFile, globalStrings, openExisting);
  }

  @Override
  protected void addOldLog(TransactionLog oldLog, boolean removeOld) {
    if (oldLog == null) return;

    numOldRecords += oldLog.numRecords();

    int currRecords = numOldRecords;

    if (oldLog != tlog && tlog != null) {
      currRecords += tlog.numRecords();
    }

    while (removeOld && logs.size() > 0) {
      TransactionLog log = logs.peekLast();
      int nrec = log.numRecords();

      // remove oldest log if we don't need it to keep at least numRecordsToKeep, or if
      // we already have the limit of 10 log files.
      if (currRecords - nrec >= numRecordsToKeep || logs.size() >= 10) {
        // remove the oldest log if nobody points to it
        if (!this.hasLogPointer(log)) {
          currRecords -= nrec;
          numOldRecords -= nrec;
          TransactionLog last = logs.removeLast();
          last.deleteOnClose = true;
          last.close();  // it will be deleted if no longer in use
          continue;
        }
        // we have one log with one pointer, we should stop removing logs
        break;
      }

      break;
    }

    // Decref old log as we do not write to it anymore
    // If the oldlog is uncapped, i.e., a write commit has to be performed
    // during recovery, the output stream will be automatically re-open when
    // TransaactionLog#incref will be called.
    oldLog.deleteOnClose = false;
    oldLog.decref();

    // don't incref... we are taking ownership from the caller.
    logs.addFirst(oldLog);
  }

  /**
   * Checks if one of the log pointer is pointing to the given tlog.
   */
  private boolean hasLogPointer(TransactionLog tlog) {
    for (CdcrLogPointer pointer : logPointers.values()) {
      // if we have a pointer that is not initialised, then do not remove the old tlogs
      // as we have a log reader that didn't pick them up yet.
      if (!pointer.isInitialised()) {
        return true;
      }

      if (pointer.tlogFile == tlog.tlogFile) {
        return true;
      }
    }
    return false;
  }

  @Override
  public long getLastLogId() {
    if (id != -1) return id;
    if (tlogFiles.length == 0) return -1;
    String last = tlogFiles[tlogFiles.length - 1];
    if (TLOG_NAME.length() + 1 > last.lastIndexOf('.'))  {
      // old tlog created by default UpdateLog impl
      return Long.parseLong(last.substring(TLOG_NAME.length() + 1));
    } else  {
      return Long.parseLong(last.substring(TLOG_NAME.length() + 1, last.lastIndexOf('.')));
    }
  }

  @Override
  public void add(AddUpdateCommand cmd, boolean clearCaches) {
    // Ensure we create a new tlog file following our filename format,
    // the variable tlog will be not null, and the ensureLog of the parent will be skipped
    synchronized (this) {
      if ((cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
        ensureLog(cmd.getVersion());
      }
    }
    // Then delegate to parent method
    super.add(cmd, clearCaches);
  }

  @Override
  public void delete(DeleteUpdateCommand cmd) {
    // Ensure we create a new tlog file following our filename format
    // the variable tlog will be not null, and the ensureLog of the parent will be skipped
    synchronized (this) {
      if ((cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
        ensureLog(cmd.getVersion());
      }
    }
    // Then delegate to parent method
    super.delete(cmd);
  }

  @Override
  public void deleteByQuery(DeleteUpdateCommand cmd) {
    // Ensure we create a new tlog file following our filename format
    // the variable tlog will be not null, and the ensureLog of the parent will be skipped
    synchronized (this) {
      if ((cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
        ensureLog(cmd.getVersion());
      }
    }
    // Then delegate to parent method
    super.deleteByQuery(cmd);
  }

  /**
   * Creates a new {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader}
   * initialised with the current list of tlogs.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public CdcrLogReader newLogReader() {
    return new CdcrLogReader(new ArrayList(logs), tlog);
  }

  /**
   * Enable the buffering of the tlogs. When buffering is activated, the update logs will not remove any
   * old transaction log files.
   */
  public void enableBuffer() {
    if (bufferToggle == null) {
      bufferToggle = this.newLogReader();
    }
  }

  /**
   * Disable the buffering of the tlogs.
   */
  public void disableBuffer() {
    if (bufferToggle != null) {
      bufferToggle.close();
      bufferToggle = null;
    }
  }

  public CdcrLogReader getBufferToggle() {
    return bufferToggle;
  }

  /**
   * Is the update log buffering the tlogs ?
   */
  public boolean isBuffering() {
    return bufferToggle == null ? false : true;
  }

  protected void ensureLog(long startVersion) {
    if (tlog == null) {
      long absoluteVersion = Math.abs(startVersion); // version is negative for deletes
      if (tlog == null) {
        String newLogName = String.format(Locale.ROOT, LOG_FILENAME_PATTERN, TLOG_NAME, id, absoluteVersion);
        tlog = new CdcrTransactionLog(new File(tlogDir, newLogName), globalStrings);
      }

      // push the new tlog to the opened readers
      for (CdcrLogReader reader : logPointers.keySet()) {
        reader.push(tlog);
      }
    }
  }

  /**
   * expert: Reset the update log before initialisation. This is called by
   * {@link org.apache.solr.handler.IndexFetcher#moveTlogFiles(File)} during a
   * a Recovery operation in order to re-initialise the UpdateLog with a new set of tlog files.
   * @see #initForRecovery(File, long)
   */
  public BufferedUpdates resetForRecovery() {
    synchronized (this) { // since we blocked updates in IndexFetcher, this synchronization shouldn't strictly be necessary.
      // If we are buffering, we need to return the related information to the index fetcher
      // for properly initialising the new update log - SOLR-8263
      BufferedUpdates bufferedUpdates = new BufferedUpdates();
      if (state == State.BUFFERING && tlog != null) {
        bufferedUpdates.tlog = tlog.tlogFile; // file to keep
        bufferedUpdates.offset = this.recoveryInfo.positionOfStart;
      }

      // Close readers
      for (CdcrLogReader reader : logPointers.keySet()) {
        reader.close();
      }
      logPointers.clear();

      // Close and clear logs
      doClose(prevTlog);
      doClose(tlog);

      for (TransactionLog log : logs) {
        if (log == prevTlog || log == tlog) continue;
        doClose(log);
      }

      logs.clear();
      newestLogsOnStartup.clear();
      tlog = prevTlog = null;
      prevMapLog = prevMapLog2 = null;

      map.clear();
      if (prevMap != null) prevMap.clear();
      if (prevMap2 != null) prevMap2.clear();

      tlogFiles = null;
      numOldRecords = 0;

      oldDeletes.clear();
      deleteByQueries.clear();

      return bufferedUpdates;
    }
  }

  public static class BufferedUpdates {
    public File tlog;
    public long offset;
  }

  /**
   * <p>
   *   expert: Initialise the update log with a tlog file containing buffered updates. This is called by
   *   {@link org.apache.solr.handler.IndexFetcher#moveTlogFiles(File)} during a Recovery operation.
   *   This is mainly a copy of the original {@link UpdateLog#init(UpdateHandler, SolrCore)} method, but modified
   *   to:
   *   <ul>
   *     <li>preserve the same {@link VersionInfo} instance in order to not "unblock" updates, since the
   *     {@link org.apache.solr.handler.IndexFetcher#moveTlogFiles(File)} acquired a write lock from this instance.</li>
   *     <li>copy the buffered updates.</li>
   *   </ul>
   * @see #resetForRecovery()
   */
  public void initForRecovery(File bufferedTlog, long offset) {
    tlogFiles = getLogList(tlogDir);
    id = getLastLogId() + 1;   // add 1 since we will create a new log for the next update

    if (debug) {
      log.debug("UpdateHandler init: tlogDir={}, existing tlogs={}, next id={}", tlogDir, Arrays.asList(tlogFiles), id);
    }

    TransactionLog oldLog = null;
    for (String oldLogName : tlogFiles) {
      File f = new File(tlogDir, oldLogName);
      try {
        oldLog = newTransactionLog(f, null, true);
        addOldLog(oldLog, false);  // don't remove old logs on startup since more than one may be uncapped.
      } catch (Exception e) {
        SolrException.log(log, "Failure to open existing log file (non fatal) " + f, e);
        deleteFile(f);
      }
    }

    // Record first two logs (oldest first) at startup for potential tlog recovery.
    // It's possible that at abnormal close both "tlog" and "prevTlog" were uncapped.
    for (TransactionLog ll : logs) {
      newestLogsOnStartup.addFirst(ll);
      if (newestLogsOnStartup.size() >= 2) break;
    }

    // TODO: these startingVersions assume that we successfully recover from all non-complete tlogs.
    UpdateLog.RecentUpdates startingUpdates = getRecentUpdates();
    long latestVersion = startingUpdates.getMaxRecentVersion();
    try {
      startingVersions = startingUpdates.getVersions(numRecordsToKeep);

      // populate recent deletes list (since we can't get that info from the index)
      for (int i=startingUpdates.deleteList.size()-1; i>=0; i--) {
        DeleteUpdate du = startingUpdates.deleteList.get(i);
        oldDeletes.put(new BytesRef(du.id), new LogPtr(-1,du.version));
      }

      // populate recent deleteByQuery commands
      for (int i=startingUpdates.deleteByQueryList.size()-1; i>=0; i--) {
        Update update = startingUpdates.deleteByQueryList.get(i);
        @SuppressWarnings({"unchecked"})
        List<Object> dbq = (List<Object>) update.log.lookup(update.pointer);
        long version = (Long) dbq.get(1);
        String q = (String) dbq.get(2);
        trackDeleteByQuery(q, version);
      }

    } finally {
      startingUpdates.close();
    }

    // Copy buffered updates
    if (bufferedTlog != null) {
      this.copyBufferedUpdates(bufferedTlog, offset, latestVersion);
    }
  }

  /**
   * <p>
   *   Read the entries from the given tlog file and replay them as buffered updates.
   *   The buffered tlog that we are trying to copy might contain duplicate operations with the
   *   current update log. During the tlog replication process, the replica might buffer update operations
   *   that will be present also in the tlog files downloaded from the leader. In order to remove these
   *   duplicates, it will skip any operations with a version inferior to the latest know version.
   */
  private void copyBufferedUpdates(File tlogSrc, long offsetSrc, long latestVersion) {
    recoveryInfo = new RecoveryInfo();
    state = State.BUFFERING;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM, DistributedUpdateProcessor.DistribPhase.FROMLEADER.toString());
    SolrQueryRequest req = new LocalSolrQueryRequest(uhandler.core, params);

    CdcrTransactionLog src = new CdcrTransactionLog(tlogSrc, null, true);
    TransactionLog.LogReader tlogReader = src.getReader(offsetSrc);
    try {
      int operationAndFlags = 0;
      for (; ; ) {
        Object o = tlogReader.next();
        if (o == null) break; // we reached the end of the tlog
        // should currently be a List<Oper,Ver,Doc/Id>
        @SuppressWarnings({"rawtypes"})
        List entry = (List) o;
        operationAndFlags = (Integer) entry.get(0);
        int oper = operationAndFlags & OPERATION_MASK;
        long version = (Long) entry.get(1);
        if (version <= latestVersion) {
          // probably a buffered update that is also present in a tlog file coming from the leader,
          // skip it.
          log.debug("Dropping buffered operation - version {} < {}", version, latestVersion);
          continue;
        }

        switch (oper) {
          case UpdateLog.ADD: {
            SolrInputDocument sdoc = (SolrInputDocument) entry.get(entry.size() - 1);
            AddUpdateCommand cmd = new AddUpdateCommand(req);
            cmd.solrDoc = sdoc;
            cmd.setVersion(version);
            cmd.setFlags(UpdateCommand.BUFFERING);
            this.add(cmd);
            break;
          }
          case UpdateLog.DELETE: {
            byte[] idBytes = (byte[]) entry.get(2);
            DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
            cmd.setIndexedId(new BytesRef(idBytes));
            cmd.setVersion(version);
            cmd.setFlags(UpdateCommand.BUFFERING);
            this.delete(cmd);
            break;
          }

          case UpdateLog.DELETE_BY_QUERY: {
            String query = (String) entry.get(2);
            DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
            cmd.query = query;
            cmd.setVersion(version);
            cmd.setFlags(UpdateCommand.BUFFERING);
            this.deleteByQuery(cmd);
            break;
          }

          default:
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid Operation! " + oper);
        }

      }
    }
    catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to copy buffered updates", e);
    }
    finally {
      try {
        tlogReader.close();
      }
      finally {
        this.doClose(src);
      }
    }
  }

  private void doClose(TransactionLog theLog) {
    if (theLog != null) {
      theLog.deleteOnClose = false;
      theLog.decref();
      theLog.forceClose();
    }
  }

  @Override
  public void close(boolean committed, boolean deleteOnClose) {
    for (CdcrLogReader reader : new ArrayList<>(logPointers.keySet())) {
      reader.close();
    }
    super.close(committed, deleteOnClose);
  }

  private static class CdcrLogPointer {

    File tlogFile = null;

    private CdcrLogPointer() {
    }

    private void set(File tlogFile) {
      this.tlogFile = tlogFile;
    }

    private boolean isInitialised() {
      return tlogFile == null ? false : true;
    }

    @Override
    public String toString() {
      return "CdcrLogPointer(" + tlogFile + ")";
    }

  }

  public class CdcrLogReader {

    private TransactionLog currentTlog;
    private TransactionLog.LogReader tlogReader;

    // we need to use a blocking deque because of #getNumberOfRemainingRecords
    private final LinkedBlockingDeque<TransactionLog> tlogs;
    private final CdcrLogPointer pointer;

    /**
     * Used to record the last position of the tlog
     */
    private long lastPositionInTLog = 0;

    /**
     * lastVersion is used to get nextToLastVersion
     */
    private long lastVersion = -1;

    /**
     * nextToLastVersion is communicated by leader to replicas so that they can remove no longer needed tlogs
     * <p>
     * nextToLastVersion is used because thanks to {@link #resetToLastPosition()} lastVersion can become the current version
     */
    private long nextToLastVersion = -1;

    /**
     * Used to record the number of records read in the current tlog
     */
    private long numRecordsReadInCurrentTlog = 0;

    private CdcrLogReader(List<TransactionLog> tlogs, TransactionLog tlog) {
      this.tlogs = new LinkedBlockingDeque<>();
      this.tlogs.addAll(tlogs);
      if (tlog != null) this.tlogs.push(tlog); // ensure that the tlog being written is pushed

      // Register the pointer in the parent UpdateLog
      pointer = new CdcrLogPointer();
      logPointers.put(this, pointer);

      // If the reader is initialised while the updates log is empty, do nothing
      if ((currentTlog = this.tlogs.peekLast()) != null) {
        tlogReader = currentTlog.getReader(0);
        pointer.set(currentTlog.tlogFile);
        numRecordsReadInCurrentTlog = 0;
        log.debug("Init new tlog reader for {} - tlogReader = {}", currentTlog.tlogFile, tlogReader);
      }
    }

    private void push(TransactionLog tlog) {
      this.tlogs.push(tlog);

      // The reader was initialised while the update logs was empty, or reader was exhausted previously,
      // we have to update the current tlog and the associated tlog reader.
      if (currentTlog == null && !tlogs.isEmpty()) {
        currentTlog = tlogs.peekLast();
        tlogReader = currentTlog.getReader(0);
        pointer.set(currentTlog.tlogFile);
        numRecordsReadInCurrentTlog = 0;
        log.debug("Init new tlog reader for {} - tlogReader = {}", currentTlog.tlogFile, tlogReader);
      }
    }

    /**
     * Expert: Instantiate a sub-reader. A sub-reader is used for batch updates. It allows to iterates over the
     * update logs entries without modifying the state of the parent log reader. If the batch update fails, the state
     * of the sub-reader is discarded and the state of the parent reader is not modified. If the batch update
     * is successful, the sub-reader is used to fast forward the parent reader with the method
     * {@link #forwardSeek(org.apache.solr.update.CdcrUpdateLog.CdcrLogReader)}.
     */
    public CdcrLogReader getSubReader() {
      // Add the last element of the queue to properly initialise the pointer and log reader
      CdcrLogReader clone = new CdcrLogReader(new ArrayList<TransactionLog>(), this.tlogs.peekLast());
      clone.tlogs.clear(); // clear queue before copy
      clone.tlogs.addAll(tlogs); // perform a copy of the list
      clone.lastPositionInTLog = this.lastPositionInTLog;
      clone.numRecordsReadInCurrentTlog = this.numRecordsReadInCurrentTlog;
      clone.lastVersion = this.lastVersion;
      clone.nextToLastVersion = this.nextToLastVersion;

      // If the update log is not empty, we need to initialise the tlog reader
      // NB: the tlogReader is equal to null if the update log is empty
      if (tlogReader != null) {
        clone.tlogReader.close();
        clone.tlogReader = currentTlog.getReader(this.tlogReader.currentPos());
      }

      return clone;
    }

    /**
     * Expert: Fast forward this log reader with a log subreader. The subreader will be closed after calling this
     * method. In order to avoid unexpected results, the log
     * subreader must be created from this reader with the method {@link #getSubReader()}.
     */
    public void forwardSeek(CdcrLogReader subReader) {
      // If a subreader has a null tlog reader, does nothing
      // This can happened if a subreader is instantiated from a non-initialised parent reader, or if the subreader
      // has been closed.
      if (subReader.tlogReader == null) {
        return;
      }

      tlogReader.close(); // close the existing reader, a new one will be created
      while (this.tlogs.peekLast().id < subReader.tlogs.peekLast().id) {
        tlogs.removeLast();
        currentTlog = tlogs.peekLast();
      }
      assert this.tlogs.peekLast().id == subReader.tlogs.peekLast().id : this.tlogs.peekLast().id+" != "+subReader.tlogs.peekLast().id;
      this.pointer.set(currentTlog.tlogFile);
      this.lastPositionInTLog = subReader.lastPositionInTLog;
      this.numRecordsReadInCurrentTlog = subReader.numRecordsReadInCurrentTlog;
      this.lastVersion = subReader.lastVersion;
      this.nextToLastVersion = subReader.nextToLastVersion;
      this.tlogReader = currentTlog.getReader(subReader.tlogReader.currentPos());
    }

    /**
     * Advances to the next log entry in the updates log and returns the log entry itself.
     * Returns null if there are no more log entries in the updates log.<br>
     * <p>
     * <b>NOTE:</b> after the reader has exhausted, you can call again this method since the updates
     * log might have been updated with new entries.
     */
    public Object next() throws IOException, InterruptedException {
      while (!tlogs.isEmpty()) {
        lastPositionInTLog = tlogReader.currentPos();
        Object o = tlogReader.next();

        if (o != null) {
          pointer.set(currentTlog.tlogFile);
          nextToLastVersion = lastVersion;
          lastVersion = getVersion(o);
          numRecordsReadInCurrentTlog++;
          return o;
        }

        if (tlogs.size() > 1) { // if the current tlog is not the newest one, we can advance to the next one
          tlogReader.close();
          tlogs.removeLast();
          currentTlog = tlogs.peekLast();
          tlogReader = currentTlog.getReader(0);
          pointer.set(currentTlog.tlogFile);
          numRecordsReadInCurrentTlog = 0;
          log.debug("Init new tlog reader for {} - tlogReader = {}", currentTlog.tlogFile, tlogReader);
        } else {
          // the only tlog left is the new tlog which is currently being written,
          // we should not remove it as we have to try to read it again later.
          return null;
        }
      }

      return null;
    }

    /**
     * Advances to the first beyond the current whose version number is greater
     * than or equal to <i>targetVersion</i>.<br>
     * Returns true if the reader has been advanced. If <i>targetVersion</i> is
     * greater than the highest version number in the updates log, the reader
     * has been advanced to the end of the current tlog, and a call to
     * {@link #next()} will probably return null.<br>
     * Returns false if <i>targetVersion</i> is lower than the oldest known entry.
     * In this scenario, it probably means that there is a gap in the updates log.<br>
     * <p>
     * <b>NOTE:</b> This method must be called before the first call to {@link #next()}.
     */
    public boolean seek(long targetVersion) throws IOException, InterruptedException {
      Object o;
      // version is negative for deletes - ensure that we are manipulating absolute version numbers.
      targetVersion = Math.abs(targetVersion);

      if (tlogs.isEmpty() || !this.seekTLog(targetVersion)) {
        return false;
      }

      // now that we might be on the right tlog, iterates over the entries to find the one we are looking for
      while ((o = this.next()) != null) {
        if (this.getVersion(o) >= targetVersion) {
          this.resetToLastPosition();
          return true;
        }
      }

      return true;
    }

    /**
     * Seeks the tlog associated to the target version by using the updates log index,
     * and initialises the log reader to the start of the tlog. Returns true if it was able
     * to seek the corresponding tlog, false if the <i>targetVersion</i> is lower than the
     * oldest known entry (which probably indicates a gap).<br>
     * <p>
     * <b>NOTE:</b> This method might modify the tlog queue by removing tlogs that are older
     * than the target version.
     */
    private boolean seekTLog(long targetVersion) {
      // if the target version is lower than the oldest known entry, we have probably a gap.
      if (targetVersion < ((CdcrTransactionLog) tlogs.peekLast()).startVersion) {
        return false;
      }

      // closes existing reader before performing seek and possibly modifying the queue;
      tlogReader.close();

      // iterates over the queue and removes old tlogs
      TransactionLog last = null;
      while (tlogs.size() > 1) {
        if (((CdcrTransactionLog) tlogs.peekLast()).startVersion >= targetVersion) {
          break;
        }
        last = tlogs.pollLast();
      }

      // the last tlog removed is the one we look for, add it back to the queue
      if (last != null) tlogs.addLast(last);

      currentTlog = tlogs.peekLast();
      tlogReader = currentTlog.getReader(0);
      pointer.set(currentTlog.tlogFile);
      numRecordsReadInCurrentTlog = 0;

      return true;
    }

    /**
     * Extracts the version number and converts it to its absolute form.
     */
    private long getVersion(Object o) {
      @SuppressWarnings({"rawtypes"})
      List entry = (List) o;
      // version is negative for delete, ensure that we are manipulating absolute version numbers
      return Math.abs((Long) entry.get(1));
    }

    /**
     * If called after {@link #next()}, it resets the reader to its last position.
     */
    public void resetToLastPosition() {
      try {
        if (tlogReader != null) {
          tlogReader.fis.seek(lastPositionInTLog);
          numRecordsReadInCurrentTlog--;
          lastVersion = nextToLastVersion;
        }
      } catch (IOException e) {
        log.error("Failed to seek last position in tlog", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to seek last position in tlog", e);
      }
    }

    /**
     * Returns the number of remaining records (including commit but excluding header) to be read in the logs.
     */
    public long getNumberOfRemainingRecords() {
      long numRemainingRecords = 0;

      synchronized (tlogs) {
        for (TransactionLog tlog : tlogs) {
          numRemainingRecords += tlog.numRecords() - 1; // minus 1 as the number of records returned by the tlog includes the header
        }
      }

      return numRemainingRecords - numRecordsReadInCurrentTlog;
    }

    /**
     * Closes streams and remove the associated {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogPointer} from the
     * parent {@link org.apache.solr.update.CdcrUpdateLog}.
     */
    public void close() {
      if (tlogReader != null) {
        tlogReader.close();
        tlogReader = null;
        currentTlog = null;
      }
      tlogs.clear();
      logPointers.remove(this);
    }

    /**
     * Returns the absolute form of the version number of the last entry read. If the current version is equal
     * to 0 (because of a commit), it will return the next to last version number.
     */
    public long getLastVersion() {
      return lastVersion == 0 ? nextToLastVersion : lastVersion;
    }
  }

}


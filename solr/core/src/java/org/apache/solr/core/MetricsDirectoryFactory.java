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
package org.apache.solr.core;

import java.io.IOException;
import java.util.Collection;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * An implementation of {@link DirectoryFactory} that decorates provided factory by
 * adding metrics for directory IO operations.
 */
public class MetricsDirectoryFactory extends DirectoryFactory implements SolrCoreAware {
  private final SolrMetricManager metricManager;
  private final String registry;
  private final DirectoryFactory in;
  private boolean directoryDetails = false;

  public MetricsDirectoryFactory(SolrMetricManager metricManager, String registry, DirectoryFactory in) {
    this.metricManager = metricManager;
    this.registry = registry;
    this.in = in;
  }

  public DirectoryFactory getDelegate() {
    return in;
  }

  /**
   * Currently the following arguments are supported:
   * <ul>
   *   <li><code>directoryDetails</code> - (optional bool) when true then additional detailed metrics
   *   will be collected. These include eg. IO size histograms and per-file counters and histograms</li>
   * </ul>
   * @param args init args
   */
  @Override
  public void init(NamedList args) {
    // should be already inited
    // in.init(args);
    if (args == null) {
      return;
    }
    Boolean dd = args.getBooleanArg("directoryDetails");
    if (dd != null) {
      directoryDetails = dd;
    } else {
      directoryDetails = false;
    }
  }

  /**
   * Unwrap just one level if the argument is a {@link MetricsDirectory}
   * @param dir directory
   * @return delegate if the instance was a {@link MetricsDirectory}, otherwise unchanged.
   */
  private static Directory unwrap(Directory dir) {
    if (dir instanceof MetricsDirectory) {
      return ((MetricsDirectory)dir).getDelegate();
    } else {
      return dir;
    }
  }

  @Override
  public void doneWithDirectory(Directory dir) throws IOException {
    dir = unwrap(dir);
    in.doneWithDirectory(dir);
  }

  @Override
  public void addCloseListener(Directory dir, CachingDirectoryFactory.CloseListener closeListener) {
    dir = unwrap(dir);
    in.addCloseListener(dir, closeListener);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
    Directory dir = in.create(path, lockFactory, dirContext);
    return new MetricsDirectory(metricManager, registry, dir, directoryDetails);
  }

  @Override
  protected LockFactory createLockFactory(String rawLockType) throws IOException {
    return in.createLockFactory(rawLockType);
  }

  @Override
  public boolean exists(String path) throws IOException {
    return in.exists(path);
  }

  @Override
  public void remove(Directory dir) throws IOException {
    dir = unwrap(dir);
    in.remove(dir);
  }

  @Override
  public void remove(Directory dir, boolean afterCoreClose) throws IOException {
    dir = unwrap(dir);
    in.remove(dir, afterCoreClose);
  }

  @Override
  public boolean isSharedStorage() {
    return in.isSharedStorage();
  }

  @Override
  public boolean isAbsolute(String path) {
    return in.isAbsolute(path);
  }

  @Override
  public boolean searchersReserveCommitPoints() {
    return in.searchersReserveCommitPoints();
  }

  @Override
  public String getDataHome(CoreDescriptor cd) throws IOException {
    return in.getDataHome(cd);
  }

  @Override
  public long size(Directory dir) throws IOException {
    dir = unwrap(dir);
    return in.size(dir);
  }

  @Override
  public long size(String path) throws IOException {
    return in.size(path);
  }

  @Override
  public Collection<SolrInfoMBean> offerMBeans() {
    return in.offerMBeans();
  }

  @Override
  public void cleanupOldIndexDirectories(String dataDirPath, String currentIndexDirPath) {
    in.cleanupOldIndexDirectories(dataDirPath, currentIndexDirPath);
  }

  @Override
  public void remove(String path, boolean afterCoreClose) throws IOException {
    in.remove(path, afterCoreClose);
  }

  @Override
  public void remove(String path) throws IOException {
    in.remove(path);
  }

  @Override
  public void move(Directory fromDir, Directory toDir, String fileName, IOContext ioContext) throws IOException {
    fromDir = unwrap(fromDir);
    toDir = unwrap(toDir);
    in.move(fromDir, toDir, fileName, ioContext);
  }

  @Override
  public Directory get(String path, DirContext dirContext, String rawLockType) throws IOException {
    Directory dir = in.get(path, dirContext, rawLockType);
    if (dir instanceof MetricsDirectory) {
      return dir;
    } else {
      return new MetricsDirectory(metricManager, registry, dir, directoryDetails);
    }
  }

  @Override
  public void renameWithOverwrite(Directory dir, String fileName, String toName) throws IOException {
    dir = unwrap(dir);
    in.renameWithOverwrite(dir, fileName, toName);
  }

  @Override
  public String normalize(String path) throws IOException {
    return in.normalize(path);
  }

  @Override
  protected boolean deleteOldIndexDirectory(String oldDirPath) throws IOException {
    return in.deleteOldIndexDirectory(oldDirPath);
  }

  @Override
  public void initCoreContainer(CoreContainer cc) {
    in.initCoreContainer(cc);
  }

  @Override
  public void incRef(Directory dir) {
    dir = unwrap(dir);
    in.incRef(dir);
  }

  @Override
  public boolean isPersistent() {
    return in.isPersistent();
  }

  @Override
  public void inform(SolrCore core) {
    if (in instanceof  SolrCoreAware) {
      ((SolrCoreAware)in).inform(core);
    }
  }

  @Override
  public void release(Directory dir) throws IOException {
    dir = unwrap(dir);
    in.release(dir);
  }



  private static final String SEGMENTS = "segments";
  private static final String SEGMENTS_PREFIX = "segments_";
  private static final String PENDING_SEGMENTS_PREFIX = "pending_segments_";
  private static final String TEMP = "temp";
  private static final String OTHER = "other";

  public static class MetricsDirectory extends FilterDirectory {

    private final Directory in;
    private final String registry;
    private final SolrMetricManager metricManager;
    private final Meter totalReads;
    private final Histogram totalReadSizes;
    private final Meter totalWrites;
    private final Histogram totalWriteSizes;
    private final boolean directoryDetails;

    private final String PREFIX = SolrInfoMBean.Category.DIRECTORY.toString() + ".";

    public MetricsDirectory(SolrMetricManager metricManager, String registry, Directory in, boolean directoryDetails) throws IOException {
      super(in);
      this.metricManager = metricManager;
      this.registry = registry;
      this.in = in;
      this.directoryDetails = directoryDetails;
      this.totalReads = metricManager.meter(registry, "reads", SolrInfoMBean.Category.DIRECTORY.toString(), "total");
      this.totalWrites = metricManager.meter(registry, "writes", SolrInfoMBean.Category.DIRECTORY.toString(), "total");
      if (directoryDetails) {
        this.totalReadSizes = metricManager.histogram(registry, "readSizes", SolrInfoMBean.Category.DIRECTORY.toString(), "total");
        this.totalWriteSizes = metricManager.histogram(registry, "writeSizes", SolrInfoMBean.Category.DIRECTORY.toString(), "total");
      } else {
        this.totalReadSizes = null;
        this.totalWriteSizes = null;
      }
    }

    private String getMetricName(String name, boolean output) {
      if (!directoryDetails) {
        return null;
      }
      String lastName;
      if (name.startsWith(SEGMENTS_PREFIX) || name.startsWith(PENDING_SEGMENTS_PREFIX)) {
        lastName = SEGMENTS;
      } else {
        int pos = name.lastIndexOf('.');
        if (pos != -1 && name.length() > pos + 1) {
          lastName = name.substring(pos + 1);
        } else {
          lastName = OTHER;
        }
      }
      StringBuilder sb = new StringBuilder(PREFIX);
      sb.append(lastName);
      sb.append('.');
      if (output) {
        sb.append("write");
      } else {
        sb.append("read");
      }
      return sb.toString();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      IndexOutput output = in.createOutput(name, context);
      if (output != null) {
        return new MetricsOutput(totalWrites, totalWriteSizes, metricManager, registry, getMetricName(name, true), output);
      } else {
        return null;
      }
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
      IndexOutput output = in.createTempOutput(prefix, suffix, context);
      if (output != null) {
        return new MetricsOutput(totalWrites, totalWriteSizes, metricManager, registry, getMetricName(TEMP, true), output);
      } else {
        return null;
      }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      IndexInput input = in.openInput(name, context);
      if (input != null) {
        return new MetricsInput(totalReads, totalReadSizes, metricManager, registry, getMetricName(name, false), input);
      } else {
        return null;
      }
    }
  }

  public static class MetricsOutput extends IndexOutput {
    private final IndexOutput in;
    private final Histogram histogram;
    private final Meter meter;
    private final Meter totalMeter;
    private final Histogram totalHistogram;
    private final boolean withDetails;

    public MetricsOutput(Meter totalMeter, Histogram totalHistogram, SolrMetricManager metricManager,
                         String registry, String metricName, IndexOutput in) {
      super(in.toString(), in.getName());
      this.in = in;
      this.totalMeter = totalMeter;
      this.totalHistogram = totalHistogram;
      if (metricName != null && totalHistogram != null) {
        withDetails = true;
        String histName = metricName + "Sizes";
        String meterName = metricName + "s";
        this.histogram = metricManager.histogram(registry, histName);
        this.meter = metricManager.meter(registry, meterName);
      } else {
        withDetails = false;
        this.histogram = null;
        this.meter = null;
      }
    }

    @Override
    public void writeByte(byte b) throws IOException {
      in.writeByte(b);
      totalMeter.mark();
      if (withDetails) {
        totalHistogram.update(1);
        meter.mark();
        histogram.update(1);
      }
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      in.writeBytes(b, offset, length);
      totalMeter.mark(length);
      if (withDetails) {
        totalHistogram.update(length);
        meter.mark(length);
        histogram.update(length);
      }
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public long getFilePointer() {
      return in.getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException {
      return in.getChecksum();
    }
  }

  public static class MetricsInput extends IndexInput {
    private final IndexInput in;
    private final Meter totalMeter;
    private final Histogram totalHistogram;
    private final Histogram histogram;
    private final Meter meter;
    private final boolean withDetails;

    public MetricsInput(Meter totalMeter, Histogram totalHistogram, SolrMetricManager metricManager, String registry, String metricName, IndexInput in) {
      super(in.toString());
      this.in = in;
      this.totalMeter = totalMeter;
      this.totalHistogram = totalHistogram;
      if (metricName != null && totalHistogram != null) {
        withDetails = true;
        String histName = metricName + "Sizes";
        String meterName = metricName + "s";
        this.histogram = metricManager.histogram(registry, histName);
        this.meter = metricManager.meter(registry, meterName);
      } else {
        withDetails = false;
        this.histogram = null;
        this.meter = null;
      }
    }

    public MetricsInput(Meter totalMeter, Histogram totalHistogram, Histogram histogram, Meter meter, IndexInput in) {
      super(in.toString());
      this.in = in;
      this.totalMeter = totalMeter;
      this.totalHistogram  = totalHistogram;
      this.histogram = histogram;
      this.meter = meter;
      if (totalHistogram != null && meter != null && histogram != null) {
        withDetails = true;
      } else {
        withDetails = false;
      }
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public long getFilePointer() {
      return in.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
      in.seek(pos);
    }

    @Override
    public long length() {
      return in.length();
    }

    @Override
    public IndexInput clone() {
      return new MetricsInput(totalMeter, totalHistogram, histogram, meter, in.clone());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      IndexInput slice = in.slice(sliceDescription, offset, length);
      if (slice != null) {
        return new MetricsInput(totalMeter, totalHistogram, histogram, meter, slice);
      } else {
        return null;
      }
    }

    @Override
    public byte readByte() throws IOException {
      totalMeter.mark();
      if (withDetails) {
        totalHistogram.update(1);
        meter.mark();
        histogram.update(1);
      }
      return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      totalMeter.mark(len);
      if (withDetails) {
        totalHistogram.update(len);
        meter.mark(len);
        histogram.update(len);
      }
      in.readBytes(b, offset, len);
    }
  }
}

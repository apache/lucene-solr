package org.apache.lucene.codecs;

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

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.NamedSPILoader;

public abstract class SimpleDocValuesFormat implements NamedSPILoader.NamedSPI {
  
  private static final NamedSPILoader<SimpleDocValuesFormat> loader =
      new NamedSPILoader<SimpleDocValuesFormat>(SimpleDocValuesFormat.class);
  
  /** Unique name that's used to retrieve this format when
   *  reading the index.
   */
  private final String name;

  /**
   * Creates a new docvalues format.
   * <p>
   * The provided name will be written into the index segment in some configurations
   * (such as when using {@code PerFieldDocValuesFormat}): in such configurations,
   * for the segment to be read this class should be registered with Java's
   * SPI mechanism (registered in META-INF/ of your jar file, etc).
   * @param name must be all ascii alphanumeric, and less than 128 characters in length.
   */
  protected SimpleDocValuesFormat(String name) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
  }

  public abstract SimpleDVConsumer fieldsConsumer(SegmentWriteState state) throws IOException;

  public abstract SimpleDVProducer fieldsProducer(SegmentReadState state) throws IOException;

  @Override
  public final String getName() {
    return name;
  }
  
  @Override
  public String toString() {
    return "DocValuesFormat(name=" + name + ")";
  }
  
  /** looks up a format by name */
  public static SimpleDocValuesFormat forName(String name) {
    if (loader == null) {
      throw new IllegalStateException("You called DocValuesFormat.forName() before all formats could be initialized. "+
          "This likely happens if you call it from a DocValuesFormat's ctor.");
    }
    return loader.lookup(name);
  }
  
  /** returns a list of all available format names */
  public static Set<String> availableDocValuesFormats() {
    if (loader == null) {
      throw new IllegalStateException("You called DocValuesFormat.availableDocValuesFormats() before all formats could be initialized. "+
          "This likely happens if you call it from a DocValuesFormat's ctor.");
    }
    return loader.availableServices();
  }
  
  /** 
   * Reloads the DocValues format list from the given {@link ClassLoader}.
   * Changes to the docvalues formats are visible after the method ends, all
   * iterators ({@link #availableDocValuesFormats()},...) stay consistent. 
   * 
   * <p><b>NOTE:</b> Only new docvalues formats are added, existing ones are
   * never removed or replaced.
   * 
   * <p><em>This method is expensive and should only be called for discovery
   * of new docvalues formats on the given classpath/classloader!</em>
   */
  public static void reloadDocValuesFormats(ClassLoader classloader) {
    loader.reload(classloader);
  }
}

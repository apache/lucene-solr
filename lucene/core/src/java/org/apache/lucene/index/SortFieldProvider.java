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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.search.SortField;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.NamedSPILoader;

/**
 * Reads/Writes a named SortField from a segment info file, used to record index sorts
 */
public abstract class SortFieldProvider implements NamedSPILoader.NamedSPI {

  private static class Holder {
    private static final NamedSPILoader<SortFieldProvider> LOADER = new NamedSPILoader<>(SortFieldProvider.class);

    static NamedSPILoader<SortFieldProvider> getLoader() {
      if (LOADER == null) {
        throw new IllegalStateException("You tried to lookup a SortFieldProvider by name before all SortFieldProviders could be initialized. "+
            "This likely happens if you call SortFieldProvider#forName from a SortFieldProviders's ctor.");
      }
      return LOADER;
    }
  }

  /**
   * Looks up a SortFieldProvider by name
   */
  public static SortFieldProvider forName(String name) {
    return Holder.getLoader().lookup(name);
  }

  /**
   * Lists all available SortFieldProviders
   */
  public static Set<String> availableSortFieldProviders() {
    return Holder.getLoader().availableServices();
  }

  /**
   * Reloads the SortFieldProvider list from the given {@link ClassLoader}.
   * Changes to the list are visible after the method ends, all
   * iterators ({@link #availableSortFieldProviders()} ()},...) stay consistent.
   *
   * <p><b>NOTE:</b> Only new SortFieldProviders are added, existing ones are
   * never removed or replaced.
   *
   * <p><em>This method is expensive and should only be called for discovery
   * of new SortFieldProviders on the given classpath/classloader!</em>
   */
  public static void reloadSortFieldProviders(ClassLoader classLoader) {
    Holder.getLoader().reload(classLoader);
  }

  /**
   * Writes a SortField to a DataOutput
   */
  public static void write(SortField sf, DataOutput output) throws IOException {
    IndexSorter sorter = sf.getIndexSorter();
    if (sorter == null) {
      throw new IllegalArgumentException("Cannot serialize sort field " + sf);
    }
    SortFieldProvider provider = SortFieldProvider.forName(sorter.getProviderName());
    provider.writeSortField(sf, output);
  }

  /** The name this SortFieldProvider is registered under */
  protected final String name;

  /**
   * Creates a new SortFieldProvider.
   * <p>
   * The provided name will be written into the index segment: in order to
   * for the segment to be read this class should be registered with Java's
   * SPI mechanism (registered in META-INF/ of your jar file, etc).
   * @param name must be all ascii alphanumeric, and less than 128 characters in length.
   */
  protected SortFieldProvider(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  /**
   * Reads a SortField from serialized bytes
   */
  public abstract SortField readSortField(DataInput in) throws IOException;

  /**
   * Writes a SortField to a DataOutput
   *
   * This is used to record index sort information in segment headers
   */
  public abstract void writeSortField(SortField sf, DataOutput out) throws IOException;

}

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
package org.apache.solr.index.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.core.HdfsDirectoryFactory;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.apache.solr.util.HdfsUtil;

public class CheckHdfsIndex {
  public static void main(String[] args) throws IOException, InterruptedException {
    int exitCode = doMain(args);
    System.exit(exitCode);
  }

  // actual main: returns exit code instead of terminating JVM (for easy testing)
  @SuppressForbidden(reason = "System.out required: command line tool")
  protected static int doMain(String[] args) throws IOException, InterruptedException {
    CheckIndex.Options opts;
    try {
      opts = CheckIndex.parseOptions(args);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
      return 1;
    }

    if (!CheckIndex.assertsOn()) {
      System.out.println("\nNOTE: testing will be more thorough if you run java with '-ea:org.apache.lucene...', so assertions are enabled");
    }

    if (opts.getDirImpl() != null) {
      System.out.println("\nIgnoring specified -dir-impl, instead using " + HdfsDirectory.class.getSimpleName());
    }

    Path indexPath = new Path(opts.getIndexPath());
    System.out.println("\nOpening index @ " + indexPath + "\n");

    Directory directory;
    try {
      directory = new HdfsDirectory(indexPath, getConf(indexPath));
    } catch (IOException e) {
      System.out.println("ERROR: could not open hdfs directory \"" + indexPath + "\"; exiting");
      e.printStackTrace(System.out);
      return 1;
    }

    try (Directory dir = directory; CheckIndex checker = new CheckIndex(dir)) {
      opts.setOut(System.out);
      return checker.doCheck(opts);
    }
  }

  private static Configuration getConf(Path path) {
    Configuration conf = new Configuration();
    String confDir = System.getProperty(HdfsDirectoryFactory.CONFIG_DIRECTORY);
    HdfsUtil.addHdfsResources(conf, confDir);

    String fsScheme = path.toUri().getScheme();
    if(fsScheme != null) {
      conf.setBoolean("fs." + fsScheme + ".impl.disable.cache", true);
    }
    return conf;
  }
}

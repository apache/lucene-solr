package org.apache.lucene.server;

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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.server.handlers.DocHandler;
import org.apache.lucene.server.handlers.Handler;
import org.apache.lucene.server.plugins.Plugin;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;

/** Holds all global state for the server.  Per-index state
 *  is held in {@link IndexState}.
 *
 * <p> Filesystem state ($stateDir is the "global state
 * dir", passed to GlobalState on init):
 *
 * <ul>
 *   <li> $stateDir/indices.gen: holds map of indexName to
 *     root filesystem path for that index
 *   <li> $stateDir/plugins: a directory with one
 *     sub-directory per plugin
 * </ul>
 *
 * Each plugin sub-directory ($stateDir/plugins/foo/)
 * defines one plugin, and contains:
 * <ul>
 *   <li> lucene-server-plugin.properties:
 *     file with the properties for plugin {@code foo};
 *     alternatively, the properties file can be inside a
 *     *.zip or *.jar in the plugin directory.  This must
 *     define the property "class" with the fully qualified
 *     path to the Plugin class to instantiate
 *   <li> $stateDir/plugins/foo/lib/*: optional, contains
 *     *.jar to add to the classpath while loading the
 *     plugin
 *   <li> $stateDir/plugins/*.zip or *.jar contains the
 *     plugins class files and optionally the properties
 *     file
 * </ul>

 * Each index has its own indexDir, specified when the index
 * is created; see {@link IndexState} for details. */
public class GlobalState implements Closeable {

  private static final String PLUGIN_PROPERTIES_FILE = "lucene-server-plugin.properties";

  // TODO: make these controllable
  private final static int MAX_INDEXING_THREADS = 6;

  final DocHandler docHandler = new DocHandler();

  private final Map<String,Handler> handlers = new HashMap<String,Handler>();

  // TODO: really this queue should be based on total size
  // of the queued docs:
  private final static int MAX_BUFFERED_DOCS = 2*MAX_INDEXING_THREADS;

  private final Map<String,Plugin> plugins = new HashMap<String,Plugin>();

  final BlockingQueue<Runnable> docsToIndex = new ArrayBlockingQueue<Runnable>(MAX_BUFFERED_DOCS, true);

  /** Common thread pool to index documents. */
  public final ExecutorService indexService = new BlockingThreadPoolExecutor(MAX_BUFFERED_DOCS,
                                                                             MAX_INDEXING_THREADS,
                                                                             MAX_INDEXING_THREADS,
                                                                             60, TimeUnit.SECONDS,
                                                                             docsToIndex,
                                                                             new NamedThreadFactory("LuceneIndexing"));
  /** Server shuts down once this latch is decremented. */
  public final CountDownLatch shutdownNow = new CountDownLatch(1);

  /** Current indices. */
  final Map<String,IndexState> indices = new ConcurrentHashMap<String,IndexState>();

  final File stateDir;

  /** This is persisted so on restart we know about all
   *  previously created indices. */
  private final JSONObject indexNames = new JSONObject();
  
  private long lastIndicesGen;

  /** Sole constructor. */
  public GlobalState(File stateDir) {
    this.stateDir = stateDir;
    if (!stateDir.exists()) {
      stateDir.mkdirs();
    }
  }

  /** Record a new handler, by methode name (search,
   *  addDocument, etc.).  The server registers all builtin
   *  handlers on startup, but plugins can also register
   *  their own handlers when they are instantiated. */
  public void addHandler(String name, Handler handler) {
    if (handlers.containsKey(name)) {
      throw new IllegalArgumentException("handler \"" + name + "\" is already defined");
    }
    handlers.put(name, handler);
  }

  /** Retrieve a handler by method name (search,
   *  addDocument, etc.). */
  public Handler getHandler(String name) {
    Handler h = handlers.get(name);
    if (h == null) {
      throw new IllegalArgumentException("handler \"" + name + "\" is not defined");
    }
    return h;
  }

  /** Get all handlers. */
  public Map<String,Handler> getHandlers() {
    return Collections.unmodifiableMap(handlers);
  }

  /** Get the {@link IndexState} by index name. */
  public IndexState get(String name) throws Exception {
    synchronized(indices) {
      IndexState state = indices.get(name);
      if (state == null) {
        String rootPath = (String) indexNames.get(name);
        if (rootPath != null) {
          if (rootPath.equals("NULL")) {
            state = new IndexState(this, name, null, false);
          } else {
            state = new IndexState(this, name, new File(rootPath), false);
          }
          indices.put(name, state);
        } else {
          throw new IllegalArgumentException("index \"" + name + "\" was not yet created");
        }
      }
      return state;
    }
  }

  /** Remove the specified index. */
  public void deleteIndex(String name) {
    synchronized(indices) {
      indexNames.remove(name);
    }
  }

  /** Create a new index. */
  public IndexState createIndex(String name, File rootDir) throws Exception {
    synchronized (indices) {
      if (indexNames.containsKey(name)) {
        throw new IllegalArgumentException("index \"" + name + "\" already exists");
      }
      if (rootDir == null) {
        indexNames.put(name, "NULL");
      } else {
        if (rootDir.exists()) {
          throw new IllegalArgumentException("rootDir \"" + rootDir + "\" already exists");
        }
        indexNames.put(name, rootDir.getAbsolutePath().toString());
      }
      saveIndexNames();
      IndexState state = new IndexState(this, name, rootDir, true);
      indices.put(name, state);
      return state;
    }
  }

  void removeIndex(String name) {
    synchronized(indices) {
      indices.remove(name);
    }
  }

  void loadIndexNames() throws IOException {
    long gen = IndexState.getLastGen(stateDir, "indices");
    lastIndicesGen = gen;
    if (gen != -1) {
      File path = new File(stateDir, "indices." + gen);
      RandomAccessFile raf = new RandomAccessFile(path, "r");
      byte[] bytes = new byte[(int) raf.length()];
      raf.read(bytes);
      raf.close();
      JSONObject o;
      try {
        o = (JSONObject) JSONValue.parseStrict(IndexState.fromUTF8(bytes));
      } catch (ParseException pe) {
        // Something corrupted the save state since we last
        // saved it ...
        throw new RuntimeException("index state file \"" + path + "\" cannot be parsed: " + pe.getMessage());
      }
      indexNames.putAll(o);
    }
  }

  private void saveIndexNames() throws IOException {
    synchronized(indices) {
      lastIndicesGen++;
      File f = new File(stateDir, "indices." + lastIndicesGen);
      RandomAccessFile raf = new RandomAccessFile(f, "rw");
      raf.write(IndexState.toUTF8(indexNames.toString()));
      raf.getFD().sync();
      raf.close();
      for(String sub : stateDir.list()) {
        if (sub.startsWith("indices.")) {
          long gen = Long.parseLong(sub.substring(8));
          if (gen != lastIndicesGen) {
            new File(stateDir, sub).delete();
          }
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    //System.out.println("GlobalState.close");
    IOUtils.close(indices.values());
    indexService.shutdown();
    TimeLimitingCollector.getGlobalTimerThread().stopTimer();
    try {
      TimeLimitingCollector.getGlobalTimerThread().join();
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  /** Load any plugins. */
  @SuppressWarnings({"unchecked"})
  public void loadPlugins() throws Exception {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Class<?> classLoaderClass = classLoader.getClass();
    Method addURL = null;
    while (!classLoaderClass.equals(Object.class)) {
      try {
        addURL = classLoaderClass.getDeclaredMethod("addURL", URL.class);
        addURL.setAccessible(true);
        break;
      } catch (NoSuchMethodException e) {
        // no method, try the parent
        classLoaderClass = classLoaderClass.getSuperclass();
      }
    }

    if (addURL == null) {
      throw new IllegalStateException("failed to find addURL method on classLoader [" + classLoader + "] to add methods");
    }

    File pluginsDir = new File(stateDir, "plugins");
    if (pluginsDir.exists()) {

      if (!pluginsDir.isDirectory()) {
        throw new IllegalStateException("\"" + pluginsDir.getAbsolutePath() + "\" is not a directory");
      }

      File[] files = pluginsDir.listFiles();
      if (files == null) {
        throw new IllegalStateException("failed to list files for plugin directory \"" + pluginsDir.getAbsolutePath() + "\"");
      }

      // First, add all plugin resources onto classpath:
      for(File pluginDir : files) {
        if (pluginDir.isDirectory()) {
          File[] pluginFiles = pluginDir.listFiles();
          if (pluginFiles == null) {
            throw new IllegalStateException("failed to list files for plugin directory \"" + pluginDir.getAbsolutePath() + "\"");
          }

          // Verify the plugin contains
          // PLUGIN_PROPERTIES_FILE somewhere:
          File propFile = new File(pluginDir, PLUGIN_PROPERTIES_FILE);

          if (!propFile.exists()) {
            // See if properties file is in root JAR/ZIP:
            boolean found = false;
            for(File pluginFile : pluginFiles) {
              if (pluginFile.getName().endsWith(".jar") || 
                  pluginFile.getName().endsWith(".zip")) {
                ZipInputStream zis;
                try {
                  zis = new ZipInputStream(new FileInputStream(pluginFile));
                } catch (Exception e) {
                  throw new IllegalStateException("failed to open \"" + pluginFile + "\" as ZipInputStream");
                }
                try {
                  ZipEntry e;
                  while((e = zis.getNextEntry()) != null) {
                    if (e.getName().equals(PLUGIN_PROPERTIES_FILE)) {
                      found = true;
                      break;
                    }
                  }
                } finally {
                  zis.close();
                }
                if (found) {
                  break;
                }
              }
            }

            if (!found) {
              throw new IllegalStateException("plugin \"" + pluginDir.getAbsolutePath() + "\" is missing the " + PLUGIN_PROPERTIES_FILE + " file");
            }
          }

          System.out.println("Start plugin " + pluginDir.getAbsolutePath());

          // Add the plugin's root
          addURL.invoke(classLoader, pluginDir.toURI().toURL());

          // Add any .jar/.zip in the plugin's root directory:
          for(File pluginFile : pluginFiles) {
            if (pluginFile.getName().endsWith(".jar") || 
                pluginFile.getName().endsWith(".zip")) {
              addURL.invoke(classLoader, pluginFile.toURI().toURL());
            }
          }

          // Add any .jar files in the plugin's lib sub
          // directory, if it exists:
          File pluginLibDir = new File(pluginDir, "lib");
          if (pluginLibDir.exists()) {
            File[] pluginLibFiles = pluginLibDir.listFiles();
            if (pluginLibFiles == null) {
              throw new IllegalStateException("failed to list files for plugin lib directory \"" + pluginLibDir.getAbsolutePath() + "\"");
            }
            
            for(File pluginFile : pluginLibFiles) {
              if (pluginFile.getName().endsWith(".jar")) {
                addURL.invoke(classLoader, pluginFile.toURI().toURL());
              }
            }
          }
        }
      }
          
      // Then, init/load all plugins:
      Enumeration<URL> pluginURLs = classLoader.getResources(PLUGIN_PROPERTIES_FILE);

      while (pluginURLs.hasMoreElements()) {
        URL pluginURL = pluginURLs.nextElement();
        Properties pluginProps = new Properties();
        InputStream is = pluginURL.openStream();
        try {
          pluginProps.load(new InputStreamReader(is, "UTF-8"));
        } catch (Exception e) {
          throw new IllegalStateException("property file \"" + pluginURL + "\" could not be loaded", e);
        } finally {
          is.close();
        }

        String pluginClassName = pluginProps.getProperty("class");
        if (pluginClassName == null) {
          throw new IllegalStateException("property file \"" + pluginURL + "\" does not have the \"class\" property");
        }

        Class<? extends Plugin> pluginClass = (Class<? extends Plugin>) classLoader.loadClass(pluginClassName);
        Constructor<? extends Plugin> ctor;
        try {
          ctor = pluginClass.getConstructor(GlobalState.class);
        } catch (NoSuchMethodException e1) {
          throw new IllegalStateException("class \"" + pluginClassName + "\" for plugin \"" + pluginURL + "\" does not have constructor that takes GlobalState");
        }

        Plugin plugin;
        try {
          plugin = ctor.newInstance(this);
        } catch (Exception e) {
          throw new IllegalStateException("failed to instantiate class \"" + pluginClassName + "\" for plugin \"" + pluginURL, e);
        }
        if (plugins.containsKey(plugin.getName())) {
          throw new IllegalStateException("plugin \"" + plugin.getName() + "\" appears more than once");
        }
        // nocommit verify plugin name matches subdir directory name
        plugins.put(plugin.getName(), plugin);
      }
    }
  }
}

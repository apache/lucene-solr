package org.apache.solr.core;

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

import org.apache.commons.lang.StringUtils;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.SolrXMLSerializer.SolrCoreXMLDef;
import org.apache.solr.util.DOMUtil;
import org.w3c.dom.Node;

import javax.xml.xpath.XPathExpressionException;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;


class SolrCores {
  private static SolrXMLSerializer SOLR_XML_SERIALIZER = new SolrXMLSerializer();
  private static Object modifyLock = new Object(); // for locking around manipulating any of the core maps.
  private final Map<String, SolrCore> cores = new LinkedHashMap<String, SolrCore>(); // For "permanent" cores

  //WARNING! The _only_ place you put anything into the list of transient cores is with the putTransientCore method!
  private Map<String, SolrCore> transientCores = new LinkedHashMap<String, SolrCore>(); // For "lazily loaded" cores

  private final Map<String, CoreDescriptor> dynamicDescriptors = new LinkedHashMap<String, CoreDescriptor>();

  private final Map<String, SolrCore> createdCores = new LinkedHashMap<String, SolrCore>();

  private Map<SolrCore, String> coreToOrigName = new ConcurrentHashMap<SolrCore, String>();

  private final CoreContainer container;

  // This map will hold objects that are being currently operated on. The core (value) may be null in the case of
  // initial load. The rule is, never to any operation on a core that is currently being operated upon.
  private static final Set<String> pendingCoreOps = new HashSet<String>();

  // Due to the fact that closes happen potentially whenever anything is _added_ to the transient core list, we need
  // to essentially queue them up to be handled via pendingCoreOps.
  private static final List<SolrCore> pendingCloses = new ArrayList<SolrCore>();

  SolrCores(CoreContainer container) {
    this.container = container;
  }

  // Trivial helper method for load, note it implements LRU on transient cores. Also note, if
  // there is no setting for max size, nothing is done and all cores go in the regular "cores" list
  protected void allocateLazyCores(final ConfigSolr cfg, final SolrResourceLoader loader) {
    final int transientCacheSize = cfg.getInt(ConfigSolr.CfgProp.SOLR_TRANSIENTCACHESIZE, Integer.MAX_VALUE);
    if (transientCacheSize != Integer.MAX_VALUE) {
      CoreContainer.log.info("Allocating transient cache for {} transient cores", transientCacheSize);
      transientCores = new LinkedHashMap<String, SolrCore>(transientCacheSize, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, SolrCore> eldest) {
          if (size() > transientCacheSize) {
            synchronized (modifyLock) {
              pendingCloses.add(eldest.getValue()); // Essentially just queue this core up for closing.
              modifyLock.notifyAll(); // Wakes up closer thread too
            }
            return true;
          }
          return false;
        }
      };
    }
  }

  protected void putDynamicDescriptor(String rawName, CoreDescriptor p) {
    synchronized (modifyLock) {
      dynamicDescriptors.put(rawName, p);
    }
  }

  // We are shutting down. You can't hold the lock on the various lists of cores while they shut down, so we need to
  // make a temporary copy of the names and shut them down outside the lock.
  protected void close() {
    Collection<SolrCore> coreList = new ArrayList<SolrCore>();

    // It might be possible for one of the cores to move from one list to another while we're closing them. So
    // loop through the lists until they're all empty. In particular, the core could have moved from the transient
    // list to the pendingCloses list.

    do {
      coreList.clear();
      synchronized (modifyLock) {
        // make a copy of the cores then clear the map so the core isn't handed out to a request again
        coreList.addAll(cores.values());
        cores.clear();

        coreList.addAll(transientCores.values());
        transientCores.clear();

        coreList.addAll(pendingCloses);
        pendingCloses.clear();
      }

      for (SolrCore core : coreList) {
        try {
          core.close();
        } catch (Throwable t) {
          SolrException.log(CoreContainer.log, "Error shutting down core", t);
        }
      }
    } while (coreList.size() > 0);
  }

  //WARNING! This should be the _only_ place you put anything into the list of transient cores!
  protected SolrCore putTransientCore(ConfigSolr cfg, String name, SolrCore core, SolrResourceLoader loader) {
    SolrCore retCore;
    CoreContainer.log.info("Opening transient core {}", name);
    synchronized (modifyLock) {
      retCore = transientCores.put(name, core);
    }
    return retCore;
  }

  protected SolrCore putCore(String name, SolrCore core) {
    synchronized (modifyLock) {
      return cores.put(name, core);
    }
  }

  List<SolrCore> getCores() {
    List<SolrCore> lst = new ArrayList<SolrCore>();

    synchronized (modifyLock) {
      lst.addAll(cores.values());
      return lst;
    }
  }

  Set<String> getCoreNames() {
    Set<String> set = new TreeSet<String>();

    synchronized (modifyLock) {
      set.addAll(cores.keySet());
      set.addAll(transientCores.keySet());
    }
    return set;
  }

  List<String> getCoreNames(SolrCore core) {
    List<String> lst = new ArrayList<String>();

    synchronized (modifyLock) {
      for (Map.Entry<String, SolrCore> entry : cores.entrySet()) {
        if (core == entry.getValue()) {
          lst.add(entry.getKey());
        }
      }
      for (Map.Entry<String, SolrCore> entry : transientCores.entrySet()) {
        if (core == entry.getValue()) {
          lst.add(entry.getKey());
        }
      }
    }
    return lst;
  }

  /**
   * Gets a list of all cores, loaded and unloaded (dynamic)
   *
   * @return all cores names, whether loaded or unloaded.
   */
  public Collection<String> getAllCoreNames() {
    Set<String> set = new TreeSet<String>();
    synchronized (modifyLock) {
      set.addAll(cores.keySet());
      set.addAll(transientCores.keySet());
      set.addAll(dynamicDescriptors.keySet());
      set.addAll(createdCores.keySet());
    }
    return set;
  }

  SolrCore getCore(String name) {

    synchronized (modifyLock) {
      return cores.get(name);
    }
  }

  protected void swap(String n0, String n1) {

    synchronized (modifyLock) {
      SolrCore c0 = cores.get(n0);
      SolrCore c1 = cores.get(n1);
      if (c0 == null) { // Might be an unloaded transient core
        c0 = container.getCore(n0);
        if (c0 == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such core: " + n0);
        }
      }
      if (c1 == null) { // Might be an unloaded transient core
        c1 = container.getCore(n1);
        if (c1 == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such core: " + n1);
        }
      }
      cores.put(n0, c1);
      cores.put(n1, c0);

      c0.setName(n1);
      c0.getCoreDescriptor().putProperty(CoreDescriptor.CORE_NAME, n1);
      c1.setName(n0);
      c1.getCoreDescriptor().putProperty(CoreDescriptor.CORE_NAME, n0);
    }

  }

  protected SolrCore remove(String name, boolean removeOrig) {

    synchronized (modifyLock) {
      SolrCore tmp = cores.remove(name);
      SolrCore ret = null;
      if (removeOrig && tmp != null) {
        coreToOrigName.remove(tmp);
      }
      ret = (ret == null) ? tmp : ret;
      // It could have been a newly-created core. It could have been a transient core. The newly-created cores
      // in particular should be checked. It could have been a dynamic core.
      tmp = transientCores.remove(name);
      ret = (ret == null) ? tmp : ret;
      tmp = createdCores.remove(name);
      ret = (ret == null) ? tmp : ret;
      dynamicDescriptors.remove(name);
      return ret;
    }
  }

  protected void putCoreToOrigName(SolrCore c, String name) {

    synchronized (modifyLock) {
      coreToOrigName.put(c, name);
    }

  }

  protected void removeCoreToOrigName(SolrCore newCore, SolrCore core) {

    synchronized (modifyLock) {
      String origName = coreToOrigName.remove(core);
      if (origName != null) {
        coreToOrigName.put(newCore, origName);
      }
    }
  }

  /* If you don't increment the reference count, someone could close the core before you use it. */
  protected SolrCore  getCoreFromAnyList(String name, boolean incRefCount) {
    synchronized (modifyLock) {
      SolrCore core = cores.get(name);

      if (core == null) {
        core = transientCores.get(name);
      }

      if (core != null && incRefCount) {
        core.open();
      }

      return core;
    }
  }

  protected CoreDescriptor getDynamicDescriptor(String name) {
    synchronized (modifyLock) {
      return dynamicDescriptors.get(name);
    }
  }

  protected boolean isLoaded(String name) {
    synchronized (modifyLock) {
      if (cores.containsKey(name)) {
        return true;
      }
      if (transientCores.containsKey(name)) {
        return true;
      }
    }
    return false;

  }

  protected CoreDescriptor getUnloadedCoreDescriptor(String cname) {
    synchronized (modifyLock) {
      CoreDescriptor desc = dynamicDescriptors.get(cname);
      if (desc == null) {
        return null;
      }
      return new CoreDescriptor(desc);
    }

  }

  protected String getCoreToOrigName(SolrCore solrCore) {
    synchronized (modifyLock) {
      return coreToOrigName.get(solrCore);
    }
  }

  public void persistCores(Config cfg, Properties containerProperties,
      Map<String,String> rootSolrAttribs, Map<String,String> coresAttribs,
      Map<String, String> loggingAttribs, Map<String,String> watcherAttribs,
      Map<String, String> shardHandlerAttrib, Map<String,String> shardHandlerProps,
      File file, SolrResourceLoader loader) throws XPathExpressionException {


    List<SolrXMLSerializer.SolrCoreXMLDef> solrCoreXMLDefs = new ArrayList<SolrXMLSerializer.SolrCoreXMLDef>();
    synchronized (modifyLock) {

      persistCores(cfg, cores, loader, solrCoreXMLDefs);
      persistCores(cfg, transientCores, loader, solrCoreXMLDefs);
      // add back all the cores that aren't loaded, either in cores or transient
      // cores
      for (Map.Entry<String,CoreDescriptor> ent : dynamicDescriptors.entrySet()) {
        if (!cores.containsKey(ent.getKey())
            && !transientCores.containsKey(ent.getKey())) {
          addCoreToPersistList(cfg, loader, ent.getValue(), null, solrCoreXMLDefs);
        }
      }
      for (Map.Entry<String,SolrCore> ent : createdCores.entrySet()) {
        if (!cores.containsKey(ent.getKey())
            && !transientCores.containsKey(ent.getKey())
            && !dynamicDescriptors.containsKey(ent.getKey())) {
          addCoreToPersistList(cfg, loader, ent.getValue().getCoreDescriptor(),
              null, solrCoreXMLDefs);
        }
      }

      SolrXMLSerializer.SolrXMLDef solrXMLDef = new SolrXMLSerializer.SolrXMLDef();
      solrXMLDef.coresDefs = solrCoreXMLDefs;
      solrXMLDef.containerProperties = containerProperties;
      solrXMLDef.solrAttribs = rootSolrAttribs;
      solrXMLDef.coresAttribs = coresAttribs;
      solrXMLDef.loggingAttribs = loggingAttribs;
      solrXMLDef.watcherAttribs = watcherAttribs;
      solrXMLDef.shardHandlerAttribs = shardHandlerAttrib;
      solrXMLDef.shardHandlerProps = shardHandlerProps;
      SOLR_XML_SERIALIZER.persistFile(file, solrXMLDef);
    }

  }
  // Wait here until any pending operations (load, unload or reload) are completed on this core.
  protected SolrCore waitAddPendingCoreOps(String name) {

    // Keep multiple threads from operating on a core at one time.
    synchronized (modifyLock) {
      boolean pending;
      do { // Are we currently doing anything to this core? Loading, unloading, reloading?
        pending = pendingCoreOps.contains(name); // wait for the core to be done being operated upon
        if (! pending) { // Linear list, but shouldn't be too long
          for (SolrCore core : pendingCloses) {
            if (core.getName().equals(name)) {
              pending = true;
              break;
            }
          }
        }
        if (container.isShutDown()) return null; // Just stop already.

        if (pending) {
          try {
            modifyLock.wait();
          } catch (InterruptedException e) {
            return null; // Seems best not to do anything at all if the thread is interrupted
          }
        }
      } while (pending);
      // We _really_ need to do this within the synchronized block!
      if (! container.isShutDown()) {
        if (! pendingCoreOps.add(name)) {
          CoreContainer.log.warn("Replaced an entry in pendingCoreOps {}, we should not be doing this", name);
        }
        return getCoreFromAnyList(name, false); // we might have been _unloading_ the core, so return the core if it was loaded.
      }
    }
    return null;
  }

  // We should always be removing the first thing in the list with our name! The idea here is to NOT do anything n
  // any core while some other operation is working on that core.
  protected void removeFromPendingOps(String name) {
    synchronized (modifyLock) {
      if (! pendingCoreOps.remove(name)) {
        CoreContainer.log.warn("Tried to remove core {} from pendingCoreOps and it wasn't there. ", name);
      }
      modifyLock.notifyAll();
    }
  }


  protected void persistCores(Config cfg, Map<String, SolrCore> whichCores, SolrResourceLoader loader, List<SolrCoreXMLDef> solrCoreXMLDefs) throws XPathExpressionException {
    for (SolrCore solrCore : whichCores.values()) {
      addCoreToPersistList(cfg, loader, solrCore.getCoreDescriptor(), getCoreToOrigName(solrCore), solrCoreXMLDefs);
    }
  }

  private void addCoreProperty(Map<String,String> propMap, SolrResourceLoader loader, Node node, String name,
                               String value) {
    addCoreProperty(propMap, loader, node, name, value, null);
  }

  private void addCoreProperty(Map<String,String> propMap, SolrResourceLoader loader, Node node, String name,
      String value, String defaultValue) {

    if (node == null) {
      propMap.put(name, value);
      return;
    }

    if (node != null) {
      String rawAttribValue = DOMUtil.getAttr(node, name, null);

      if (rawAttribValue == null) {
        return; // It was never in the original definition.
      }

      if (value == null) {
        propMap.put(name, rawAttribValue);
        return;
      }

      // There are some _really stupid_ additions/subtractions of the slash that we should look out for. I'm (EOE)
      // ashamed of this but it fixes some things and we're throwing persistence away anyway (although
      // maybe not for core.properties files).
      String defComp = regularizeAttr(defaultValue);

      if (defComp != null && regularizeAttr(value).equals(defComp)) {
        return;
      }
      String rawComp = regularizeAttr(rawAttribValue);
      if (rawComp != null && regularizeAttr(value).equals(
          regularizeAttr(DOMUtil.substituteProperty(rawAttribValue, loader.getCoreProperties())))) {
        propMap.put(name, rawAttribValue);
      } else {
        propMap.put(name, value);
      }
    }
  }

  protected String regularizeAttr(String path) {
    if (path == null)
      return null;
    path = path.replace('/', File.separatorChar);
    path = path.replace('\\', File.separatorChar);
    if (path.endsWith(File.separator)) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }
  protected void addCoreToPersistList(Config cfg, SolrResourceLoader loader,
      CoreDescriptor dcore, String origCoreName,
      List<SolrCoreXMLDef> solrCoreXMLDefs) throws XPathExpressionException {

    Map<String,String> coreAttribs = new HashMap<String,String>();
    Properties newProps = new Properties();

    // This is simple, just take anything sent in and saved away in at core creation and write it out.
    if (dcore.getCreatedProperties().size() > 0) {
      final List<String> stdNames = new ArrayList<String>(Arrays.asList(CoreDescriptor.standardPropNames));
      coreAttribs.put(CoreDescriptor.CORE_NAME, dcore.getName()); // NOTE: may have been swapped or renamed!
      for (String key : dcore.getCreatedProperties().stringPropertyNames()) {
        if (! stdNames.contains(key) && ! key.startsWith(CoreAdminParams.PROPERTY_PREFIX)) continue;
        if (key.indexOf(CoreAdminParams.PROPERTY_PREFIX) == 0) {
          newProps.put(key.substring(CoreAdminParams.PROPERTY_PREFIX.length()), dcore.getCreatedProperties().getProperty(key));
        } else if (! CoreDescriptor.CORE_NAME.equals(key)) {
          coreAttribs.put(key, dcore.getCreatedProperties().getProperty(key));
        }
      }
      // Insure instdir is persisted if it's the default since it's checked at startup even if not specified on the
      // create command.
      if (! dcore.getCreatedProperties().containsKey(CoreDescriptor.CORE_INSTDIR)) {
        coreAttribs.put(CoreDescriptor.CORE_INSTDIR, dcore.getRawInstanceDir());
      }
    } else {

      String coreName = dcore.getProperty(CoreDescriptor.CORE_NAME);

      CloudDescriptor cd = dcore.getCloudDescriptor();
      String collection = null;
      if (cd != null) collection = cd.getCollectionName();

      if (origCoreName == null) {
        origCoreName = coreName;
      }

      Node node = null;
      if (cfg != null) {
        node = cfg.getNode("/solr/cores/core[@name='" + origCoreName + "']",
            false);
      }

      coreAttribs.put(CoreDescriptor.CORE_NAME, coreName);
      //coreAttribs.put(CoreDescriptor.CORE_INSTDIR, dcore.getRawInstanceDir());
      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_INSTDIR, dcore.getRawInstanceDir(), null);

      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_COLLECTION,
          StringUtils.isNotBlank(collection) ? collection : dcore.getName());

      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_DATADIR,
          dcore.getDataDir());
      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_ULOGDIR,
          dcore.getUlogDir());
      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_TRANSIENT,
          Boolean.toString(dcore.isTransient()));
      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_LOADONSTARTUP,
          Boolean.toString(dcore.isLoadOnStartup()));
      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_CONFIG,
          dcore.getConfigName());
      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_SCHEMA,
          dcore.getSchemaName());

      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_COLLECTION,
          collection, dcore.getName());

      String shard = null;
      String roles = null;
      String node_name = null;
      if (cd != null) {
        shard = cd.getShardId();
        roles = cd.getRoles();
        node_name = cd.getCoreNodeName();
      }
      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_SHARD,
          shard);

      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_ROLES,
          roles);

      addCoreProperty(coreAttribs, loader, node, CoreDescriptor.CORE_NODE_NAME,
          node_name);


      for (Object key : dcore.getCoreProperties().keySet()) {

        if (cfg != null) {
          Node propNode = cfg.getNode("/solr/cores/core[@name='" + origCoreName + "']/property[@name='" + key + "']",
              false);

          if (propNode != null) { // This means it was in the original DOM element, so just copy it.
            newProps.put(DOMUtil.getAttr(propNode, "name", null), DOMUtil.getAttr(propNode, "value", null));
          }
        }
      }
    }



    SolrXMLSerializer.SolrCoreXMLDef solrCoreXMLDef = new SolrXMLSerializer.SolrCoreXMLDef();
    solrCoreXMLDef.coreAttribs = coreAttribs;
    solrCoreXMLDef.coreProperties = newProps;
    solrCoreXMLDefs.add(solrCoreXMLDef);
  }

  protected Object getModifyLock() {
    return modifyLock;
  }

  // Be a little careful. We don't want to either open or close a core unless it's _not_ being opened or closed by
  // another thread. So within this lock we'll walk along the list of pending closes until we find something NOT in
  // the list of threads currently being loaded or reloaded. The "usual" case will probably return the very first
  // one anyway..
  protected SolrCore getCoreToClose() {
    synchronized (modifyLock) {
      for (SolrCore core : pendingCloses) {
        if (! pendingCoreOps.contains(core.getName())) {
          pendingCoreOps.add(core.getName());
          pendingCloses.remove(core);
          return core;
        }
      }
    }
    return null;
  }

  protected void addCreated(SolrCore core) {
    synchronized (modifyLock) {
      createdCores.put(core.getName(), core);
    }
  }
}

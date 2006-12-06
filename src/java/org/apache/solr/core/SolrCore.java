/**
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.request.*;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.*;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.StrUtils;
import org.apache.solr.util.XML;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import javax.xml.xpath.XPathConstants;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;


/**
 * @author yonik
 * @author <a href='mailto:mbaranczak@epublishing.com'> Mike Baranczak </a> 
 * @version $Id$
 */

public final class SolrCore {
  public static final String version="1.0";  

  public static Logger log = Logger.getLogger(SolrCore.class.getName());

  private final IndexSchema schema;
  private final String dataDir;
  private final String index_path;
  private final UpdateHandler updateHandler;
  private static final long startTime = System.currentTimeMillis();

  public long getStartTime() { return startTime; }

  public static SolrIndexConfig mainIndexConfig = new SolrIndexConfig("mainIndex");

  static {
    BooleanQuery.setMaxClauseCount(SolrConfig.config.getInt("query/maxBooleanClauses",BooleanQuery.getMaxClauseCount()));
  }


  public static List<SolrEventListener> parseListener(String path) {
    List<SolrEventListener> lst = new ArrayList<SolrEventListener>();
    log.info("Searching for listeners: " +path);
    NodeList nodes = (NodeList)SolrConfig.config.evaluate(path, XPathConstants.NODESET);
    if (nodes!=null) {
      for (int i=0; i<nodes.getLength(); i++) {
        Node node = nodes.item(i);
          String className = DOMUtil.getAttr(node,"class");
          SolrEventListener listener = (SolrEventListener)Config.newInstance(className);
          listener.init(DOMUtil.childNodesToNamedList(node));
          lst.add(listener);
          log.info("added SolrEventListener: " + listener);
      }
    }
    return lst;
  }

  List<SolrEventListener> firstSearcherListeners;
  List<SolrEventListener> newSearcherListeners;
  private void parseListeners() {
    firstSearcherListeners = parseListener("//listener[@event=\"firstSearcher\"]");
    newSearcherListeners = parseListener("//listener[@event=\"newSearcher\"]");
  }

  public IndexSchema getSchema() { return schema; }
  public String getDataDir() { return index_path; }
  public String getIndexDir() { return index_path; }

  private final RequestHandlers reqHandlers = new RequestHandlers(SolrConfig.config);

  public SolrRequestHandler getRequestHandler(String handlerName) {
    return reqHandlers.get(handlerName);
  }


  // gets a non-caching searcher
  public SolrIndexSearcher newSearcher(String name) throws IOException {
    return new SolrIndexSearcher(schema, name,getDataDir(),false);
  }


  void initIndex() {
    try {
      File dirFile = new File(getIndexDir());
      boolean indexExists = dirFile.canRead();

      boolean removeLocks = SolrConfig.config.getBool("mainIndex/unlockOnStartup", false);
      if (removeLocks) {
        // to remove locks, the directory must already exist... so we create it
        // if it didn't exist already...
        Directory dir = FSDirectory.getDirectory(dirFile, !indexExists);
        if (IndexReader.isLocked(dir)) {
          log.warning("WARNING: Solr index directory '" + getDataDir() + "' is locked.  Unlocking...");
          IndexReader.unlock(dir);
        }
      }

      // Create the index if it doesn't exist. Note that indexExists was tested *before*
      // lock removal, since that will result in the creation of the directory.
      if(!indexExists) {
        log.warning("Solr index directory '" + dirFile + "' doesn't exist."
                + " Creating new index...");

        SolrIndexWriter writer = new SolrIndexWriter("SolrCore.initIndex",getDataDir(), true, schema, mainIndexConfig);
        writer.close();

      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  private UpdateHandler createUpdateHandler(String className) {
    try {
      Class handlerClass = Config.findClass(className);
      java.lang.reflect.Constructor cons = handlerClass.getConstructor(new Class[]{SolrCore.class});
      return (UpdateHandler)cons.newInstance(new Object[]{this});
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(500,"Error Instantiating Update Handler "+className, e);
    }
  }


  // Singleton for now...
  private static SolrCore core;

  public static SolrCore getSolrCore() {
    synchronized (SolrCore.class) {
      if (core==null) core = new SolrCore(null,null);
      return core;
    }
  }


  public SolrCore(String dataDir, IndexSchema schema) {
    synchronized (SolrCore.class) {
      // this is for backward compatibility (and also the reason
      // the sync block is needed)
      core = this;   // set singleton

      if (dataDir ==null) {
        dataDir =SolrConfig.config.get("dataDir",Config.getInstanceDir()+"data");
      }

      log.info("Opening new SolrCore at " + Config.getInstanceDir() + ", dataDir="+dataDir);

      if (schema==null) {
        schema = new IndexSchema("schema.xml");
      }

      this.schema = schema;
      this.dataDir = dataDir;
      this.index_path = dataDir + "/" + "index";

      parseListeners();

      initIndex();
      
      initWriters();

      try {
        // Open the searcher *before* the handler so we don't end up opening
        // one in the middle.
        getSearcher(false,false,null);

        updateHandler = createUpdateHandler(
                SolrConfig.config.get("updateHandler/@class", DirectUpdateHandler.class.getName())
        );

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }


  public void close() {
    log.info("CLOSING SolrCore!");
    try {
      closeSearcher();
    } catch (Exception e) {
      SolrException.log(log,e);
    }
    try {
      searcherExecutor.shutdown();
    } catch (Exception e) {
      SolrException.log(log,e);
    }
    try {
      updateHandler.close();
    } catch (Exception e) {
      SolrException.log(log,e);
    }
  }


  void finalizer() { close(); }


  ////////////////////////////////////////////////////////////////////////////////
  // Searcher Control
  ////////////////////////////////////////////////////////////////////////////////

  // The current searcher used to service queries.
  // Don't access this directly!!!! use getSearcher() to
  // get it (and it will increment the ref count at the same time)
  private RefCounted<SolrIndexSearcher> _searcher;

  final ExecutorService searcherExecutor = Executors.newSingleThreadExecutor();
  private int onDeckSearchers;  // number of searchers preparing
  private Object searcherLock = new Object();  // the sync object for the searcher


  public RefCounted<SolrIndexSearcher> getSearcher() {
    try {
      return getSearcher(false,true,null);
    } catch (IOException e) {
      SolrException.log(log,null,e);
      return null;
    }
  }

  /**
   * Get a {@link SolrIndexSearcher} or start the process of creating a new one.
   * <p>
   * The registered searcher is the default searcher used to service queries.
   * A searcher will normally be registered after all of the warming
   * and event handlers (newSearcher or firstSearcher events) have run.
   * In the case where there is no registered searcher, the newly created searcher will
   * be registered before running the event handlers (a slow searcher is better than no searcher).
   *
   * <p>
   * If <tt>forceNew==true</tt> then
   *  A new searcher will be opened and registered irregardless if there is already
   *    a registered searcher or other searchers in the process of being created.
   * <p>
   * If <tt>forceNew==false</tt> then:<ul>
   *   <li>If a searcher is already registered, that searcher will be returned</li>
   *   <li>If no searcher is currently registered, but at least one is in the process of being created, then
   * this call will block until the first searcher is registered</li>
   *   <li>If no searcher is currently registered, and no searchers in the process of being registered, a new
   * searcher will be created.</li>
   * </ul>
   * <p>
   * If <tt>returnSearcher==true</tt> then a {@link RefCounted}&lt;{@link SolrIndexSearcher}&gt; will be returned with
   * the reference count incremented.  It <b>must</b> be decremented when no longer needed.
   * <p>
   * If <tt>waitSearcher!=null</tt> and a new {@link SolrIndexSearcher} was created,
   * then it is filled in with a Future that will return after the searcher is registered.  The Future may be set to
   * <tt>null</tt> in which case the SolrIndexSearcher created has already been registered at the time
   * this method returned.
   * <p>
   * @param forceNew           if true, force the open of a new index searcher regardless if there is already one open.
   * @param returnSearcher     if true, returns a {@link SolrIndexSearcher} holder with the refcount already incremented.
   * @param waitSearcher       if non-null, will be filled in with a {@link Future} that will return after the new searcher is registered.
   * @throws IOException
   */
  public RefCounted<SolrIndexSearcher> getSearcher(boolean forceNew, boolean returnSearcher, final Future[] waitSearcher) throws IOException {
    // it may take some time to open an index.... we may need to make
    // sure that two threads aren't trying to open one at the same time
    // if it isn't necessary.

    synchronized (searcherLock) {
      // see if we can return the current searcher
      if (_searcher!=null && !forceNew) {
        if (returnSearcher) {
          _searcher.incref();
          return _searcher;
        } else {
          return null;
        }
      }

      // check to see if we can wait for someone elses searcher to be set
      if (onDeckSearchers>0 && !forceNew && _searcher==null) {
        try {
          searcherLock.wait();
        } catch (InterruptedException e) {
          log.info(SolrException.toStr(e));
        }
      }

      // check again: see if we can return right now
      if (_searcher!=null && !forceNew) {
        if (returnSearcher) {
          _searcher.incref();
          return _searcher;
        } else {
          return null;
        }
      }

      // At this point, we know we need to open a new searcher...
      // first: increment count to signal other threads that we are
      //        opening a new searcher.
      onDeckSearchers++;
    }

    // open the index synchronously
    // if this fails, we need to decrement onDeckSearchers again.
    SolrIndexSearcher tmp;
    try {
      if (onDeckSearchers < 1) {
        // should never happen... just a sanity check
        log.severe("ERROR!!! onDeckSearchers is " + onDeckSearchers);
        // reset to 1 (don't bother synchronizing)
        onDeckSearchers=1;
      } else if (onDeckSearchers > 1) {
        log.info("PERFORMANCE WARNING: Overlapping onDeckSearchers=" + onDeckSearchers);
      }
      tmp = new SolrIndexSearcher(schema, "main", index_path, true);
    } catch (Throwable th) {
      synchronized(searcherLock) {
        onDeckSearchers--;
        // notify another waiter to continue... it may succeed
        // and wake any others.
        searcherLock.notify();
      }
      // need to close the searcher here??? we shouldn't have to.
      throw new RuntimeException(th);
    }

    final SolrIndexSearcher newSearcher=tmp;

    RefCounted<SolrIndexSearcher> currSearcherHolder=null;
    final RefCounted<SolrIndexSearcher> newSearchHolder=newHolder(newSearcher);
    if (returnSearcher) newSearchHolder.incref();

    // a signal to decrement onDeckSearchers if something goes wrong.
    final boolean[] decrementOnDeckCount=new boolean[1];
    decrementOnDeckCount[0]=true;

    try {

      boolean alreadyRegistered = false;
      synchronized (searcherLock) {
        if (_searcher == null) {
          // if there isn't a current searcher then we may
          // want to register this one before warming is complete instead of waiting.
          if (SolrConfig.config.getBool("query/useColdSearcher",false)) {
            registerSearcher(newSearchHolder);
            decrementOnDeckCount[0]=false;
            alreadyRegistered=true;
          }
        } else {
          // get a reference to the current searcher for purposes of autowarming.
          currSearcherHolder=_searcher;
          currSearcherHolder.incref();
        }
      }


      final SolrIndexSearcher currSearcher = currSearcherHolder==null ? null : currSearcherHolder.get();

      //
      // Note! if we registered the new searcher (but didn't increment it's
      // reference count because returnSearcher==false, it's possible for
      // someone else to register another searcher, and thus cause newSearcher
      // to close while we are warming.
      //
      // Should we protect against that by incrementing the reference count?
      // Maybe we should just let it fail?   After all, if returnSearcher==false
      // and newSearcher has been de-registered, what's the point of continuing?
      //

      Future future=null;

      // warm the new searcher based on the current searcher.
      // should this go before the other event handlers or after?
      if (currSearcher != null) {
        future = searcherExecutor.submit(
                new Callable() {
                  public Object call() throws Exception {
                    try {
                      newSearcher.warm(currSearcher);
                    } catch (Throwable e) {
                      SolrException.logOnce(log,null,e);
                    }
                    return null;
                  }
                }
        );
      }

      if (currSearcher==null && firstSearcherListeners.size() > 0) {
        future = searcherExecutor.submit(
                new Callable() {
                  public Object call() throws Exception {
                    try {
                      for (SolrEventListener listener : firstSearcherListeners) {
                        listener.newSearcher(newSearcher,null);
                      }
                    } catch (Throwable e) {
                      SolrException.logOnce(log,null,e);
                    }
                    return null;
                  }
                }
        );
      }

      if (currSearcher!=null && newSearcherListeners.size() > 0) {
        future = searcherExecutor.submit(
                new Callable() {
                  public Object call() throws Exception {
                    try {
                      for (SolrEventListener listener : newSearcherListeners) {
                        listener.newSearcher(newSearcher,null);
                      }
                    } catch (Throwable e) {
                      SolrException.logOnce(log,null,e);
                    }
                    return null;
                  }
                }
        );
      }

      // WARNING: this code assumes a single threaded executor (that all tasks
      // queued will finish first).
      final RefCounted<SolrIndexSearcher> currSearcherHolderF = currSearcherHolder;
      if (!alreadyRegistered) {
        future = searcherExecutor.submit(
                new Callable() {
                  public Object call() throws Exception {
                    try {
                      // signal that we no longer need to decrement
                      // the count *before* registering the searcher since
                      // registerSearcher will decrement even if it errors.
                      decrementOnDeckCount[0]=false;
                      registerSearcher(newSearchHolder);
                    } catch (Throwable e) {
                      SolrException.logOnce(log,null,e);
                    } finally {
                      // we are all done with the old searcher we used
                      // for warming...
                      if (currSearcherHolderF!=null) currSearcherHolderF.decref();
                    }
                    return null;
                  }
                }
        );
      }

      if (waitSearcher != null) {
        waitSearcher[0] = future;
      }

      // Return the searcher as the warming tasks run in parallel
      // callers may wait on the waitSearcher future returned.
      return returnSearcher ? newSearchHolder : null;

    }
    catch (Exception e) {
      SolrException.logOnce(log,null,e);
      if (currSearcherHolder != null) currSearcherHolder.decref();

      synchronized (searcherLock) {
        if (decrementOnDeckCount[0]) {
          onDeckSearchers--;
        }
        if (onDeckSearchers < 0) {
          // sanity check... should never happen
          log.severe("ERROR!!! onDeckSearchers after decrement=" + onDeckSearchers);
          onDeckSearchers=0; // try and recover
        }
        // if we failed, we need to wake up at least one waiter to continue the process
        searcherLock.notify();
      }

      // since the indexreader was already opened, assume we can continue on
      // even though we got an exception.
      return returnSearcher ? newSearchHolder : null;
    }

  }


  private RefCounted<SolrIndexSearcher> newHolder(SolrIndexSearcher newSearcher) {
    RefCounted<SolrIndexSearcher> holder = new RefCounted<SolrIndexSearcher>(newSearcher)
    {
      public void close() {
        try {
          resource.close();
        } catch (IOException e) {
          log.severe("Error closing searcher:" + SolrException.toStr(e));
        }
      }
    };
    holder.incref();  // set ref count to 1 to account for this._searcher
    return holder;
  }


  // Take control of newSearcherHolder (which should have a reference count of at
  // least 1 already.  If the caller wishes to use the newSearcherHolder directly
  // after registering it, then they should increment the reference count *before*
  // calling this method.
  //
  // onDeckSearchers will also be decremented (it should have been incremented
  // as a result of opening a new searcher).
  private void registerSearcher(RefCounted<SolrIndexSearcher> newSearcherHolder) throws IOException {
    synchronized (searcherLock) {
      try {
        if (_searcher != null) {
          _searcher.decref();   // dec refcount for this._searcher
          _searcher=null;
        }

        _searcher = newSearcherHolder;
        SolrIndexSearcher newSearcher = newSearcherHolder.get();

        newSearcher.register(); // register subitems (caches)
        log.info("Registered new searcher " + newSearcher);

      } catch (Throwable e) {
        log(e);
      } finally {
        // wake up anyone waiting for a searcher
        // even in the face of errors.
        onDeckSearchers--;
        searcherLock.notifyAll();
      }
    }
  }



  public void closeSearcher() {
    log.info("Closing main searcher on request.");
    synchronized (searcherLock) {
      if (_searcher != null) {
        _searcher.decref();   // dec refcount for this._searcher
        _searcher=null;
        SolrInfoRegistry.getRegistry().remove("currentSearcher");
      }
    }
  }



  public void execute(SolrQueryRequest req, SolrQueryResponse rsp) {
    SolrRequestHandler handler = getRequestHandler(req.getQueryType());
    if (handler==null) {
      log.warning("Unknown Request Handler '" + req.getQueryType() +"' :" + req);
      throw new SolrException(400,"Unknown Request Handler '" + req.getQueryType() + "'", true);
    }
    handler.handleRequest(req,rsp);
    log.info(req.getParamString()+ " 0 "+
	     (int)(rsp.getEndTime() - req.getStartTime()));
  }





  XmlPullParserFactory factory;
  {
    try {
      factory = XmlPullParserFactory.newInstance();
    } catch (XmlPullParserException e) {
      throw new RuntimeException(e);
    }
    factory.setNamespaceAware(false);
  }


  private int findNextTag(XmlPullParser xpp, String tag) throws XmlPullParserException, IOException {
    int eventType;
    while((eventType=xpp.next()) != XmlPullParser.END_DOCUMENT) {
      if(eventType == XmlPullParser.START_TAG) {
        if (tag.equals(xpp.getName())) break;
      }
    }
    return eventType;
  }


  public void update(Reader reader, Writer writer) {

    // TODO: add param to specify maximum time to commit?

    // todo - might be nice to separate command parsing w/ a factory
    // then new commands could be added w/o risk to old ones


    XmlPullParser xpp = null;
    try {
      xpp = factory.newPullParser();
    } catch (XmlPullParserException e) {
      throw new RuntimeException(e);
    }

    long startTime=System.currentTimeMillis();

    try {
      xpp.setInput(reader);
      xpp.nextTag();

      String currTag = xpp.getName();
      if ("add".equals(currTag)) {
        log.finest("SolrCore.update(add)");
        AddUpdateCommand cmd = new AddUpdateCommand();
        cmd.allowDups=false;  // the default

        int status=0;
        boolean pendingAttr=false, committedAttr=false;
        int attrcount = xpp.getAttributeCount();
        for (int i=0; i<attrcount; i++) {
          String attrName = xpp.getAttributeName(i);
          String attrVal = xpp.getAttributeValue(i);
          if ("allowDups".equals(attrName)) {
            cmd.allowDups = StrUtils.parseBoolean(attrVal);
          } else if ("overwritePending".equals(attrName)) {
            cmd.overwritePending = StrUtils.parseBoolean(attrVal);
            pendingAttr=true;
          } else if ("overwriteCommitted".equals(attrName)) {
            cmd.overwriteCommitted = StrUtils.parseBoolean(attrVal);
            committedAttr=true;
          } else {
            log.warning("Unknown attribute id in add:" + attrName);
          }
        }

        //set defaults for committed and pending based on allowDups value
        if (!pendingAttr) cmd.overwritePending=!cmd.allowDups;
        if (!committedAttr) cmd.overwriteCommitted=!cmd.allowDups;

        DocumentBuilder builder = new DocumentBuilder(schema);
        SchemaField uniqueKeyField = schema.getUniqueKeyField();
        int eventType=0;
        // accumulate responses
        List<String> added = new ArrayList<String>(10);
        while(true) {
          // this may be our second time through the loop in the case
          // that there are multiple docs in the add... so make sure that
          // objects can handle that.

          cmd.indexedId = null;  // reset the id for this add

          if (eventType !=0) {
            eventType=xpp.getEventType();
            if (eventType==XmlPullParser.END_DOCUMENT) break;
          }
          // eventType = xpp.next();
          eventType = xpp.nextTag();
          if (eventType == XmlPullParser.END_TAG || eventType == XmlPullParser.END_DOCUMENT) break;  // should match </add>

          readDoc(builder,xpp);
          builder.endDoc();
          cmd.doc = builder.getDoc();
          log.finest("adding doc...");
          updateHandler.addDoc(cmd);
          String docId = null;
          if (uniqueKeyField!=null)
            docId = schema.printableUniqueKey(cmd.doc);
          added.add(docId);
          
        } // end while
        // write log and result
        StringBuilder out = new StringBuilder();
        for (String docId: added)
          if(docId != null)
            out.append(docId + ",");
        String outMsg = out.toString();
        if(outMsg.length() > 0)
          outMsg = outMsg.substring(0, outMsg.length() - 1);
        log.info("added id={" + outMsg  + "} in " + (System.currentTimeMillis()-startTime) + "ms");
        writer.write("<result status=\"0\"></result>");

    } // end add

      else if ("commit".equals(currTag) || "optimize".equals(currTag)) {
        log.finest("parsing "+currTag);
        try {
          CommitUpdateCommand cmd = new CommitUpdateCommand("optimize".equals(currTag));

          boolean sawWaitSearcher=false, sawWaitFlush=false;
          int attrcount = xpp.getAttributeCount();
          for (int i=0; i<attrcount; i++) {
            String attrName = xpp.getAttributeName(i);
            String attrVal = xpp.getAttributeValue(i);
            if ("waitFlush".equals(attrName)) {
              cmd.waitFlush = StrUtils.parseBoolean(attrVal);
              sawWaitFlush=true;
            } else if ("waitSearcher".equals(attrName)) {
              cmd.waitSearcher = StrUtils.parseBoolean(attrVal);
              sawWaitSearcher=true;
            } else {
              log.warning("unexpected attribute commit/@" + attrName);
            }
          }

          // If waitFlush is specified and waitSearcher wasn't, then
          // clear waitSearcher.
          if (sawWaitFlush && !sawWaitSearcher) {
            cmd.waitSearcher=false;
          }

          updateHandler.commit(cmd);
          if ("optimize".equals(currTag)) {
            log.info("optimize 0 "+(System.currentTimeMillis()-startTime));
          }
          else {
            log.info("commit 0 "+(System.currentTimeMillis()-startTime));
          }
          while (true) {
            int eventType = xpp.nextTag();
            if (eventType == XmlPullParser.END_TAG) break; // match </commit>
          }
          writer.write("<result status=\"0\"></result>");
        } catch (SolrException e) {
          log(e);
          if ("optimize".equals(currTag)) {
            log.info("optimize "+e.code+" "+
                    (System.currentTimeMillis()-startTime));
          }
          else {
            log.info("commit "+e.code+" "+
                    (System.currentTimeMillis()-startTime));
          }
          writeResult(writer,e);
        } catch (Exception e) {
          SolrException.log(log, "Exception during commit/optimize",e);
          writeResult(writer,e);
        }
      }  // end commit

    else if ("delete".equals(currTag)) {
      log.finest("parsing delete");
      try {
        DeleteUpdateCommand cmd = new DeleteUpdateCommand();
        cmd.fromPending=true;
        cmd.fromCommitted=true;
        int attrcount = xpp.getAttributeCount();
        for (int i=0; i<attrcount; i++) {
          String attrName = xpp.getAttributeName(i);
          String attrVal = xpp.getAttributeValue(i);
          if ("fromPending".equals(attrName)) {
            cmd.fromPending = StrUtils.parseBoolean(attrVal);
          } else if ("fromCommitted".equals(attrName)) {
            cmd.fromCommitted = StrUtils.parseBoolean(attrVal);
          } else {
            log.warning("unexpected attribute delete/@" + attrName);
          }
        }

        int eventType = xpp.nextTag();
        currTag = xpp.getName();
        String val = xpp.nextText();

        if ("id".equals(currTag)) {
          cmd.id =  val;
          updateHandler.delete(cmd);
          log.info("delete(id " + val + ") 0 " +
                   (System.currentTimeMillis()-startTime));
        } else if ("query".equals(currTag)) {
          cmd.query =  val;
          updateHandler.deleteByQuery(cmd);
          log.info("deleteByQuery(query " + val + ") 0 " +
                   (System.currentTimeMillis()-startTime));
        } else {
          log.warning("unexpected XML tag /delete/"+currTag);
          throw new SolrException(400,"unexpected XML tag /delete/"+currTag);
        }

        writer.write("<result status=\"0\"></result>");

        while (xpp.nextTag() != XmlPullParser.END_TAG);

      } catch (SolrException e) {
        log(e);
        log.info("delete "+e.code+" "+(System.currentTimeMillis()-startTime));
        writeResult(writer,e);
      } catch (Exception e) {
        log(e);
        writeResult(writer,e);
      }
    } // end delete


    } catch (XmlPullParserException e) {
      log(e);
      writeResult(writer,e);
    } catch (IOException e) {
      log(e);
      writeResult(writer,e);
    } catch (SolrException e) {
      log(e);
      log.info("update "+e.code+" "+(System.currentTimeMillis()-startTime));
      writeResult(writer,e);
    } catch (Throwable e) {
      log(e);
      writeResult(writer,e);
    }

  }

  private void readDoc(DocumentBuilder builder, XmlPullParser xpp) throws IOException, XmlPullParserException {
    // xpp should be at <doc> at this point

    builder.startDoc();

    int attrcount = xpp.getAttributeCount();
    float docBoost = 1.0f;

    for (int i=0; i<attrcount; i++) {
      String attrName = xpp.getAttributeName(i);
      String attrVal = xpp.getAttributeValue(i);
      if ("boost".equals(attrName)) {
        docBoost = Float.parseFloat(attrVal);
        builder.setBoost(docBoost);
      } else {
        log.warning("Unknown attribute doc/@" + attrName);
      }
    }
    if (docBoost != 1.0f) builder.setBoost(docBoost);

    // while (findNextTag(xpp,"field") != XmlPullParser.END_DOCUMENT) {

    while(true) {
      int eventType = xpp.nextTag();
      if (eventType == XmlPullParser.END_TAG) break;  // </doc>

      String tname=xpp.getName();
      // System.out.println("FIELD READER AT TAG " + tname);

      if (!"field".equals(tname)) {
        log.warning("unexpected XML tag doc/"+tname);
        throw new SolrException(400,"unexpected XML tag doc/"+tname);
      }

      //
      // get field name and parse field attributes
      //
      attrcount = xpp.getAttributeCount();
      String name=null;
      float boost=1.0f;
      boolean isNull=false;

      for (int i=0; i<attrcount; i++) {
        String attrName = xpp.getAttributeName(i);
        String attrVal = xpp.getAttributeValue(i);
        if ("name".equals(attrName)) {
          name=attrVal;
        } else if ("boost".equals(attrName)) {
          boost=Float.parseFloat(attrVal);
        } else if ("null".equals(attrName)) {
          isNull=StrUtils.parseBoolean(attrVal);
        } else {
          log.warning("Unknown attribute doc/field/@" + attrName);
        }
      }

      // now get the field value
      String val = xpp.nextText();      // todo... text event for <field></field>???
                                        // need this line for isNull???
      // Don't add fields marked as null (for now at least)
      if (!isNull) {
        if (boost != 1.0f) {
          builder.addField(name,val,boost);
        } else {
          builder.addField(name,val);
        }
      }

      // do I have to do a nextTag here to read the end_tag?

    } // end field loop

  }


  final public static void log(Throwable e) {
    SolrException.logOnce(log,null,e);
  }


  final static void writeResult(Writer out, SolrException e) {
    try {
      XML.writeXML(out,"result",e.getMessage(),"status",e.code());
    } catch (Exception ee) {
      log.severe("Error writing to putput stream: "+ee);
    }
  }

  final static void writeResult(Writer out, Throwable e) {
    try {
      XML.writeXML(out,"result",SolrException.toStr(e),"status","1");
    } catch (Exception ee) {
      log.severe("Error writing to putput stream: "+ee);
    }
  }
  
  
  
  private QueryResponseWriter defaultResponseWriter;
  private final Map<String, QueryResponseWriter> responseWriters = new HashMap<String, QueryResponseWriter>();
  
  /** Configure the query response writers. There will always be a default writer; additional 
   * writers may also be configured. */
  private void initWriters() {
    String xpath = "queryResponseWriter";
    NodeList nodes = (NodeList) SolrConfig.config.evaluate(xpath, XPathConstants.NODESET);
    int length = nodes.getLength();
    for (int i=0; i<length; i++) {
      Element elm = (Element) nodes.item(i);
      
      try {
        String name = DOMUtil.getAttr(elm,"name", xpath+" config");
        String className = DOMUtil.getAttr(elm,"class", xpath+" config");
        log.info("adding queryResponseWriter "+name+"="+className);
          
        QueryResponseWriter writer = (QueryResponseWriter) Config.newInstance(className);
        writer.init(DOMUtil.childNodesToNamedList(elm));
        responseWriters.put(name, writer);
      } catch (Exception ex) {
        SolrException.logOnce(log,null, ex);
        // if a writer can't be created, skip it and continue
      }
    }

    // configure the default response writer; this one should never be null
    if (responseWriters.containsKey("standard")) {
      defaultResponseWriter = responseWriters.get("standard");
    }
    if (defaultResponseWriter == null) {
      defaultResponseWriter = new XMLResponseWriter();
    }

    // make JSON response writers available by default
    if (responseWriters.get("json")==null) {
      responseWriters.put("json", new JSONResponseWriter());
    }
    if (responseWriters.get("python")==null) {
      responseWriters.put("python", new PythonResponseWriter());
    }
    if (responseWriters.get("ruby")==null) {
      responseWriters.put("ruby", new RubyResponseWriter());
    }

  }
  
  /** Finds a writer by name, or returns the default writer if not found. */
  public final QueryResponseWriter getQueryResponseWriter(String writerName) {
    if (writerName != null) {
        QueryResponseWriter writer = responseWriters.get(writerName);
        if (writer != null) {
            return writer;
        }
    }
    return defaultResponseWriter;
  }

  /** Returns the appropriate writer for a request. If the request specifies a writer via the
   * 'wt' parameter, attempts to find that one; otherwise return the default writer.
   */
  public final QueryResponseWriter getQueryResponseWriter(SolrQueryRequest request) {
    return getQueryResponseWriter(request.getParam("wt")); 
  }
}





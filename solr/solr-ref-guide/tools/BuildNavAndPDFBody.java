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

import java.io.*;
import java.io.FilenameFilter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.json.*;

import org.asciidoctor.Asciidoctor.Factory;
import org.asciidoctor.Asciidoctor;
import org.asciidoctor.ast.DocumentHeader;


public class BuildNavAndPDFBody {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new RuntimeException("Wrong # of args: " + args.length);
    }

    final File adocDir = new File(args[0]);
    final String mainPageShortname = args[1];
    if (! adocDir.exists()) {
      throw new RuntimeException("asciidoc directory does not exist: " + adocDir.toString());
    }

    // build up a quick mapping of every known page
    System.out.println("Building up tree of all known pages");
    final Map<String,Page> allPages = new LinkedHashMap<String,Page>();
    Asciidoctor doctor = null;
    try {
      doctor = Factory.create();
      final File[] adocFiles = adocDir.listFiles(ADOC_FILE_NAMES);
      for (File file : adocFiles) {
        Page page = new Page(file, doctor.readDocumentHeader(file));
        if (allPages.containsKey(page.shortname)) {
          throw new RuntimeException("multiple pages with same shortname: " + page.file.toString() + " and " + allPages.get(page.shortname));
        }
        allPages.put(page.shortname, page);
      }
    } finally {
      if (null != doctor) {
        doctor.shutdown();
        doctor = null;
      }
    }

    // build up a hierarchical structure rooted at our mainPage
    final Page mainPage = allPages.get(mainPageShortname);
    if (null == mainPage) {
      throw new RuntimeException("no main-page found with shortname: " + mainPageShortname);
    }
    mainPage.buildKidsRecursive(allPages);

    // TODO: use depthFirstWalk to prune allPages to validate that we don't have any loops or orphan pages


    // Build up the PDF file,
    // while doing this also build up some next/prev maps for use in building the scrollnav
    File pdfFile = new File(new File(adocDir, "_data"), "pdf-main-body.adoc");
    if (pdfFile.exists()) {
      throw new RuntimeException(pdfFile.toString() + " already exists");
    }
    final Map<String,Page> nextPage = new HashMap<String,Page>();
    final Map<String,Page> prevPage = new HashMap<String,Page>();
    System.out.println("Creating " + pdfFile.toString());
    try (Writer w = new OutputStreamWriter(new FileOutputStream(pdfFile), "UTF-8")) {
      // Note: not worrying about headers or anything like that ...
      // expecting this file to just be included by the main PDF file.

      // track how deep we are so we can adjust headers accordingly
      // start with a "negative" depth to treat all "top level" pages as same depth as main-page using Math.max
      // (see below)
      final AtomicInteger depth = new AtomicInteger(-1);

      // the previous page seen in our walk
      AtomicReference<Page> previous = new AtomicReference<Page>();
      
      mainPage.depthFirstWalk(new Page.RecursiveAction() {
        public boolean act(Page page) {
          try {
            if (null != previous.get()) {
              // add previous as our 'prev' page, and ourselves as the 'next' of previous
              prevPage.put(page.shortname, previous.get());
              nextPage.put(previous.get().shortname, page);
            }
            previous.set(page);

            
            // HACK: where this file actually lives will determine what we need here...
            w.write("include::../");
            w.write(page.file.getName());
            w.write("[leveloffset=+"+Math.max(0, depth.intValue())+"]\n\n");
            depth.incrementAndGet();
            return true;
          } catch (IOException ioe) {
            throw new RuntimeException("IOE recursively acting on " + page.shortname, ioe);
          }
        }
        public void postKids(Page page) {
          depth.decrementAndGet();
        }
      });
    }
    
    // Build up the scrollnav file for jekyll's footer
    File scrollnavFile = new File(new File(adocDir, "_data"), "scrollnav.json");
    if (scrollnavFile.exists()) {
      throw new RuntimeException(scrollnavFile.toString() + " already exists");
    }
    System.out.println("Creating " + scrollnavFile.toString());
    try (Writer w = new OutputStreamWriter(new FileOutputStream(scrollnavFile), "UTF-8")) {
      JSONObject scrollnav = new JSONObject();
      for (Page p : allPages.values()) {
        JSONObject current = new JSONObject();
        Page prev = prevPage.get(p.shortname);
        Page next = nextPage.get(p.shortname);
        if (null != prev) {
          current.put("prev",
                      new JSONObject()
                      .put("url", prev.permalink)
                      .put("title", prev.title));
        }
        if (null != next) {
          current.put("next",
                      new JSONObject()
                      .put("url", next.permalink)
                      .put("title", next.title));
        }
        scrollnav.put(p.shortname, current);
      }
      // HACK: jekyll doesn't like escaped forward slashes in it's JSON?
      w.write(scrollnav.toString(2).replaceAll("\\\\/","/"));
    }
    
    // Build up the sidebar file for jekyll
    File sidebarFile = new File(new File(adocDir, "_data"), "sidebar.json");
    if (sidebarFile.exists()) {
      throw new RuntimeException(sidebarFile.toString() + " already exists");
    }
    System.out.println("Creating " + sidebarFile.toString());
    try (Writer w = new OutputStreamWriter(new FileOutputStream(sidebarFile), "UTF-8")) {
      // A stack for tracking what we're working on as we recurse
      final Stack<JSONObject> stack = new Stack<JSONObject>();
      
      mainPage.depthFirstWalk(new Page.RecursiveAction() {
        public boolean act(Page page) {
          final int depth = stack.size();
          if (4 < depth) {
            System.err.println("ERROR: depth==" + depth + " for " + page.permalink);
            System.err.println("sidebar.html template can not support pages this deep");
            System.exit(-1);
          }
          try {
            final JSONObject current = new JSONObject()
              .put("title",page.title)
              .put("url", page.permalink)
              .put("depth", depth)
              .put("kids", new JSONArray());
            
            if (0 < depth) {
              JSONObject parent = stack.peek();
              ((JSONArray)parent.get("kids")).put(current);
            }
            
            stack.push(current);
          } catch (JSONException e) {
            throw new RuntimeException(e);
          }
          return true;
        }
        public void postKids(Page page) {
          final JSONObject current = stack.pop();
          if (0 == stack.size()) {
            assert page == mainPage;
            try {
              // HACK: jekyll doesn't like escaped forward slashes in it's JSON?
              w.write(current.toString(2).replaceAll("\\\\/","/"));
            } catch (IOException | JSONException e) {
              throw new RuntimeException(e);
            }
          }
        }
      });
    }
    
  }

  /** Simple struct for modeling the key metadata for dealing with page navigation */
  public static final class Page {
    public final File file;
    public final String title;
    public final String shortname;
    public final String permalink;
    public final List<String> kidShortnames;
    /** NOTE: not populated on construction
     * @see #buildKidsRecursive
     */
    public final List<Page> kids;
    private final List<Page> mutableKids;
    public Page(File file, DocumentHeader header) {
      this.file = file;
      this.title = header.getDocumentTitle().getMain();
      
      // TODO: do error checking if attribute metadata we care about is missing
      Map<String,Object> attrs = header.getAttributes();
      this.shortname = (String) attrs.get("page-shortname");
      this.permalink = (String) attrs.get("page-permalink");
      
      if (attrs.containsKey("page-children")) {
        String kidsString = ((String) attrs.get("page-children")).trim();
        this.kidShortnames = Collections.<String>unmodifiableList
          (Arrays.asList(kidsString.split(",\\s+")));
        this.mutableKids = new ArrayList<Page>(kidShortnames.size());
      } else {
        this.kidShortnames = Collections.<String>emptyList();
        this.mutableKids = Collections.<Page>emptyList();
      }
      this.kids = Collections.<Page>unmodifiableList(mutableKids);
    }

    /** Recursively populates {@link #kids} from {@link #kidShortnames} via the <code>allPages</code> Map */
    public void buildKidsRecursive(Map<String,Page> allPages) {
      for (String kidShortname : kidShortnames) {
        Page kid = allPages.get(kidShortname);
        if (null == kid) {
          throw new RuntimeException("Unable to locate " + kidShortname + "; child of " + shortname + "("+file.toString());
        }
        mutableKids.add(kid);
        kid.buildKidsRecursive(allPages);
      }
    }

    /** 
     * Do a depth first recursive action on this node and it's {@link #kids} 
     * @see RecursiveAction
     */
    public void depthFirstWalk(RecursiveAction action) {
      if (action.act(this)) {
        for (Page kid : kids) {
          kid.depthFirstWalk(action);
        }
        action.postKids(this);
      }
    }

    /** @see #depthFirstWalk */
    public static interface RecursiveAction {
      /** return true if kids should also be visited */
      public boolean act(Page page);
      /** 
       * called after recusion to each kid (if any) of specified node, 
       * never called if {@link #act} returned false 
       */
      public default void postKids(Page page) { /* No-op */ };
    }
  }

  
  /** Trivial filter for only "*.adoc" files */
  public static final FilenameFilter ADOC_FILE_NAMES = new FilenameFilter() {
    public boolean accept(File dir, String name) {
      return name.endsWith(".adoc");
    }
  };
}

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


public class BuildNavDataFiles {

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
    // NOTE: mainPage claims to be its own parent to prevent anyone decendent from introducing a loop
    mainPage.buildPageTreeRecursive(mainPage, allPages);

    { // validate that there are no orphan pages
      int orphans = 0;
      for (Page p : allPages.values()) {
        if (null == p.getParent()) {
          orphans++;
          System.err.println("ERROR: Orphan page: " + p.file);
        }
      }
      if (0 != orphans) {
        throw new RuntimeException("Found " + orphans + " orphan pages (which are not in the 'page-children' attribute of any other pages)");
      }
    }

    // Loop over all files "in order" to build up next/prev maps for use in building the scrollnav
    final Map<String,Page> nextPage = new HashMap<String,Page>();
    final Map<String,Page> prevPage = new HashMap<String,Page>();
    System.out.println("Looping over pages to build nav data");

    { // the previous page seen during our walk
      AtomicReference<Page> previous = new AtomicReference<Page>();
      
      mainPage.depthFirstWalk(new Page.RecursiveAction() {
        public boolean act(Page page) {
          if (null != previous.get()) {
            // add previous as our 'prev' page, and ourselves as the 'next' of previous
            prevPage.put(page.shortname, previous.get());
            nextPage.put(previous.get().shortname, page);
          }
          previous.set(page);
          return true;
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
    public final String title; // NOTE: has html escape codes in it
    public final String shortname;
    public final String permalink;
    public final List<String> kidShortnames;
    /** NOTE: not populated on construction
     * @see #buildPageTreeRecursive
     */
    private Page parent;
    public Page getParent() {
      return parent;
    }
    /** NOTE: not populated on construction
     * @see #buildPageTreeRecursive
     */
    public final List<Page> kids;
    private final List<Page> mutableKids;
    public Page(File file, DocumentHeader header) {
      if (! file.getName().endsWith(".adoc")) {
        throw new RuntimeException(file + " has does not end in '.adoc' - this code can't be used");
      }
      
      this.file = file;
      this.title = header.getDocumentTitle().getMain();

      this.shortname = file.getName().replaceAll("\\.adoc$","");
      this.permalink = this.shortname + ".html";
      
      // TODO: do error checking if attribute metadata we care about is missing
      Map<String,Object> attrs = header.getAttributes();

      // See SOLR-11541: Fail if a user adds new docs with older missleading attributes we don't use/want
      for (String attr : Arrays.asList("page-shortname", "page-permalink")) {
        if (attrs.containsKey(attr)) {
          throw new RuntimeException(file + ": remove the " + attr + " attribute, it's no longer needed, and may confuse readers/editors");
        }
      }
      
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

    /** 
     * Recursively sets {@link #getParent} and populates {@link #kids} from {@link #kidShortnames} 
     * via the <code>allPages</code> Map 
     */
    public void buildPageTreeRecursive(Page parent, Map<String,Page> allPages) {
      if (null != parent) {
        if (null != this.parent) {
          // as long as we also check (later) that every page has a parent, this check (prior to recusion)
          // also ensures we never have any loops
          throw new RuntimeException(file.getName() + " is listed as the child of (at least) 2 pages: '" + parent.shortname + "' and '" + this.parent.shortname + "'");
        }
        this.parent = parent;
      }
      for (String kidShortname : kidShortnames) {
        Page kid = allPages.get(kidShortname);
        if (null == kid) {
          throw new RuntimeException("Unable to locate " + kidShortname + "; child of " + shortname + "("+file.toString());
        }
        mutableKids.add(kid);
        kid.buildPageTreeRecursive(this, allPages);
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

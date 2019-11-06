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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.parser.Parser;
import org.jsoup.parser.Tag;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeVisitor;

/**
 * Check various things regarding anchors, links &amp; general doc structure in the generated HTML site.
 *
 * <p>
 * Usage: <code>java CheckLinksAndAnchors some-html-dir-name/ [-check-all-relative-links] [-bare-bones]</code>

 * </p>
 * <p>
 * Problems this tool checks for...
 * </p>
 *
 * <ul>
 * <li>
 * Validates that no file contains the same anchor more then once.
 * </li>
 * <li>
 * Validates that relative links point to a file that actually exists, and if it's part of the ref-guide that the '#fragement' in the link refers to an ID that exists in that file.
 * </li>
 * <li>
 * Our use of "<a href="https://getbootstrap.com/">Bootstrap</a>" features leverage some custom javascript
 * for manipulating the DOM to keep the markup needed in the source <code>*.adoc</code> files simple, but it's
 * still possible users may create asciidctor "blocks" that break conventions (either in Bootstrap or in our
 * custom javascript)
 * </li>
 * </ul>
 *
 * <p>
 * This tool parses the generated HTML site, looking for these situations in order to fail the build, since
 * (depending on the type of check) these situations will result in inconsistent/broken HTML.
 * </p>
 * <p>
 * This tool supports 2 command line options:
 * </p>
 * <ul>
 *  <li><b>-check-all-relative-links</b><br />
 *    <p>By default, only relative links to files in the same directory (ie: not startin with
 *       <code>"../"</code> are checked for existence.  This means that we can do a "quick" validatation of
 *       links to other ref-guide files, but ignore relative links to things outside of the ref-guide --
 *       such as javadocs that we may not currently have built.  If this option is specified then we
 *       <em>also</em> check relative links where the path starts with <code>"../"</code>
 *    </p>
 *  </li>
 *  <li><b>-bare-bones</b><br/>
 *    <p>By default, this tool assumes it is analyzing Jekyll generated files.  If this option is specified,
 *       then it instead assumes it's checking "bare bones" HTML files...
 *    </p>
 *    <ul>
 *      <li>Jekyll Mode:
 *        <ul>
 *          <li>Requires all html pages have a "content" div; ignores all DOM Nodes that are
 *              <em>not</em> decendents of this div (to exclude redundent template based header, footer,
 *              &amp; sidebar links)
 *          </li>
 *          <li>Expects that the <code>&lt;body/&gt;</code> tag will have an <code>id</code> matching
 *              the page shortname.</li>
 *        </ul>
 *      </li>
 *      <li>Bare Bones Mode:
 *        <ul>
 *          <li>Checks all links &amp; anchors in the page.</li>
 *          <li>"Fakes" the existence of a <code>&lt;body id="..."&gt;</code> tag containing the
 *              page shortname.</li>
 *        </ul>
 *      </li>
 *    </ul>
 *  </li>
 * </ul>
 *
 * TODO: build a list of all known external links so that some other tool could (optionally) ping them all for 200 status?
 *
 * @see https://github.com/asciidoctor/asciidoctor/issues/1865
 * @see https://github.com/asciidoctor/asciidoctor/issues/1866
 */
public class CheckLinksAndAnchors { // TODO: rename this class now that it does more then just links & anchors

  public static final class HtmlFileFilter implements FileFilter {
    public boolean accept(File pathname) {
      return pathname.getName().toLowerCase().endsWith("html");
    }
  }

  public static void main(String[] args) throws Exception {
    int problems = 0;

    if (args.length < 1) {
      System.err.println("usage: CheckLinksAndAnchors <htmldir> [-check-all-relative-links] [-bare-bones]");
      System.exit(-1);
    }
    final File htmlDir = new File(args[0]);
    final Set<String> options = new LinkedHashSet<>();
    for (int i = 1; i < args.length; i++) {
      if (! args[i].trim().isEmpty()) { // ignore blank options - maybe an ant sysprop blanked on purpose
        options.add(args[i]);
      }
    }
    final boolean bareBones = options.remove("-bare-bones");
    final boolean checkAllRelativeLinks = options.remove("-check-all-relative-links");
    if (! options.isEmpty()) {
      for (String brokenOpt : options) {
        System.err.println("CheckLinksAndAnchors: Unrecognized option: " + brokenOpt);
      }
      System.exit(-1);
    }

    final File[] pages = htmlDir.listFiles(new HtmlFileFilter());
    if (0 == pages.length) {
      System.err.println("CheckLinksAndAnchors: No HTML Files found, wrong htmlDir? forgot to built the site?");
      System.exit(-1);
    }

    final Map<File,List<URI>> filesToRelativeLinks = new HashMap<>();
    final Map<String,Set<String>> filesToIds = new HashMap<>();

    int totalLinks = 0;
    int totalRelativeLinks = 0;
    int totalIds = 0;

    for (File file : pages) {
      //System.out.println("input File URI: " + file.toURI().toString());

      assert ! filesToRelativeLinks.containsKey(file);
      final List<URI> linksInThisFile = new ArrayList<URI>(17);
      filesToRelativeLinks.put(file, linksInThisFile);
      final Set<String> idsInThisFile = new LinkedHashSet<String>(17);
      filesToIds.put(file.getName(), idsInThisFile);

      // use this for error reporting if an ID exists multiple times in a single document
      final Map<String,List<Element>> idsToNodes = new HashMap<>();

      final String fileContents = readFile(file.getPath());
      final Document doc = Jsoup.parse(fileContents);

      // For Jekyll, we only care about class='content' -- we don't want to worry
      // about ids/links duplicated in the header/footer of every page,
      final String mainContentSelector = bareBones ? "body" : ".content";
      final Element mainContent = doc.select(mainContentSelector).first();
      if (mainContent == null) {
        throw new RuntimeException(file.getName() + " has no main content: " + mainContentSelector);
      }

      // All of the ID (nodes) in (the content of) this doc
      final Elements nodesWithIds = mainContent.select("[id]");
      if (bareBones) {
        // It's a pain in the ass to customize the HTML output structure asciidoctor's bare-bones html5 backend
        // so instead we "fake" that the body tag contains the attribute we use in jekyll
        nodesWithIds.add(new Element(Tag.valueOf("body"), "").attr("id", file.getName().replaceAll("\\.html$","")));
      } else {
        // We have to add Jekyll's <body> to the nodesWithIds so we check the main section anchor as well
        // since we've already
        nodesWithIds.addAll(doc.select("body[id]"));
      }

      boolean foundPreamble = false;
      for (Element node : nodesWithIds) {
        final String id = node.id();
        assert null != id;
        assert 0 != id.length();

        // special case id: we ignore the first 'preamble' because
        // it's part of the core markup that asciidoctor always uses
        // if we find it a second time in a single page, fail with a special error...
        if (id.equals("preamble")) {
          if (foundPreamble) {
            problems++;
            System.err.println(file.toURI().toString() +
                               " contains 'preamble' anchor, this is special in jekyll and must not be used in content.");
          } else {
            foundPreamble = true;
            continue; // Note: we specifically don't count this in totalIds
          }
        }

        if (idsInThisFile.contains(id)) {
          problems++;
          System.err.println(file.toURI().toString() + " contains ID multiple times: " + id);
        }
        idsInThisFile.add(id);
        totalIds++; // Note: we specifically don't count 'preamble'
      }

      // check for (relative) links that don't include a fragment
      final Elements links = mainContent.select("a[href]");
      for (Element link : links) {
        totalLinks++;
        final String href = link.attr("href");
        if (0 == href.length()) {
          problems++;
          System.err.println(file.toURI().toString() + " contains link with empty href");
        }
        try {
          final URI uri = new URI(href);
          if (! uri.isAbsolute()) {
            totalRelativeLinks++;
            final String frag = uri.getFragment();
            if ((null == frag || "".equals(frag)) && ! uri.getPath().startsWith("../")) {
              // we must have a fragment for intra-page links to work correctly
              // but relative links "up and out" of ref-guide (Ex: local javadocs)
              // don't require them (even if checkAllRelativeLinks is set)
              problems++;
              System.err.println(file.toURI().toString() + " contains relative link w/o an '#anchor': " + href);
            } else {
              // track the link to validate it exists in the target doc
              linksInThisFile.add(uri);
            }
          }
        } catch (URISyntaxException uri_ex) {
          // before reporting a problem, see if it can be parsed as a valid (absolute) URL
          // some solr examples URLs have characters that aren't legal URI characters
          // Example: "ipod^3.0", "foo:[*+TO+*]", etc...
          boolean href_is_valid_absolute_url = false;
          try {
            // if this isn't absolute, it will fail
            final URL ignored = new URL(href);
            href_is_valid_absolute_url = true;
          } catch (MalformedURLException url_ex) {
            problems++;
            System.err.println(file.toURI().toString() + " contains link w/ invalid syntax: " + href);
            System.err.println(" ... as URI: " + uri_ex.toString());
            System.err.println(" ... as URL: " + url_ex.toString());
          }
        }
      }

      problems += validateHtmlStructure(file, mainContent);
    }

    // check every (realtive) link in every file to ensure the frag exists in the target page
    for (Map.Entry<File,List<URI>> entry : filesToRelativeLinks.entrySet()) {
      final File source = entry.getKey();
      for (URI link : entry.getValue()) {
        final String path = (null == link.getPath() || "".equals(link.getPath())) ? source.getName() : link.getPath();
        final File dest = new File(htmlDir, path);
        if ( ! dest.exists() ) {
          // this is only a problem if it's in our dir, or checkAllRelativeLinks is set...
          if (checkAllRelativeLinks || ! path.startsWith("../")) {
            problems++;
            System.err.println("Relative link points at dest file that doesn't exist: " + link);
            System.err.println(" ... source: " + source.toURI().toString());
          }
        } else {
          if ( ! path.startsWith("../") ) {
            // if the dest file is part of the ref guide (ie: not an "up and out" link to javadocs)
            // then we validate the fragment is known and exists in that file...
            final String frag = link.getFragment();
            final Set<String> knownIdsInDest = filesToIds.get(dest.getName());
            assert null != knownIdsInDest : dest.getName();
            if (! knownIdsInDest.contains(frag) ) {
              problems++;
              System.err.println("Relative link points at id that doesn't exist in dest: " + link);
              System.err.println(" ... source: " + source.toURI().toString());
            }
          }
        }
      }
    }

    System.err.println("Processed " + totalLinks + " links (" + totalRelativeLinks + " relative) to " +
                       totalIds + " anchors in " + pages.length + " files");
    if (0 < problems) {
      System.err.println("Total of " + problems + " problems found");
      System.exit(-1);
    }
  }

  static String readFile(String fileName) throws IOException {
    InputStream in = new FileInputStream(fileName);
    Reader reader = new InputStreamReader(in,"UTF-8");
    BufferedReader br = new BufferedReader(reader);
    try {
      StringBuilder sb = new StringBuilder();
      String line = br.readLine();
      while (line != null) {
        sb.append(line);
        sb.append("\n");
        line = br.readLine();
      }
      return sb.toString();
    } finally {
      br.close();
    }
  }

  /**
   * returns the number of problems found with this file
   */
  private static int validateHtmlStructure(final File f, final Element mainContent) {
    final String file = f.toURI().toString();
    int problems = 0;

    for (Element tab : mainContent.select(".dynamic-tabs")) {
      // must be at least two tab-pane decendents of each dynamic-tabs
      final Elements panes = tab.select(".tab-pane");
      final int numPanes = panes.size();
      if (numPanes < 2) {
        System.err.println(file + " contains a 'dynamic-tabs' with "+ numPanes+" 'tab-pane' decendents -- must be at least 2");
        problems++;
      }

      // must not have any decendents of a dynamic-tabs that are not part of tab-pane
      //
      // this is kind of tricky, because asciidoctor creates wrapper divs around the tab-panes
      // so we can't make assumptions about direct children
      //
      final Elements elementsToIgnore = panes.parents();
      for (Element pane : panes) {
        elementsToIgnore.addAll(pane.select("*"));
      }
      final Elements nonPaneDecendents = tab.select("*");
      nonPaneDecendents.removeAll(elementsToIgnore);
      if (0 != nonPaneDecendents.size()) {
        System.err.println(file + " contains a 'dynamic-tabs' with content outside of a 'tab-pane': " +
                           shortStr(nonPaneDecendents.text()));
        problems++;
      }
    }

    // Now fetch all tab-panes, even if they aren't in a dynamic-tabs instance
    // (that's a type of error we want to check for)
    final Elements validPanes = mainContent.select(".dynamic-tabs .tab-pane");
    final Elements allPanes = mainContent.select(".tab-pane");

    for (Element pane : allPanes) {
      // every tab-pane must have an id
      if (pane.id().trim().isEmpty()) {
        System.err.println(file + " contains a 'tab-pane' that does not have a (unique) '#id'");
        problems++;
      }
      final String debug = "'tab-pane" + (pane.id().isEmpty() ? "" : "#" + pane.id()) + "'";

      // no 'active' class on any tab-pane
      if (pane.classNames().contains("active")) {
        System.err.println(file + " contains " + debug + " with 'active' defined -- this must be removed");
        problems++;
      }

      // every tab-pane must be a decendent of a dynamic-tabs
      if (! validPanes.contains(pane)) {
        System.err.println(file + " contains " + debug + " that is not a decendent of a 'dynamic-tabs'");
        problems++;
      }

      // every tab-pane must have exactly 1 tab-label which is <strong>
      Elements labels = pane.select(".tab-label");
      if (1 != labels.size()) {
        System.err.println(file + " contains " + debug + " with " + labels.size() + " 'tab-label' decendents -- must be exactly 1");
        problems++;
      } else {
        Element label = labels.first();
        if (! label.tagName().equals("strong")) {
          System.err.println(file + " contains " + debug + " with a 'tab-label' using <"
                             + labels.first().tagName() + "> -- each 'tab-label' must be <strong> (example: '[.tab-label]*Text*')");
          problems++;
        }
        final String labelText = label.text().trim();
        // if the tab-label is the empty string, asciidoctor should optimize it away -- but let's check for it anyway
        if (labelText.isEmpty()) {
          System.err.println(file + " contains " + debug + " with a blank 'tab-label'");
          problems++;
        }
        // validate label must be first paragraph? first text content?
        if (! pane.text().trim().startsWith(labelText)) {
          System.err.println(file + " contains " + debug + " with text before the 'tab-label' ('" + labelText + "')");
          problems++;
        }

      }

    }

    return problems;
  }

  public static final String shortStr(String s) {
    if (s.length() < 20) {
      return s;
    }
    return s.substring(0, 17) + "...";
  }

}

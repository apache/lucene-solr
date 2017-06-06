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
import java.util.HashSet;
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
 * Check various things regarding links in the generated HTML site.
 * <p>
 * Asciidoctor doesn't do a good job of rectifying situations where multiple documents are included in one
 * massive (PDF) document may have identical anchors (either explicitly defined, or implicitly defined because of 
 * section headings).  Asciidoctor also doesn't support linking directly to another (included) document by name, 
 * unless there is an explicit '#fragement' used inthe link.
 * </p>
 * <p>
 * This tool parses the generated HTML site, looking for these situations in order to fail the build -- since the 
 * equivilent PDF will be broken.  It also does sme general check of the relative URLs to ensure the destination 
 * files/anchors actaully exist.
 * </p>
 * 
 * TODO: build a list of all known external links so that some other tool could (optionally) ping them all for 200 status?
 *
 * @see https://github.com/asciidoctor/asciidoctor/issues/1865
 * @see https://github.com/asciidoctor/asciidoctor/issues/1866
 */
public class CheckLinksAndAnchors {

  public static final class HtmlFileFilter implements FileFilter {
    public boolean accept(File pathname) {
      return pathname.getName().toLowerCase().endsWith("html");
    }
  }
  
  public static void main(String[] args) throws Exception {
    int problems = 0;
    
    if (args.length != 1) {
      System.err.println("usage: CheckLinksAndAnchors <htmldir>");
      System.exit(-1);
    }
    final File htmlDir = new File(args[0]);
    
    final File[] pages = htmlDir.listFiles(new HtmlFileFilter());
    if (0 == pages.length) {
      System.err.println("No HTML Files found, wrong htmlDir? forgot to built the site?");
      System.exit(-1);
    }

    final Map<String,List<File>> idsToFiles = new HashMap<>();
    final Map<File,List<URI>> filesToRelativeLinks = new HashMap<>();
    final Set<String> idsInMultiFiles = new HashSet<>(0);
    
    for (File file : pages) {
      //System.out.println("input File URI: " + file.toURI().toString());

      assert ! filesToRelativeLinks.containsKey(file);
      final List<URI> linksInThisFile = new ArrayList<URI>(17);
      filesToRelativeLinks.put(file, linksInThisFile);
      
      final String fileContents = readFile(file.getPath());
      final Document doc = Jsoup.parse(fileContents);
      // we only care about class='main-content' -- we don't want to worry
      // about ids/links duplicated in the header/footer of every page,
      final Element mainContent = doc.select(".main-content").first();
      if (mainContent == null) {
        throw new RuntimeException(file.getName() + " has no main-content div");
      }

      // Add all of the IDs in (the main-content of) this doc to idsToFiles (and idsInMultiFiles if needed)
      final Elements nodesWithIds = mainContent.select("[id]");
      // NOTE: add <body> to the nodesWithIds so we check the main section anchor as well
      nodesWithIds.addAll(doc.select("body[id]"));
      for (Element node : nodesWithIds) {
        final String id = node.id();
        assert null != id;
        assert 0 != id.length();

        // special case ids that we ignore
        if (id.equals("preamble")) {
          continue;
        }
        
        if (idsToFiles.containsKey(id)) {
          idsInMultiFiles.add(id);
        } else {
          idsToFiles.put(id, new ArrayList<File>(1));
        }
        idsToFiles.get(id).add(file);
      }

      // check for (relative) links that don't include a fragment
      final Elements links = mainContent.select("a[href]");
      for (Element link : links) {
        final String href = link.attr("href");
        if (0 == href.length()) {
          problems++;
          System.err.println(file.toURI().toString() + " contains link with empty href");
        }
        try {
          final URI uri = new URI(href);
          if (! uri.isAbsolute()) {
            final String frag = uri.getFragment();
            if (null == frag || "".equals(frag)) {
              // we must have a fragment for intra-page links to work correctly
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
    }

    // if there are problematic ids, report them
    for (String id : idsInMultiFiles) {
      problems++;
      System.err.println("ID occurs multiple times: " + id);
      for (File file : idsToFiles.get(id)) {
        System.err.println(" ... " + file.toURI().toString());
      }
    }

    // check every (realtive) link in every file to ensure the frag exists in the target page
    for (Map.Entry<File,List<URI>> entry : filesToRelativeLinks.entrySet()) {
      final File source = entry.getKey();
      for (URI link : entry.getValue()) {
        final String path = (null == link.getPath() || "".equals(link.getPath())) ? source.getName() : link.getPath();
        final String frag = link.getFragment();
        if ( ! idsInMultiFiles.contains(frag) ) { // skip problematic dups already reported
          final File dest = new File(htmlDir, path);
          if ( ! dest.exists() ) {
            problems++;
            System.err.println("Relative link points at dest file that doesn't exist: " + link);
            System.err.println(" ... source: " + source.toURI().toString());
          } else if ( ( ! idsToFiles.containsKey(frag) ) || // no file contains this id, or...
                      // id exists, but not in linked file
                      ( ! idsToFiles.get(frag).get(0).getName().equals(path) )) { 
            problems++;
            System.err.println("Relative link points at id that doesn't exist in dest: " + link);
            System.err.println(" ... source: " + source.toURI().toString());
          }
        }
      }
    }

    
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
  
}


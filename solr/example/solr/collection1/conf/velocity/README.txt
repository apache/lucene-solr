Introduction
------------
Solr Search Velocity Templates

A quick demo of using Solr using http://wiki.apache.org/solr/VelocityResponseWriter

You typically access these templates via:
	http://localhost:8983/solr/collection1/browse

It's called "browse" because you can click around with your mouse
without needing to type any search terms.  And of course it
also works as a standard search app as well.

Known Limitations
-----------------
* The /browse and the VelocityResponseWriter component
  serve content directly from Solr, which usually requires
  Solr's HTTP API to be exposed.  Advanced users could
  potentially access other parts of Solr directly.
* There are some hard coded fields in these templates.
  Since these templates live under conf, they should be
  considered part of the overall configuration, and
  must be coordinated with schema.xml and solrconfig.xml

Velocity Info
-------------
Java-based template language.

It's nice in this context because change to the templates
are immediately visible in browser on the next visit.

Links:
	http://velocity.apache.org
	http://wiki.apache.org/velocity/
	http://velocity.apache.org/engine/releases/velocity-1.7/user-guide.html


File List
---------

System and Misc:
  VM_global_library.vm    - Macros used other templates,
                            exact filename is important for Velocity to see it
  error.vm                - shows errors, if any
  debug.vm                - includes toggle links for "explain" and "all fields"
                            activated by debug link in footer.vm
  README.txt              - this file

Overall Page Composition:
  browse.vm               - Main entry point into templates
  layout.vm               - overall HTML page layout
  head.vm                 - elements in the <head> section of the HTML document
  header.vm               - top section of page visible to users
  footer.vm               - bottom section of page visible to users,
                            includes debug and help links
  main.css                - CSS style for overall pages
                            see also jquery.autocomplete.css

Query Form and Options:
  query_form.vm           - renders query form
  query_group.vm          - group by fields
                            e.g.: Manufacturer or Poplularity
  query_spatial.vm        - select box for location based Geospacial search

Spelling Suggestions:
  did_you_mean.vm         - hyperlinked spelling suggestions in results
  suggest.vm              - dynamic spelling suggestions
                            as you type in the search form
  jquery.autocomplete.js  - supporting files for dynamic suggestions
  jquery.autocomplete.css - Most CSS is defined in main.css


Search Results, General:
  (see also browse.vm)
  tabs.vm                 - provides navigation to advanced search options
  pagination_top.vm       - paging and staticis at top of results
  pagination_bottom.vm    - paging and staticis at bottom of results
  results_list.vm
  hit.vm                  - called for each matching doc,
                            decides which template to use
  hit_grouped.vm          - display results grouped by field values
  product_doc.vm          - display a Product
  join_doc.vm             - display a joined document
  richtext_doc.vm         - display a complex/misc. document
  hit_plain.vm            - basic display of all fields,
                            edit results_list.vm to enable this


Search Results, Facets & Clusters:
  facets.vm               - calls the 4 facet and 1 cluster template
  facet_fields.vm         - display facets based on field values
                            e.g.: fields specified by &facet.field=
  facet_queries.vm        - display facets based on specific facet queries
                            e.g.: facets specified by &facet.query=
  facet_ranges.vm         - display facets based on ranges
                            e.g.: ranges specified by &facet.range=
  facet_pivot.vm          - display pivot based facets
                            e.g.: facets specified by &facet.pivot=
  cluster.vm              - if clustering is available
                            then call cluster_results.vm
  cluster_results.vm      - actual rendering of clusters

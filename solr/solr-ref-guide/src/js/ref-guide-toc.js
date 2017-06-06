// see toc.js and toc.html

// NOTE: toc.html is only include if :page-toc: is true, so it's safe to do this document.ready unconditionally
function do_tocs(page_toc_levels) {
  $( document ).ready(function() {

    // headers used - start at 2, and we have to be careful about trailing comma
    toc_headers = 0 < page_toc_levels ? 'h2' : '';

    for (i = 1; i < page_toc_levels; i++) {
      toc_headers += ",h" + (2 + i);
    }
    // top level TOC
    $('#toc').toc({ minimumHeaders: 2, listType: 'ul', showSpeed: 0, headers: toc_headers });

    // any subsection TOCs
    $('.section-toc').each(function() {
      // toc() needs a selector string, so we'll build one by fetching the id of the nearest header
      // that comes before us, then use to make a selector for all sub headers in the same section
      // NOTE: this depends a lot of the particular structure of HTML asciidoctor generates
      header = $(this).closest("div:has(:header:first-child)").children(":header").first();
      selector = "#" + header.attr("id") + " ~ * :header";
      $(this).toc({ minimumHeaders: 2, listType: 'ul', showSpeed: 0, headers: selector });
    });

    /* this offset helps account for the space taken up by the floating toolbar. */
    $('.toc').on('click', 'a', function() {
      var target = $(this.getAttribute('href'))
          , scroll_target = target.offset().top

      $(window).scrollTop(scroll_target - 10);
      return false
    })

  });
}


$('#mysidebar').height($(".nav").height());


$( document ).ready(function() {

  //this script says, if the height of the viewport is greater than 800px, then insert affix class, which makes the nav bar float in a fixed
  // position as your scroll. if you have a lot of nav items, this height may not work for you.
  // commented out...to add back, uncomment the next line, then the 3 after the "console.log" comment
  //var h = $(window).height();
  //console.log (h);
  //if (h > 800) {
  //     $( "#mysidebar" ).attr("class", "nav affix");
  // }
  // activate tooltips. although this is a bootstrap js function, it must be activated this way in your theme.
  $('[data-toggle="tooltip"]').tooltip({
    placement : 'top'
  });

  /**
   * AnchorJS
   */
  anchors.add('h2,h3,h4,h5');

  // "Bootstrap Bootstrap"
  // NOTE: by default, we use "dynamic-tabs" in our wrapper instead of "tab-content"
  // so that if javascript is disabled, bootstrap's CSS doesn't hide all non-active "tab-pane" divs
  $(".dynamic-tabs").each(function(ignored) {
    $(this).addClass("tab-content");
    var nav_ul = $("<ul>", { "class": "nav nav-pills" });
    $(".tab-pane", this).each(function(tab_index) {
      var pill_li = $("<li>");
      // force the first tab to always be the active tab
      if (0 == tab_index) {
        $(this).addClass("active");
        pill_li.addClass("active");
      } else {
        // our validator should have prevented this, but remove them just in case
        $(this).removeClass("active");
      }

      var pill_a = $("<a>", { "data-toggle" : "pill" } );
      if ($(this)[0].hasAttribute("id")) {
        pill_a.attr("href", "#" + $(this).attr("id"));
      } else {
        // our validator will complain if a tab-pane has no id, but if the user
        // views the resultin HTML, draw attention to it...
        pill_a.append( " BAD TAB-PANE HAS NO ID ");
      }

      var label = $(".tab-label", this);
      if (0==label.length) {
        // our validator will complain if a tab-pane has no tab-label, but if the user
        // views the resultin HTML, draw attention to it...
        pill_a.append( " BAD TAB-PANE HAS NO TAB-LABEL ");
      } else {
        // NOTE: using the "inner" HTML of the label...
        // so by default we can use "bold" (or whatever) in our asciidoc and have that
        // be what people see when javascript is disabled,
        // but when the pills+tabs get active, the pills won't all be bold (or whatever)
        pill_a.append( label.html() );
        // NOTE: Removing the label isn't strictly neccessary, but makes the pills/tabs less redundent
        label.remove();
      }

      pill_li.append(pill_a);
      nav_ul.append(pill_li);
    });
    $(this).before(nav_ul);
  });

});

// needed for nav tabs on pages. See Formatting > Nav tabs for more details.
// script from http://stackoverflow.com/questions/10523433/how-do-i-keep-the-current-tab-active-with-twitter-bootstrap-after-a-page-reload
$(function() {
  var json, tabsState;
  $('a[data-toggle="pill"], a[data-toggle="tab"]').on('shown.bs.tab', function(e) {
    var href, json, parentId, tabsState;

    tabsState = localStorage.getItem("tabs-state");
    json = JSON.parse(tabsState || "{}");
    parentId = $(e.target).parents("ul.nav.nav-pills, ul.nav.nav-tabs").attr("id");
    href = $(e.target).attr('href');
    json[parentId] = href;

    return localStorage.setItem("tabs-state", JSON.stringify(json));
  });

  tabsState = localStorage.getItem("tabs-state");
  json = JSON.parse(tabsState || "{}");

  $.each(json, function(containerId, href) {
    return $("#" + containerId + " a[href=" + href + "]").tab('show');
  });

  $("ul.nav.nav-pills, ul.nav.nav-tabs").each(function() {
    var $this = $(this);
    if (!json[$this.attr("id")]) {
      return $this.find("a[data-toggle=tab]:first, a[data-toggle=pill]:first").tab("show");
    }
  });
});

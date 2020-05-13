
//$('#sidebar').height($(".nav").height());


$( document ).ready(function() {

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
      var pill_li = $("<li>", { "class": "nav-item" });
      var pill_a = $("<a>", { "class": "nav-link", "role": "tab", "data-toggle": "pill" } );

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

      // force the first tab to always be the active tab
      if (0 == tab_index) {
        $(this).addClass("active");
        pill_a.addClass("active");
      } else {
        // our validator should have prevented this, but remove them just in case
        $(this).removeClass("active");
      }

      pill_li.append(pill_a);
      nav_ul.append(pill_li);
    });
    $(this).before(nav_ul);
  });

});

// Adds scrollbar for the sidebar nav
$(document).ready(function () {

    $("#sidebar").mCustomScrollbar({
         theme: "minimal"
    });

});

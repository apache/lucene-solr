#!/usr/bin/perl
#
# Transforms Lucene Core's or Solr's CHANGES.txt into Changes.html
#
# Input is on STDIN, output is to STDOUT
#
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

use strict;
use warnings;

my $jira_url_prefix = 'http://issues.apache.org/jira/browse/';
my $github_pull_request_prefix = 'https://github.com/apache/lucene-solr/pull/';
my $month_regex = &setup_month_regex;
my %month_nums = &setup_month_nums;
my %lucene_bugzilla_jira_map = &setup_lucene_bugzilla_jira_map;
my $title = undef;
my $release = undef;
my $reldate = undef;
my $relinfo = undef;
my $sections = undef;
my $items = undef;
my $first_relid = undef;
my $second_relid = undef;
my @releases = ();

my @lines = <STDIN>;                        # Get all input at once

#
# Cmdline args:  <LUCENE|SOLR>  <project-DOAP-rdf-file>  <lucene-javadoc-url>(only from Solr)
#
my $product = uc($ARGV[0]);
if ($product !~ /^(LUCENE|SOLR)$/) {
  print STDERR "Unknown product name '$ARGV[0]'\n";
  exit(1);
}
my %release_dates = &setup_release_dates($ARGV[1]);
my $lucene_javadoc_url = ($product eq 'SOLR' ? $ARGV[2] : ''); # Only Solr supplies this on the cmdline
my $in_major_component_versions_section = 0;


#
# Parse input and build hierarchical release structure in @releases
#
for (my $line_num = 0 ; $line_num <= $#lines ; ++$line_num) {
  $_ = $lines[$line_num];
  unless (/\S/) {                      # Skip blank lines
    $in_major_component_versions_section = 0;
    next;
  }
  next if (/^\s{0,4}-{5,}\s*$/);       # Skip Solr's section underlines

  unless ($title) {
    if (/\S/) {
      s/^[^\p{N}\p{L}]*//;             # Trim leading non-alphanum chars, including BOM chars, if any
      s/\s+$//;                        # Trim trailing whitespace
    }
    $title = $_;
    next;
  }

  if (/\s*===+\s*(.*?)\s*===+\s*/) {   # New-style release headings
    $release = $1;
    $release =~ s/^(?:release|lucene)\s*//i;  # Trim "Release " or "Lucene " prefix
    ($release, $relinfo) = ($release =~ /^(\d+(?:\.(?:\d+))*(?:-(?:ALPHA|BETA))?|Trunk)\s*(.*)/i);
    $relinfo =~ s/\s*:\s*$//;          # Trim trailing colon
    $relinfo =~ s/^\s*,\s*//;          # Trim leading comma
    ($reldate, $relinfo) = get_release_date($release, $relinfo);
    $sections = [];
    push @releases, [ $release, $reldate, $relinfo, $sections ];
    ($first_relid = 'v'.lc($release)) =~ s/\s+/_/g
       if ($#releases == 0 or ($#releases == 1 and not ($releases[0][0])));
    ($second_relid = 'v'.lc($release)) =~ s/\s+/_/g
       if (   ($#releases == 1 and $releases[0][0])
           or ($#releases == 2 and not $releases[0][0]));
    $items = undef;
    next;
  }

  if (/^\s*([01](?:\.[0-9]{1,2}){1,2}[a-z]?(?:\s*(?:RC\d+|final))?)\s*
       ((?:200[0-7]-.*|.*,.*200[0-7].*)?)$/x) { # Old-style release heading
    $release = $1;
    $relinfo = $2;
    $relinfo =~ s/\s*:\s*$//;          # Trim trailing colon
    $relinfo =~ s/^\s*,\s*//;          # Trim leading comma
    ($reldate, $relinfo) = get_release_date($release, $relinfo);
    $sections = [];
    push @releases, [ $release, $reldate, $relinfo, $sections ];
    $items = undef;
    next;
  }

  if (m!^20\d\d[-/]\d{1,2}[-/]\d{1,2}!) { # Collect dated postscripts
    my $item = $_;
    my $line = '';
    while ($line_num < $#lines and ($line = $lines[++$line_num]) =~ /\S/) {
      $line =~ s/^\s+//;                   # Trim leading whitespace
      $line =~ s/\s+$//;                   # Trim trailing whitespace
      $item .= "$line\n";
    }
    push @releases, [ $item, '', '', [] ];
    next;
  }

  # Section heading: no leading whitespace, initial word capitalized,
  #                  six words or less, and no trailing punctuation,
  #                  except colons - don't match the one otherwise matching
  #                  non-section-name by excluding "StandardTokenizer"
  if (    /^(?!.*StandardTokenizer)([A-Z]\S*(?:\s+\S+){0,5})(?<![-.;!()])\s*$/
      and not $in_major_component_versions_section) {
    my $heading = $1;
    $heading =~ s/:$//; # Strip trailing colon, if any
    $items = [];
    unless (@releases) {
      $sections = [];
      # Make a fake release to hold pre-release sections
      push @releases, [ undef, undef, undef, $sections ];
    }
    for my $section (@$sections) {
      if ($heading eq $section->[0]) {
        die "Section '$heading' appears more than once under release '$releases[-1][0]'";
      }
    }
    push @$sections, [ $heading, $items ];
    $in_major_component_versions_section
      = ($heading =~ /Versions of Major Components/i);
    next;
  }

  # Handle earlier releases without sections - create a headless section
  unless ($items) {
    $items = [];
    unless (@releases) {
      $sections = [];
      # Make a fake release to hold pre-release sections and items
      push @releases, [ undef, undef, undef, $sections ];
    }
    push @$sections, [ '', $items ];
  }

  my $type;
  if (@$items) { # A list item has been encountered in this section before
    $type = $items->[0];  # 0th position of items array is list type
  } else {
    $type = get_list_type($_);
    push @$items, $type;
  }

  if ($type eq 'numbered') { # The modern items list style
    # List item boundary is another numbered item or an unindented line
    my $line;
    my $item = $_;
    $item =~ s/^(\s{0,2}\d+\.\d?\s*)//;    # Trim the leading item number
    my $leading_ws_width = length($1);
    $item =~ s/\s+$//;                     # Trim trailing whitespace
    $item .= "\n";

    while ($line_num < $#lines
           and ($line = $lines[++$line_num]) !~ /^(?:\s{0,2}\d+\.\s*\S|\S)/) {
      $line =~ s/^\s{$leading_ws_width}//; # Trim leading whitespace
      $line =~ s/\s+$//;                   # Trim trailing whitespace
      $item .= "$line\n";
    }
    $item =~ s/\n+\Z/\n/;                  # Trim trailing blank lines
    push @$items, $item;
    --$line_num unless ($line_num == $#lines && $lines[$line_num] !~ /^20/);
  } elsif ($type eq 'paragraph') {         # List item boundary is a blank line
    my $line;
    my $item = $_;
    $item =~ s/^(\s+)//;
    my $leading_ws_width = defined($1) ? length($1) : 0;
    $item =~ s/\s+$//;                     # Trim trailing whitespace
    $item .= "\n";

    unless ($in_major_component_versions_section) {
      while ($line_num < $#lines and ($line = $lines[++$line_num]) =~ /\S/) {
        $line =~ s/^\s{$leading_ws_width}//; # Trim leading whitespace
        $line =~ s/\s+$//;                   # Trim trailing whitespace
        $item .= "$line\n";
      }
    } else {
      ++$line_num;
    }
    push @$items, $item;
    --$line_num unless ($line_num == $#lines && $lines[$line_num] !~ /^20/);
  } else { # $type is one of the bulleted types
    # List item boundary is another bullet or a blank line
    my $line;
    my $item = $_;
    $item =~ s/^(\s*\Q$type\E\s*)//;       # Trim the leading bullet
    my $leading_ws_width = defined($1) ? length($1) : 0;
    $item =~ s/\s+$//;                     # Trim trailing whitespace
    $item .= "\n";

    while ($line_num < $#lines
           and ($line = $lines[++$line_num]) !~ /^(?:\S|\s*\Q$type\E\s+)/) {
      $line =~ s/^\s{$leading_ws_width}//; # Trim leading whitespace
      $line =~ s/\s+$//;                   # Trim trailing whitespace
      $item .= "$line\n";
    }
    push @$items, $item;
    --$line_num unless ($line_num == $#lines && $lines[$line_num] !~ /^20/);
  }
}

# Recognize IDs of top level nodes of the most recent two releases,
# escaping JavaScript regex metacharacters, e.g.: "^(?:trunk|2\\\\.4\\\\.0)"
my $first_relid_regex = $first_relid;
$first_relid_regex =~ s!([.+*?{}()|^$/\[\]\\])!\\\\\\\\$1!g;
my $second_relid_regex = $second_relid;
$second_relid_regex =~ s!([.+*?{}()|^$/\[\]\\])!\\\\\\\\$1!g;
my $newer_version_regex = "^(?:$first_relid_regex|$second_relid_regex)";

#
# Print HTML-ified version to STDOUT
#
print<<"__HTML_HEADER__";
<!--
**********************************************************
** WARNING: This file is generated from CHANGES.txt by the 
**          Perl script 'changes2html.pl'.
**          Do *not* edit this file!
**********************************************************
          
****************************************************************************
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
****************************************************************************
-->
<html>
<head>
  <title>$title</title>
  <link rel="stylesheet" href="ChangesFancyStyle.css" title="Fancy">
  <link rel="alternate stylesheet" href="ChangesSimpleStyle.css" title="Simple">
  <link rel="alternate stylesheet" href="ChangesFixedWidthStyle.css" title="Fixed Width">
  <META http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <SCRIPT>
    function toggleList(id) {
      listStyle = document.getElementById(id + '.list').style;
      anchor = document.getElementById(id);
      if (listStyle.display == 'none') {
        listStyle.display = 'block';
        anchor.title = 'Click to collapse';
        location.href = '#' + id;
      } else {
        listStyle.display = 'none';
        anchor.title = 'Click to expand';
      }
      var expandButton = document.getElementById('expand.button');
      expandButton.disabled = false;
      var collapseButton = document.getElementById('collapse.button');
      collapseButton.disabled = false;
    }

    function collapseAll() {
      var unorderedLists = document.getElementsByTagName("ul");
      for (var i = 0; i < unorderedLists.length; i++) {
        if (unorderedLists[i].className != 'bulleted-list')
          unorderedLists[i].style.display = "none";
        else
          unorderedLists[i].style.display = "block";
      }
      var orderedLists = document.getElementsByTagName("ol");
      for (var i = 0; i < orderedLists.length; i++)
        orderedLists[i].style.display = "none"; 
      var anchors = document.getElementsByTagName("a");
      for (var i = 0 ; i < anchors.length; i++) {
        if (anchors[i].id != '')
          anchors[i].title = 'Click to expand';
      }
      var collapseButton = document.getElementById('collapse.button');
      collapseButton.disabled = true;
      var expandButton = document.getElementById('expand.button');
      expandButton.disabled = false;
    }

    function expandAll() {
      var unorderedLists = document.getElementsByTagName("ul");
      for (var i = 0; i < unorderedLists.length; i++)
        unorderedLists[i].style.display = "block";
      var orderedLists = document.getElementsByTagName("ol");
      for (var i = 0; i < orderedLists.length; i++)
        orderedLists[i].style.display = "block"; 
      var anchors = document.getElementsByTagName("a");
      for (var i = 0 ; i < anchors.length; i++) {
        if (anchors[i].id != '')
          anchors[i].title = 'Click to collapse';
      }
      var expandButton = document.getElementById('expand.button');
      expandButton.disabled = true;
      var collapseButton = document.getElementById('collapse.button');
      collapseButton.disabled = false;

    }

    var newerRegex = new RegExp("$newer_version_regex");
    function isOlder(listId) {
      return ! newerRegex.test(listId);
    }

    function escapeMeta(s) {
      return s.replace(/(?=[.*+?^\${}()|[\\]\\/\\\\])/g, '\\\\');
    }

    function shouldExpand(currentList, currentAnchor, listId) {
      var listName = listId.substring(0, listId.length - 5);
      var parentRegex = new RegExp("^" + escapeMeta(listName) + "\\\\.");
      return currentList == listId
             || (isOlder(currentAnchor) && listId == 'older.list')
             || parentRegex.test(currentAnchor);
    }

    function collapse() {
      /* Collapse all but the first and second releases. */
      var unorderedLists = document.getElementsByTagName("ul");
      var currentAnchor = location.hash.substring(1);
      var currentList = currentAnchor + ".list";

      for (var i = 0; i < unorderedLists.length; i++) {
        var list = unorderedLists[i];
        /* Collapse the current item, unless either the current item is one of
         * the first two releases, or the current URL has a fragment and the
         * fragment refers to the current item or one of its ancestors.
         */
        if (list.id != '$first_relid.list' 
            && list.id != '$second_relid.list'
            && list.className != 'bulleted-list'
            && (currentAnchor == ''
                || ! shouldExpand(currentList, currentAnchor, list.id))) {
          list.style.display = "none";
        }
      }
      var orderedLists = document.getElementsByTagName("ol");
      for (var i = 0; i < orderedLists.length; i++) {
        var list = orderedLists[i];
        /* Collapse the current item, unless the current URL has a fragment
         * and the fragment refers to the current item or one of its ancestors.
         */
        if (currentAnchor == ''
            || ! shouldExpand(currentList, currentAnchor, list.id)) {
          list.style.display = "none";
        }
      }
      /* Add "Click to collapse/expand" tooltips to the release/section headings */
      var anchors = document.getElementsByTagName("a");
      for (var i = 0 ; i < anchors.length; i++) {
        var anchor = anchors[i];
        if (anchor.id != '') {
          if (anchor.id == '$first_relid' || anchor.id == '$second_relid') {
            anchor.title = 'Click to collapse';
          } else {
            anchor.title = 'Click to expand';
          }
        }
      }

      /* Insert "Expand All" and "Collapse All" buttons */
      var buttonsParent = document.getElementById('buttons.parent');
      var expandButton = document.createElement('button');
      expandButton.appendChild(document.createTextNode('Expand All'));
      expandButton.onclick = function() { expandAll(); }
      expandButton.id = 'expand.button';
      buttonsParent.appendChild(expandButton);
      var collapseButton = document.createElement('button');
      collapseButton.appendChild(document.createTextNode('Collapse All'));
      collapseButton.onclick = function() { collapseAll(); }
      collapseButton.id = 'collapse.button';
      buttonsParent.appendChild(collapseButton);
    }

    window.onload = collapse;
  </SCRIPT>
</head>
<body>

<h1>$title</h1>

<div id="buttons.parent"></div>

__HTML_HEADER__

my $heading;
my $relcnt = 0;
my $header = 'h2';
my $subheader = 'h3';

for my $rel (@releases) {
  if ($relcnt == 2) {
    $header = 'h3';
    $subheader = 'h4';
    print "<h2><a id=\"older\" href=\"javascript:toggleList('older')\">";
    print "Older Releases";
    print "</a></h2>\n";
    print "<ul id=\"older.list\">\n"
  }

  ($release, $reldate, $relinfo, $sections) = @$rel;

  # The first section heading is undefined for the older sectionless releases
  my $has_release_sections = has_release_sections($sections);

  my $relid = '';
  if ($release) { # Pre-release sections have no release ID
    ++$relcnt;
    ($relid = 'v'.lc($release)) =~ s/\s+/_/g;
    print "<$header>";
    print "<a id=\"$relid\" href=\"javascript:toggleList('$relid')\">"
      unless ($release =~ /^20\d\d/);
    print "Release " unless ($release =~ /^trunk$|^20\d\d/i);
    print "$release $relinfo";
    print " [$reldate]" unless ($reldate eq 'unknown' or not $reldate);
    print "</a>" unless ($release =~ /^20\d\d/);
    print "</$header>\n";
    print "<ul id=\"$relid.list\">\n"
      if ($has_release_sections);
  }

  for my $section (@$sections) {
    ($heading, $items) = @$section;
    (my $sectid = lc($heading)) =~ s/\s+/_/g;
    my $numItemsStr = $#{$items} > 0 ? "($#{$items})" : "(none)";  

    my $list_item = "li";
    if ($release) {
      if ($heading) {
        if ($heading eq 'Detailed Change List') {
          print "  <$subheader>$heading</$subheader>\n";
          next;
        } elsif ($has_release_sections) {
          print "  <li><a id=\"$relid.$sectid\"",
                " href=\"javascript:toggleList('$relid.$sectid')\">$heading</a>",
                "&nbsp;&nbsp;&nbsp;$numItemsStr\n"
        }
      }
    } else { # $release is not defined
      print "<h2>$heading</h2>\n" if ($heading);
      $list_item = "p";
    }

    my $list_type = $items->[0] || '';
    my $list = ($has_release_sections || $list_type eq 'numbered' ? 'ol' : 'ul');
    my $listid = $sectid ? "$relid.$sectid" : $relid;
    print "    <$list id=\"$listid.list\">\n"
      unless (not $release or ($has_release_sections and not $heading));

    for my $itemnum (1..$#{$items}) {
      my $item = $items->[$itemnum];
      $item =~ s:&:&amp;:g;                       # Escape HTML metachars, but leave <code> tags
      $item =~ s~<(?!/?code>(?:[^,]|$))~&lt;~gi;  #   intact - unless followed by a comma - and
      $item =~ s:(?<!code)>:&gt;:gi;              #   add <pre> wrappers for non-inline sections
      $item =~ s{((?:^|.*\n)\s*)<code>(?!,)(?!</code>.+)(.+)</code>(?![ \t]*\S)}
                { 
                  my $prefix = $1; 
                  my $code = $2;
                  $code =~ s/\s+$//;
                  "$prefix<code><pre>$code</pre></code>"
                }gise;

      $item = markup_trailing_attribution($item) unless ($item =~ /\n[ ]*-/);

      $item =~ s{(.*?)(<code><pre>.*?</pre></code>)|(.*)}
                {
                  my $uncode = undef;
                  my ($one,$two,$three) = ($1,$2,$3);
                  if (defined($two)) {
                    $uncode = $one || '';
                    $uncode =~ s{^(.*?)(?=\n[ ]*-)}
                                {
                                  my $prefix = $1;
                                  my ($primary,$additional_work) = $prefix =~ /^(.*?)((?:\s*Additional\s+Work:\s*)?)$/si;
                                  my $result = markup_trailing_attribution($primary);
                                  $result .= "<br />\n$additional_work<br />" if ($additional_work);
                                  $result;
                                }se;
                    $uncode =~ s{((?<=\n)[ ]*-.*\n(?:.*\n)*)}
                                {
                                  my $bulleted_list = $1;
                                  $bulleted_list
                                    =~ s{(?:(?<=\n)|\A)[ ]*-[ ]*(.*(?:\n|\z)(?:[ ]+[^ -].*(?:\n|\z))*)}
                                        {
                                            qq!<li class="bulleted-list">\n!
                                          . markup_trailing_attribution($1)
                                          . "</li>\n"
                                        }ge;
                                  $bulleted_list
                                    =~ s{(<li.*</li>\n)(.*)}
                                        {
                                            qq!<ul class="bulleted-list">\n$1</ul>\n!
                                          . markup_trailing_attribution($2 || '')
                                        }se;
                                  $bulleted_list;
                                }ge;
                    "$uncode$two";
                  } else {
                    $uncode = $three || '';
                    $uncode =~ s{^(.*?)(?=\n[ ]*-)}
                                {
                                  my $prefix = $1;
                                  my ($primary,$additional_work) = $prefix =~ /^(.*?)((?:\s*Additional\s+Work:\s*)?)$/si;
                                  my $result = markup_trailing_attribution($primary);
                                  $result .= "<br />\n$additional_work<br />" if ($additional_work);
                                  $result;
                                }se;
                    $uncode =~ s{((?<=\n)[ ]*-.*\n(?:.*\n)*)}
                                {
                                  my $bulleted_list = $1;
                                  $bulleted_list
                                    =~ s{(?:(?<=\n)|\A)[ ]*-[ ]*(.*(?:\n|\z)(?:[ ]+[^ -].*(?:\n|\z))*)}
                                        {
                                            qq!<li class="bulleted-list">\n!
                                          . markup_trailing_attribution($1)
                                          . "</li>\n"
                                        }ge;
                                  $bulleted_list
                                    =~ s{(<li.*</li>\n)(.*)}
                                        {
                                            qq!<ul class="bulleted-list">\n$1</ul>\n!
                                          . markup_trailing_attribution($2 || '')
                                        }se;
                                  $bulleted_list;
                                }ge;
                    $uncode = markup_trailing_attribution($uncode);
                    $uncode;
                  }
                }sge;

      $item =~ s:\n{2,}:\n<p/>\n:g;                   # Keep paragraph breaks
      # Link LUCENE-XXX, SOLR-XXX and INFRA-XXX to JIRA
      $item =~ s{(?:${jira_url_prefix})?((?:LUCENE|SOLR|INFRA)-\d+)}
                {<a href="${jira_url_prefix}$1">$1</a>}g;
      $item =~ s{(issue\s*\#?\s*(\d{3,}))}            # Link Issue XXX to JIRA
                {<a href="${jira_url_prefix}LUCENE-$2">$1</a>}gi;
      # Link Lucene XXX, SOLR XXX and INFRA XXX to JIRA
      $item =~ s{((LUCENE|SOLR|INFRA)\s+(\d{3,}))}
                {<a href="${jira_url_prefix}\U$2\E-$3">$1</a>}gi;
      # Link "[ github | gh ] pull request [ # ] X+" to Github pull request
      $item =~ s{((?:(?:(?:github|gh)\s+)?pull\s+request\s*(?:\#?\s*)?|gh-)(\d+))}
                {<a href="${github_pull_request_prefix}$2">$1</a>}gi;
      # Link "LUCENE_CHANGES.txt" to Lucene's same-release Changes.html
      if ($product eq 'SOLR') {
        $item =~ s[(LUCENE_CHANGES.txt)]
                  [<a href="${lucene_javadoc_url}changes/Changes.html">$1</a>]g;
      }
      if ($product eq 'LUCENE') {
        # Find single Bugzilla issues
        $item =~ s~((?i:bug|patch|issue)\s*\#?\s*(\d+))
                  ~ my $issue = $1;
                    my $jira_issue_num = $lucene_bugzilla_jira_map{$2}; # Link to JIRA copies
                    $issue = qq!<a href="${jira_url_prefix}LUCENE-$jira_issue_num">!
                           . qq!$issue&nbsp;[LUCENE-$jira_issue_num]</a>!
                      if (defined($jira_issue_num));
                    $issue;
                  ~gex;
        # Find multiple Bugzilla issues
        $item =~ s~(?<=(?i:bugs))(\s*)(\d+)(\s*(?i:\&|and)\s*)(\d+)
                  ~ my $leading_whitespace = $1;
                    my $issue_num_1 = $2;
                    my $interlude = $3;
                    my $issue_num_2 = $4;
                    # Link to JIRA copies
                    my $jira_issue_1 = $lucene_bugzilla_jira_map{$issue_num_1};
                    my $issue1
                        = qq!<a href="${jira_url_prefix}LUCENE-$jira_issue_1">!
                        . qq!$issue_num_1&nbsp;[LUCENE-$jira_issue_1]</a>!
                      if (defined($jira_issue_1));
                    my $jira_issue_2 = $lucene_bugzilla_jira_map{$issue_num_2};
                    my $issue2
                        = qq!<a href="${jira_url_prefix}LUCENE-$jira_issue_2">!
                        . qq!$issue_num_2&nbsp;[LUCENE-$jira_issue_2]</a>!
                      if (defined($jira_issue_2));
                    $leading_whitespace . $issue1 . $interlude . $issue2;
                  ~gex;
      }

      # Linkify URLs, except Bugzilla links, which don't work anymore
      $item =~ s~(?<![">])(https?://(?!(?:nagoya|issues)\.apache\.org/bugzilla)[^\s\)]+)~<a href="$1">$1</a>~g;

      $item =~ s~</ul>\s+<p/>\s+<br\s*/>~</ul>~;

      print "      <$list_item>$item</$list_item>\n";
    }
    print "    </$list>\n" unless (not $release or ($has_release_sections and not $heading));
    print "  </li>\n" if ($release and $has_release_sections);
  }
  print "</ul>\n" if ($release and $has_release_sections);
}
print "</ul>\n" if ($relcnt > 3);
print "</body>\n</html>\n";


# Subroutine: markup_trailing_attribution
#
# Takes one parameter:
#
#   - text possibly containing a trailing parenthesized attribution
#
# Returns one scalar:
#
#   - text with the the trailing attribution, if any, marked up with the color green
#
sub markup_trailing_attribution {
  my $item = shift;

  # Put attributions on their own lines - this already happens if there is a preceding </ul>
  my $extra_newline = ($item =~ m:</ul>:) ? '' : '<br />';
  # Check for trailing parenthesized attribution with no following period.
  # Exclude things like "(see #3 above)" and "(use the bug number instead of xxxx)"
  unless ($item =~ s{\s+(\((?![Ss]ee )
                           (?!spans\b)
                           (?!mainly\ )
                           (?!LUCENE-\d+\))
                           (?!SOLR-\d+\))
                           (?!user's)
                           (?!like\ )
                           (?!r\d{6})     # subversion revision 
                           (?!and\ )
                           (?!backported\ )
                           (?!in\ )
                           (?!inverse\ )
                           (?![Tt]he\ )
                           (?!use\ the\ bug\ number)
                           (?!e\.?g\.?\b)
                     [^()"]+?\))\s*$}
                    {\n${extra_newline}<span class="attrib">$1</span>}x) {
    # If attribution is not found, then look for attribution with a
    # trailing period, but try not to include trailing parenthesized things
    # that are not attributions.
    #
    # Rule of thumb: if a trailing parenthesized expression with a following
    # period does not contain "LUCENE-XXX", and it either has three or
    # fewer words or it includes the word "via" or the phrase "updates from",
    # then it is considered to be an attribution.

    $item =~ s{(\s+(\((?![Ss]ee\ )
                      (?!spans\b)
                      (?!mainly\ )
                      (?!LUCENE-\d+\))
                      (?!SOLR-\d+\))
                      (?!user's)
                      (?!like\ )
                      (?!r\d{6})     # subversion revision 
                      (?!and\ )
                      (?!backported\ )
                      (?!in\ )
                      (?!inverse\ )
                      (?![Tt]he\ )
                      (?!use\ the\ bug\ number)
                      (?!e\.?g\.?\b)
                 [^()"]+?\)))
                ((?:\.|(?i:\.?\s*Issue\s+\d{3,}|LUCENE-\d+)\.?)\s*)$}
              {
                my $subst = $1;  # default: no change
                my $parenthetical = $2;
                my $trailing_period_and_or_issue = $3;
                if ($parenthetical !~ /LUCENE-\d+/) {
                  my ($no_parens) = $parenthetical =~ /^\((.*)\)$/s;
                  my @words = grep {/\S/} split /\s+/, $no_parens;
                  my $commas = $no_parens =~ s/,/,/g; # count commas
                  my $max_words = 4 + $commas;
                  if ($no_parens =~ /\b(?:via|updates\s+from)\b/i || scalar(@words) <= $max_words) {
                    $subst = "\n${extra_newline}<span class=\"attrib\">$parenthetical</span>";
                  }
                }
                $subst . $trailing_period_and_or_issue;
              }ex;
  }
  return $item;
}

#
# Subroutine: has_release_sections
#
# Takes one parameter:
#
#    - The $sections array reference
#
# Returns one scalar:
#
#    - A boolean indicating whether there are release sections 
#
sub has_release_sections {
  my $sections = shift;
  my $has_release_sections = 0;
  my $first_titled_section_num = -1;
  for my $section_num (0 .. $#{$sections}) {
    if ($sections->[$section_num][0]) {
      $has_release_sections = 1;
      last;
    }
  }
  return $has_release_sections;
}


#
# Subroutine: get_list_type
#
# Takes one parameter:
#
#    - The first line of a sub-section/point
#
# Returns one scalar:
#
#    - The list type: 'numbered'; or one of the bulleted types '-', or '.' or
#      'paragraph'.
#
sub get_list_type {
  my $first_list_item_line = shift;
  my $type = 'paragraph'; # Default to paragraph type

  if ($first_list_item_line =~ /^\s{0,2}\d+\.\s+\S+/) {
    $type = 'numbered';
  } elsif ($first_list_item_line =~ /^\s*([-.*])\s+\S+/) {
    $type = $1;
  }
  return $type;
}


#
# Subroutine: get_release_date
#
# Takes two parameters:
#
#    - Release name
#    - Release info, potentially including a release date
#
# Returns two scalars:
#
#    - The release date, in format YYYY-MM-DD
#    - The remainder of the release info (if any), with release date stripped
#
sub get_release_date {
  my $release = shift;
  my $relinfo = shift;

  my ($year, $month, $dom, $reldate);

  if ($relinfo) {
    if ($relinfo =~ s:\s*(2\d\d\d)([-./])
                      (1[012]|0?[1-9])\2
                      ([12][0-9]|30|31|0?[1-9])\s*: :x) {
      # YYYY-MM-DD   or   YYYY-M-D   or   YYYY-MM-D   or   YYYY-M-DD
      $year = $1;
      $month = $3;
      $dom = $4;
      $dom = "0$dom" if (length($dom) == 1);
      $reldate = "$year-$month-$dom";
    } elsif ($relinfo =~ s:\s*(1[012]|0?[1-9])([-./])
                           ([12][0-9]|30|31|0?[1-9])\2
                           (2\d\d\d)\s*: :x) {
      # MM-DD-YYYY   or   M-D-YYYY   or   MM-D-YYYY   or   M-DD-YYYY
      $month = $1;
      $dom = $3;
      $dom = "0$dom" if (length($dom) == 1);
      $year = $4;
      $reldate = "$year-$month-$dom";
    } elsif ($relinfo =~ s:($month_regex)\s*
                           ([12][0-9]|30|31|0?[1-9])((st|rd|th)\.?)?,?\s*
                           (2\d\d\d)\s*: :x) {
      # MMMMM DD, YYYY   or   MMMMM DDth, YYYY
      $month = $month_nums{$1};
      $dom = $2;
      $dom = "0$dom" if (length($dom) == 1);
      $year = $5;
      $reldate = "$year-$month-$dom";
    } elsif ($relinfo =~ s:([12][0-9]|30|31|0?[1-9])(\s+|[-/.])
                           ($month_regex)\2
                           (2\d\d\d)\s*: :x) {
      # DD MMMMM YYYY
      $dom = $1;
      $dom = "0$dom" if (length($dom) == 1);
      $month = $month_nums{$3};
      $year = $4;
      $reldate = "$year-$month-$dom";
    }
  }

  unless ($reldate) {     # No date found in $relinfo
    # Handle '1.2 RC6', which should be '1.2 final'
    $release = '1.2 final' if ($release eq '1.2 RC6');

    $reldate = ( exists($release_dates{$release}) 
               ? $release_dates{$release}
               : 'unknown');
  }

  $relinfo =~ s/,?\s*$//; # Trim trailing comma and whitespace

  return ($reldate, $relinfo);
}


#
# setup_release_dates
#
# Returns a list of alternating release names and dates, for use in populating
# the %release_dates hash.
#
# Pulls release dates from the project DOAP file.
#
sub setup_release_dates {
  my %release_dates = ();
  my $file = shift;
  open(FILE, "<$file") || die "could not open $file: $!";
  my $version_list = <FILE>;
  my $created_list = <FILE>;
  close(FILE);

  $version_list =~ s/^\s+|\s+$//g;
  my @versions = split /\s*,\s*/, $version_list;
  $created_list =~ s/^\s+|\s+$//g;
  my @created = split /\s*,\s*/, $created_list; 

  if (scalar(@versions) != scalar(@created)) {
    die $file . " contains" . scalar(@versions) . " versions but " . scalar(@created) . " creation dates.";
  }
  my $date;
  for my $pos (0..$#versions) {
    $date = normalize_date($created[$pos]);
    $release_dates{$versions[$pos]} = $date;
    if ($versions[$pos] =~ /^([1-9]\d*\.\d+)([^.0-9].*|$)/) {
      my $padded_version_name = "$1.0$2";             # Alias w/trailing ".0"
      $release_dates{$padded_version_name} = $date;
    } elsif ($versions[$pos] =~ /\.0(?=[^.0-9]|$)/) {
      my $trimmed_version_name = $versions[$pos];
      $trimmed_version_name =~ s/\.0(?=[^.0-9]|$)//;  # Alias w/o trailing ".0"
      $release_dates{$trimmed_version_name} = $date;
    }
  }
  return %release_dates;
}

#
# normalize_date
#
# Left-zero-pads month and day-of-month to 2 digits in dates of format YYYY-(M)M-(D)D
#
sub normalize_date {
  my $date = shift;
  my ($year, $month, $dom) = $date =~ /^(2\d\d\d)-(\d+)-(\d+)$/;
  return sprintf("%04d-%02d-%02d", $year, $month, $dom);
}


#
# setup_month_regex
#
# Returns a string containing a regular expression with alternations for
# the standard month representations in English.
#
sub setup_month_regex {
  return '(?i:Jan(?:|\.|uary)|Feb(?:|\.|ruary)|Mar(?:|\.|ch)'
       . '|Apr(?:|\.|il)|May|Jun(?:|\.|e)|Jul(?:|\.|y)|Aug(?:|\.|ust)'
       . '|Sep(?:|\.|t(?:|\.|ember))|Oct(?:|\.|ober)|Nov(?:|\.|ember)'
       . '|Dec(?:|\.|ember))';
}


#
# setup_month_nums
#
# Returns a list of alternating English month representations and the two-digit
# month number corresponding to them, for use in populating the %month_nums
# hash.
#
sub setup_month_nums {
  return ( 'Jan' => '01', 'Jan.' => '01', 'January' => '01',
           'Feb' => '02', 'Feb.' => '02', 'February' => '02',
           'Mar' => '03', 'Mar.' => '03', 'March' => '03',
           'Apr' => '04', 'Apr.' => '04', 'April' => '04',
           'May' => '05',
           'Jun' => '06', 'Jun.' => '06', 'June' => '06',
           'Jul' => '07', 'Jul.' => '07', 'July' => '07',
           'Aug' => '08', 'Aug.' => '08', 'August' => '08',
           'Sep' => '09', 'Sep.' => '09',
           'Sept' => '09', 'Sept.' => '09', 'September' => '09',
           'Oct' => '10', 'Oct.' => '10', 'October' => '10',
           'Nov' => '11', 'Nov.' => '11', 'November' => '11',
           'Dec' => '12', 'Dec.' => '12', 'December' => '12' );
}


#
# setup_lucene_bugzilla_jira_map
#
# Returns a list of alternating Bugzilla bug IDs and LUCENE-* JIRA issue
# numbers, for use in populating the %lucene_bugzilla_jira_map hash
#
sub setup_lucene_bugzilla_jira_map {
  return (  4049 =>   1,  4102 =>   2,  4105 =>   3,  4254 =>   4,
            4555 =>   5,  4568 =>   6,  4754 =>   7,  5313 =>   8,
            5456 =>   9,  6078 =>  10,  6091 =>  11,  6140 =>  12,
            6292 =>  13,  6315 =>  14,  6469 =>  15,  6914 =>  16,
            6968 =>  17,  7017 =>  18,  7019 =>  19,  7088 =>  20,
            7089 =>  21,  7275 =>  22,  7412 =>  23,  7461 =>  24,
            7574 =>  25,  7710 =>  26,  7750 =>  27,  7782 =>  28,
            7783 =>  29,  7912 =>  30,  7974 =>  31,  8307 =>  32,
            8525 =>  33,  9015 =>  34,  9110 =>  35,  9347 =>  36,
            9454 =>  37,  9782 =>  38,  9853 =>  39,  9906 =>  40,
            9970 =>  41, 10340 =>  42, 10341 =>  43, 10342 =>  44,
           10343 =>  45, 10849 =>  46, 11109 =>  47, 11359 =>  48,
           11636 =>  49, 11918 =>  50, 12137 =>  51, 12273 =>  52,
           12444 =>  53, 12569 =>  54, 12588 =>  55, 12619 =>  56,
           12667 =>  57, 12723 =>  58, 12749 =>  59, 12761 =>  60,
           12950 =>  61, 13102 =>  62, 13166 =>  63, 14028 =>  64,
           14355 =>  65, 14373 =>  66, 14412 =>  67, 14485 =>  68,
           14585 =>  69, 14665 =>  70, 14900 =>  71, 15739 =>  72,
           16025 =>  73, 16043 =>  74, 16167 =>  75, 16245 =>  76,
           16364 =>  77, 16437 =>  78, 16438 =>  79, 16470 =>  80,
           16677 =>  81, 16719 =>  82, 16730 =>  83, 16816 =>  84,
           16952 =>  85, 17242 =>  86, 17954 =>  88, 18014 =>  89,
           18088 =>  90, 18177 =>  91, 18410 =>  87, 18833 =>  92,
           18847 =>  93, 18914 =>  94, 18927 =>  95, 18928 =>  96,
           18929 =>  97, 18931 =>  98, 18932 =>  99, 18933 => 100,
           18934 => 101, 19058 => 102, 19149 => 103, 19189 => 104,
           19253 => 105, 19468 => 106, 19686 => 107, 19736 => 108,
           19751 => 109, 19834 => 110, 19844 => 111, 20024 => 112,
           20081 => 113, 20123 => 114, 20196 => 115, 20283 => 116,
           20290 => 117, 20461 => 118, 20901 => 119, 21128 => 120,
           21149 => 121, 21150 => 122, 21189 => 123, 21446 => 124,
           21921 => 126, 22344 => 128, 22469 => 130, 22987 => 131,
           23307 => 133, 23308 => 134, 23422 => 135, 23466 => 136,
           23505 => 137, 23534 => 138, 23545 => 139, 23650 => 140,
           23655 => 141, 23685 => 142, 23702 => 143, 23727 => 144,
           23730 => 145, 23750 => 146, 23754 => 147, 23770 => 148,
           23771 => 149, 23773 => 150, 23774 => 151, 23782 => 152,
           23784 => 153, 23786 => 154, 23838 => 155, 23964 => 156,
           24084 => 129, 24237 => 157, 24265 => 158, 24301 => 159,
           24370 => 160, 24665 => 161, 24786 => 162, 24902 => 163,
           24903 => 164, 24913 => 165, 25666 => 125, 25793 => 166,
           25820 => 167, 25945 => 168, 26120 => 169, 26196 => 170,
           26268 => 171, 26360 => 172, 26396 => 173, 26397 => 174,
           26624 => 175, 26634 => 176, 26666 => 177, 26702 => 178,
           26716 => 179, 26763 => 180, 26884 => 181, 26939 => 182,
           27168 => 183, 27174 => 184, 27182 => 185, 27268 => 186,
           27326 => 187, 27354 => 188, 27408 => 189, 27423 => 190,
           27433 => 191, 27491 => 192, 27587 => 193, 27626 => 194,
           27638 => 195, 27743 => 196, 27772 => 197, 27799 => 198,
           27819 => 199, 27865 => 200, 27868 => 201, 27903 => 202,
           27987 => 203, 28030 => 204, 28050 => 205, 28065 => 206,
           28074 => 207, 28108 => 208, 28181 => 209, 28182 => 210,
           28183 => 211, 28187 => 212, 28285 => 213, 28336 => 214,
           28339 => 215, 28405 => 216, 28462 => 217, 28601 => 218,
           28640 => 219, 28748 => 220, 28827 => 221, 28855 => 222,
           28856 => 223, # Clone: 28856 => 507,
           28858 => 224, 28960 => 132, 28964 => 127, 29033 => 225,
           29256 => 226, 29299 => 227, 29302 => 228, 29370 => 229,
           29432 => 230, 29548 => 231, 29749 => 232, 29756 => 233,
           29774 => 234, 29931 => 235, 29984 => 236, 30013 => 237,
           30016 => 238, 30026 => 239, 30027 => 240, 30049 => 241,
           30058 => 242, 30232 => 243, 30237 => 244, 30240 => 245,
           30242 => 246, 30265 => 247, 30327 => 248, 30330 => 249,
           30360 => 250, 30376 => 251, 30382 => 252, 30421 => 253,
           30429 => 254, 30452 => 255, 30480 => 256, 30522 => 257,
           30617 => 258, 30621 => 259, 30628 => 260, 30629 => 261,
           30668 => 262, 30678 => 263, 30685 => 264, 30736 => 265,
           30785 => 266, 30818 => 267, 30835 => 268, 30844 => 269,
           30977 => 270, 30985 => 271, 31061 => 272, 31120 => 273,
           31149 => 274, 31174 => 275, 31240 => 276, 31241 => 277,
           31294 => 278, 31350 => 279, 31368 => 280, 31420 => 281,
           31469 => 282, 31508 => 283, 31554 => 284, 31617 => 285,
           31619 => 286, 31690 => 287, 31706 => 288, 31708 => 289,
           31746 => 290, 31747 => 291, 31748 => 292, 31784 => 293,
           31785 => 294, 31841 => 295, 31882 => 296, 31926 => 297,
           31976 => 298, 32053 => 299, 32055 => 300, 32088 => 301,
           32090 => 302, 32109 => 303, 32115 => 304, 32143 => 305,
           32167 => 306, 32171 => 307, 32192 => 308, 32227 => 309,
           32228 => 310, 32234 => 311, 32291 => 312, 32307 => 313,
           32334 => 314, 32353 => 315, 32365 => 316, 32403 => 317,
           32432 => 318, 32467 => 319, 32468 => 320, 32580 => 321,
           32626 => 322, 32674 => 323, 32687 => 324, 32712 => 325,
           32847 => 326, 32887 => 327, 32921 => 328, 32942 => 329,
           32965 => 330, 32981 => 331, 32999 => 332, 33019 => 333,
           33076 => 334, 33134 => 335, 33158 => 336, 33161 => 337,
           33197 => 338, 33239 => 339, 33389 => 340, 33395 => 341,
           33397 => 342, 33442 => 343, 33449 => 344, 33459 => 345,
           33472 => 346, 33642 => 347, 33648 => 348, 33649 => 349,
           33654 => 350, 33678 => 351, 33725 => 352, 33799 => 353,
           33820 => 354, 33835 => 355, 33848 => 356, 33851 => 357,
           33877 => 358, 33884 => 359, 33974 => 360, 34028 => 361,
           34066 => 362, 34149 => 363, 34154 => 364, 34193 => 365,
           34279 => 366, 34320 => 367, 34331 => 368, 34359 => 369,
           34407 => 370, 34408 => 371, 34447 => 372, 34453 => 373,
           34477 => 374, # Clone: 34477 => 459,
           34486 => 375, 34528 => 376, 34544 => 377, 34545 => 378,
           34563 => 379, 34570 => 380, 34585 => 381, 34629 => 382,
           34673 => 383, 34684 => 384, 34695 => 385, 34816 => 386,
           34882 => 387, 34930 => 388, 34946 => 389, 34995 => 390,
           35029 => 391, 35037 => 392, 35157 => 393, 35241 => 394,
           35284 => 395, # Clone: 35284 => 466,
           35388 => 396, 35446 => 397, 35454 => 398, 35455 => 399,
           35456 => 400, 35468 => 401, 35491 => 402, 35518 => 403,
           35626 => 404, 35664 => 405, 35665 => 406, 35668 => 407,
           35729 => 408, 35730 => 409, 35731 => 410, 35796 => 411,
           35822 => 412, 35823 => 413, 35838 => 414, 35879 => 415,
           # Clone: 35879 => 616,
           35886 => 416, 35971 => 417, 36021 => 418, 36078 => 419,
           36101 => 420, 36135 => 421, 36147 => 422, 36197 => 423,
           36219 => 424, 36241 => 425, 36242 => 426, 36292 => 427,
           36296 => 428, 36333 => 429, 36622 => 430, 36623 => 431,
           36628 => 432);
}

1;

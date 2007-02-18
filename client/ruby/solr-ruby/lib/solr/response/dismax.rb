class Solr::Response::Dismax < Solr::Response::Standard
  # no need for special processing
  
  # FIXME: 2007-02-07 <coda.hale@gmail.com> --  The existence of this class indicates that
  # the Request/Response pair architecture is a little hinky. Perhaps we could refactor
  # out some of the most common functionality -- Common Query Parameters, Highlighting Parameters,
  # Simple Facet Parameters, etc. -- into modules?
end
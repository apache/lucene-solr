#!/usr/bin/env ruby
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

# TODO: distill common shell script needs into common file for parsing parameters for Solr URL, input filename, -debug, etc
#       script/runner or script/console-like, from Rails.  A data mapper would be a great generalizable piece.

require 'solr'

solr_url = ENV["SOLR_URL"] || "http://localhost:8983/solr"
dl_filename = ARGV[0]
debug = ARGV[1] == "-debug"

if dl_filename == nil
  puts "You must pass a filename as an option."
  exit
end

source = Solr::Importer::DelimitedFileSource.new(dl_filename)

# Exported column names
# medium,associatedURL,boxHeightInInches,boxLengthInInches,boxWeightInPounds,boxWidthInInches,
# scannednumber,upc,asin,country,title,fullTitle,series,numberInSeries,edition,aspect,mediacount,
# genre,price,currentValue,language,netrating,description,owner,publisher,published,rare,purchaseDate,rating,
# used,signed,hasExperienced,notes,location,paid,condition,notowned,author,illustrator,pages
mapping = {
  :id => Proc.new {|data| data[:upc].empty? ? data[:asin] : data[:upc]},
  :medium_facet => :medium,
  :country_facet => :country,
  :signed_facet => :signed,
  :rating_facet => :netrating,
  :language_facet => :language,
  :genre_facet => Proc.new {|data| data[:genre].split('/').map {|s| s.strip}},
  :title_text => :title,
  :full_title_text => :fullTitle,
  :asin_display => :asin,
  :notes_text => :notes,
  :publisher_facet => :publisher,
  :description_text => :description,
  :author_text => :author,
  :pages_text => :pages,
  :published_year_facet => Proc.new {|data| data[:published].scan(/\d\d\d\d/)[0]}
}

indexer = Solr::Indexer.new(source, mapping, :debug => debug)
indexer.index do |record, solr_document|
  # can modify solr_document before it is indexed here
end

indexer.solr.commit unless debug
indexer.solr.optimize unless debug
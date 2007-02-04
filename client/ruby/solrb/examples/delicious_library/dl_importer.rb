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

solr = Solr::Connection.new(solr_url)

lines = IO.readlines(dl_filename)
headers = lines[0].split("\t").collect{|h| h.chomp}
puts headers.join(','),"-----" if debug

# Exported column names
# medium,associatedURL,boxHeightInInches,boxLengthInInches,boxWeightInPounds,boxWidthInInches,
# scannednumber,upc,asin,country,title,fullTitle,series,numberInSeries,edition,aspect,mediacount,
# genre,price,currentValue,language,netrating,description,owner,publisher,published,rare,purchaseDate,rating,
# used,signed,hasExperienced,notes,location,paid,condition,notowned,author,illustrator,pages
mapping = {
  :id => :upc,
  :medium_facet => :medium,
  :country_facet => :country,
  :signed_facet => :signed,
  :rating_facet => :netrating,
  :language_facet => :language,
  :genre_facet => Proc.new {|data| data.genre.split('/').map {|s| s.strip}},
  :title_text => :title,
  :notes_text => :notes,
  :publisher_text => :publisher,
  :description_text => :description,
  :author_text => :author,
  :pages_text => :pages
}

lines[1..-1].each do |line|
  data = headers.zip(line.split("\t").collect{|s| s.chomp})
  def data.method_missing(key)
    self.assoc(key.to_s)[1]
  end

  doc = {}
  mapping.each do |solr_name, data_column_or_proc|
    if data_column_or_proc.is_a? Proc
      value = data_column_or_proc.call(data)
    else
      value = data.send(data_column_or_proc)
    end  
    doc[solr_name] = value if value
  end
  
  puts data.title
  puts doc.inspect if debug
  solr.add doc unless debug

end

solr.commit unless debug

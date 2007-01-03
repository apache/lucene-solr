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

require "erb"
require 'rexml/document'
include ERB::Util

module Solr
  class Request
    attr_reader :url_path
    attr_reader :response_format
    
    def initialize
      @url_path = "/solr/select"
    end
  end
  
  class UpdateRequest < Request
    # sent to /solr/update with XML body
    def initialize(body)
      @body = body.to_s
      @url_path = "/solr/update"
      @response_format = :xml
    end
    
    def to_http_body
      @body
    end
  end
  
  class AddDocumentRequest < UpdateRequest
    def initialize(doc_hash)
      xml = REXML::Element.new('add')
      
      doc = REXML::Element.new('doc')
      
      doc_hash.each do |key,value|
        #TODO: add handling of array values
        doc.add_element field(key.to_s, value)
      end
      
      xml.add_element doc
      super(xml.to_s)
    end

    private
    def field(name, value)
      field = REXML::Element.new("field")
      field.add_attribute("name", name)
      field.add_text(value)
    
      field
    end
  end
  
  class SelectRequest < Request
    # sent to /solr/select, with url query string parameters in the body
    
    def initialize
      @response_format = :ruby
      super
    end
    
    def to_http_body
      raw_params = self.to_hash
      
      http_params = []
      raw_params.each do |key,value|
        #TODO: Add array value handling
        http_params << "#{key}=#{url_encode(value)}" if value
      end
      
      http_params.join("&")
    end
    
    def to_hash
      {:wt => "ruby"}
    end
  end
  
  class CommonRequestBase < SelectRequest
    # supported by both standard and dismax request handlers
    # start
    # rows
    # filter query (multiple)
    # field list
    attr_accessor :start
    attr_accessor :rows
    attr_accessor :filter_queries
    attr_accessor :field_list
    
    # debug
    # explainOther
    
    def to_hash
      {:start => @start,
        :rows => @rows,
          :fq => @filter_queries,
          :fl => @field_list}.merge(super)
    end
  end

  class StandardRequest < CommonRequestBase
    # sort
    # default field
    # query
    # query operator (AND/OR)
    attr_accessor :sort
    attr_accessor :default_field
    attr_accessor :query
    attr_accessor :operator
    
    def to_hash
      {:df => @default_field,
        :q => @sort ? "#{@query};#{@sort}" : @query,
       :op => @operator}.merge(super)
    end
  end
  
end

#s = Solr::Request.new("http://localhost:8983")
#s.add({:title => "foo"})


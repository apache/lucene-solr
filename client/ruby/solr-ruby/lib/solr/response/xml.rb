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

require 'rexml/document'
require 'solr/exception'

class Solr::Response::Xml < Solr::Response::Base
  attr_reader :doc, :status_code, :status_message

  def initialize(xml)
    super(xml)
    # parse the xml
    @doc = REXML::Document.new(xml)

    # look for the result code and string 
    # <?xml version="1.0" encoding="UTF-8"?>
    # <response>
    # <lst name="responseHeader"><int name="status">0</int><int name="QTime">2</int></lst>
    # </response>
    result = REXML::XPath.first(@doc, './response/lst[@name="responseHeader"]/int[@name="status"]')
    if result
      @status_code =  result.text
      @status_message = result.text  # TODO: any need for a message?
    end
  rescue REXML::ParseException => e
    raise Solr::Exception.new("invalid response xml: #{e}")
  end

  def ok?
    return @status_code == '0'
  end

end

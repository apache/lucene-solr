module Solr
  
  class RequestException < Exception
    attr_reader :code, :message
    
    def initialize(code, message)
      @code = code
      @message = message
    end
    
    def to_s
      @message
    end
    
  end
  
end
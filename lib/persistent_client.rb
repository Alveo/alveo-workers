require 'net/http/persistent'
require_relative 'net_http_overrides'

class PersistentClient

  def initialize(name)
    @connection = Net::HTTP::Persistent.new(name)  
  end    

  def close
    @connection.shutdown
  end

  def parse_json_response(response)
    JSON.parse(response.body)
  end

  def request(uri, request_type=:get, headers={}, body=nil)
    request = build_request(uri, request_type, headers, body)
    perform_request(request)
  end

  def perform_request(request)
    response = @connection.request(request)
    if not response.kind_of? Net::HTTPSuccess
      raise "Error performing #{request.method} request to #{request.uri}, response: #{response.message} (#{response.code})"
    end
    response
  end

  def build_request(uri, request_type, headers, body)
    request_class = Module.const_get("Net::HTTP::#{request_type.to_s.capitalize}")
    request = request_class.new(uri)
    headers.each_pair { |field, value|
      request.add_field(field, value)
    }
    request.body = body if body
    request
  end

end

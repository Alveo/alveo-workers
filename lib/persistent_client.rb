require 'net/http/persistent'
require_relative 'net_http_overrides'
require 'easy_logging'

class PersistentClient

  include EasyLogging

  def initialize(name)
    logger.debug "#initialize: name[#{name}]"

    @connection = Net::HTTP::Persistent.new(name)  
  end    

  def close
    logger.debug "#close"
    @connection.shutdown
  end

  def parse_json_response(response)
    logger.debug "#parse_json_response: response[#{response}]"
    JSON.parse(response.body)
  end

  def request(uri, request_type=:get, headers={}, body=nil)
    logger.debug "#request: uri[#{uri}], request_type[#{request_type}], headers[#{headers}], body[#{body.length}]"
    request = build_request(uri, request_type, headers, body)
    perform_request(request)
  end

  def perform_request(request)
    logger.debug "#perform_request: request[#{request}]"
    response = @connection.request(request)
    if not response.kind_of? Net::HTTPSuccess
      msg = "Error performing #{request.method} request to #{request.uri}, response: #{response.message} (#{response.code})"
      logger.error "perform_request: #{msg}"

      raise msg
    end
    response
  end

  def build_request(uri, request_type, headers, body)
    logger.debug "#build_request: uri[#{uri}], request_type[#{request_type}], headers[#{headers}], body[#{body.length}]"
    request_class = Module.const_get("Net::HTTP::#{request_type.to_s.capitalize}")
    request = request_class.new(uri)
    headers.each_pair { |field, value|
      request.add_field(field, value)
    }
    request.body = body if body
    request
  end

end

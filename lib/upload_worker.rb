require_relative 'worker'
require_relative 'metadata_helper'

require 'easy_logging'

class UploadWorker < Worker

  include MetadataHelper
  include EasyLogging

  def initialize(options)
    logger.debug "#initialize"
    @solr_queue_name = options[:solr_queue]
    @sesame_queue_name = options[:sesame_queue]
    @postgres_queue_name = options[:postgres_queue]
    super(options[:rabbitmq])

    # EasyLogging.log_destination = options[:logger]

  end

  def connect
    super
    @solr_queue = add_queue(@solr_queue_name)
    @postgres_queue = add_queue(@postgres_queue_name)
    @sesame_queue = add_queue(@sesame_queue_name)
  end

  def process_message(headers, message)
    logger.debug "#process_message"

    if headers['action'] == 'create'
      message['items'].each { |item|
        create_item(item, headers['collection'])
      }
    end
  end

  def create_item(item, collection)
    logger.debug "#create_item"

    # check whether current json is item or not
    is_item = is_item? item

    if is_item
      item['generated'] = generate_fields(item)
    end

    message = item.to_json
    headers = {action: 'create', collection: collection}
    properties = {routing_key: @sesame_queue.name, headers: headers, persistent: true}
    @exchange.publish(message, properties)
    logger.info "create_item: publish to [#{@sesame_queue.name}]"

    if is_item
      properties = {routing_key: @postgres_queue.name, headers: headers, persistent: true}
      @exchange.publish(message, properties)
      logger.info "create_item: publish to [#{@postgres_queue.name}]"

      properties = {routing_key: @solr_queue.name, headers: headers, persistent: true}
      @exchange.publish(message, properties)
      logger.info "create_item: publish to [#{@solr_queue.name}]"
    else
      logger.info "create_item: is NOT item, only publish to [#{@sesame_queue.name}]"
    end
  end

end

require 'bunny'
require 'json'
require 'easy_logging'
require 'active_record'

class AusTalkIngester

  include EasyLogging

  attr_accessor :ingesting
  attr_reader :record_count

  def initialize(options)
    logger.debug "#initialize: options[#{options}]"
    # EasyLogging.log_destination = options[:logger]
    @ingesting = true
    @options = options
    bunny_client_class = Module.const_get(options[:client_class])
    @bunny_client = bunny_client_class.new(options)
    logger.debug "@bunny_client.class.name[#{@bunny_client.class.name}]"
    @exchange_name = options[:exchange]
    @upload_queue_name = options[:upload_queue]
    @activerecord_options = options[:activerecord]

  end

  def connect
    logger.debug "#connect"

    begin
      @bunny_client.start
      logger.debug "@bunny_client.start...done"
      @channel = @bunny_client.create_channel
      logger.debug "@channel = @bunny_client.create_channel...done"
      @exchange = @channel.direct(@exchange_name, durable: true)
      logger.debug "@exchange = @channel.direct(@exchange_name, durable: true)...done"
      @upload_queue = add_queue(@upload_queue_name)
      ActiveRecord::Base.establish_connection(@activerecord_options)

      monitor_queues
    rescue Exception => e
      logger.error "exception[#{e.message}]"

    end


  end

  def monitor_queues
    logger.debug "#monitor_queues"

    @monitor_queues = []
    @options[:monitor].each { |queue|
      @monitor_queues << add_queue(queue)
    }
  end

  def monitor_queues_message_count
    logger.debug "#monitor_queues_message_count"

    message_count = 0
    @monitor_queues.each { |queue|
      message_count += queue.message_count
    }
    logger.debug "monitor_queues_message_count: message_count[#{message_count}]"

    message_count
  end

  def close
    logger.debug "#close"

    @channel.close
    @bunny_client.close
  end

  def add_queue(name)
    logger.debug "#add_queue: name[#{name}]"

    queue = @channel.queue(name, durable: true)
    queue.bind(@exchange, routing_key: name)
    queue
  end

  def set_work(work)
    logger.debug "#set_work: work[#{work}]"
    @work = work
  end

  def process()
    logger.debug "#process"

    @work.each { |austalk_chunk|
      process_chunk(austalk_chunk, 'austalk')
    }
  end

  def add_document_sizes(austalk_fields)
    logger.debug "#add_document_sizes: austalk_fields[#{austalk_fields.length}]"

    austalk_fields['items'].each { |item|
      item['ausnc:document'].each { |document|
        size = File.size? document['dcterms:source']
        document['alveo:size'] = size
        document['dcterms:extent'] = size
      }
    }
    austalk_fields
  end

  def process_chunk(austalk_chunk, collection, resume_point=0)
    logger.debug "#process_chunk: austalk_chunk[#{austalk_chunk}], collection[#{collection}], resume_point[#{resume_point}]"

    begin
      austalk_record = File.open(austalk_chunk).read

      austalk_fields = JSON.parse(austalk_record.encode('utf-8'))

      # remove item(s) already ingested
      austalk_fields = new_item_only(austalk_fields, collection)

      logger.info "#process_chunk: found [#{austalk_fields['items'].size}] new item(s) to publish"

      if austalk_fields['items'].size > 0
        austalk_fields = add_document_sizes(austalk_fields)

        properties = {routing_key: @upload_queue.name, headers: {action: 'create', collection: collection}}

        message = austalk_fields.to_json

        @exchange.publish(message, properties)
      end

    rescue Exception => e
      # TODO: Error queue instead of log file
      # logger.error "#{e.class}: #{e.to_s}\ninput: #{austalk_record}"
      logger.error "process_chunk: exception[#{e.message}]"
    end
  end

  #
  # Only ingest new item.
  #
  # To collect handle from json:
  #
  # JSONPath: $.items[*].alveo:metadata.dcterms:identifier
  #
  # Ref:
  # - http://jsonpath.com/
  # - http://goessner.net/articles/JsonPath/index.html
  def new_item_only(json, collection_name)
    logger.debug "#new_item_only: start - json items[#{json['items'].size}], collection_name[#{collection_name}]"

    json['items'].delete_if {|item|
      if !item['alveo:metadata']['dcterms:identifier'].nil? && !item['alveo:metadata']['dcterms:identifier'].empty?
        handle = "#{collection_name}:#{item['alveo:metadata']['dcterms:identifier']}"

        #   check exits in DB
        sql = "select id from items where handle='#{handle}';"
        result = ActiveRecord::Base.connection.execute(sql)
        if result.count > 0
        #   already exists in DB
          true
        else
          false
        end
      end
    }

    logger.debug "#new_item_only: end - json items[#{json['items'].size}]"

    return json
  end

end

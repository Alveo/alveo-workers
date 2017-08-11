require 'bunny'
require 'json'

class AusTalkIngester

  attr_accessor :ingesting
  attr_reader :record_count

  def initialize(options)
    @ingesting = true
    @options = options
    bunny_client_class = Module.const_get(options[:client_class])
    @bunny_client = bunny_client_class.new(options)
    @exchange_name = options[:exchange]
    @logger = Logger.new(options[:error_log])
    @upload_queue_name = options[:upload_queue]
  end

  def connect
    @bunny_client.start
    @channel = @bunny_client.create_channel
    @exchange = @channel.direct(@exchange_name, durable: true)
    @upload_queue = add_queue(@upload_queue_name)
    monitor_queues
  end

  def monitor_queues
    @monitor_queues = []
    @options[:monitor].each { |queue|
      @monitor_queues << add_queue(queue)
    }
  end

  def monitor_queues_message_count
    message_count = 0
    @monitor_queues.each { |queue|
      message_count += queue.message_count
    }
    message_count
  end

  def close
    @channel.close
    @bunny_client.close
  end

  def add_queue(name)
    queue = @channel.queue(name, durable: true)
    queue.bind(@exchange, routing_key: name)
    queue
  end

  def set_work(work)
    @work = work
  end

  def process()
    @work.each { |austalk_chunk|
      process_chunk(austalk_chunk, 'austalk')
    }
  end

  def add_document_sizes(austalk_fields)
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
    @logger.info "process_chunk: austalk_chunk[#{austalk_chunk}], collection[#{collection}], resume_point[#{resume_point}]"

    begin
      austalk_record = File.open(austalk_chunk).read
      @logger.info "process_chunk: austalk_record[#{austalk_record}]"

      austalk_fields = JSON.parse(austalk_record.encode('utf-8'))
      @logger.info "process_chunk: austalk_fields[#{austalk_fields}]"

      austalk_fields = add_document_sizes(austalk_fields)
      @logger.info "process_chunk: austalk_fields[#{austalk_fields}]"

      properties = {routing_key: @upload_queue.name, headers: {action: 'create', collection: collection}}
      @logger.info "process_chunk: properties[#{properties}]"

      message = austalk_fields.to_json
      @logger.debug "process_chunk: message[#{message}]"

      @exchange.publish(message, properties)
    rescue Exception => e
      # TODO: Error queue instead of log file
      # @logger.error "#{e.class}: #{e.to_s}\ninput: #{austalk_record}"
      @logger.error "process_chunk: exception[#{e.message}]"
    end
  end

end

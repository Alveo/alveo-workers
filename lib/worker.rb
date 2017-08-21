require 'bunny'
require 'json'
require 'ruby-prof'
require 'easy_logging'

class Worker

  include EasyLogging
  attr_reader :processed

  def initialize(options)
    # EasyLogging.log_destination = options[:logger]
    logger.debug "#initialize..."

    @options = options
    bunny_client_class = Module.const_get(@options[:client_class])
    # TODO: clean the options
    @bunny_client = bunny_client_class.new(@options)
  end

  def add_queue(name)
    logger.debug "#add_quene: name[#{name}]"
    queue = @channel.queue(name, durable: true)
    queue.bind(@exchange, routing_key: name)
    queue
  end

  def connect
    logger.debug "#connect..."
    @bunny_client.start
    @channel = @bunny_client.create_channel
    @channel.prefetch(@options[:prefetch])
    @exchange = @channel.direct(@options[:exchange], durable: true)
    @work_queue = add_queue(@options[:work_queue])
    @error_queue = add_queue(@options[:error_queue])
  end

  def close
    logger.debug "#close..."
    @channel.close
    @bunny_client.close
  end

  def start
    logger.debug "#start..."
    if @options[:profile]
      RubyProf.start
    end
    @processed = 0
    subscribe
  end

  def stop
    logger.debug "#stop..."
    @consumer.cancel
    if @options[:profile]
      result = RubyProf.stop
      time = Time.localtime
      prof_file = File.open("pro_#{time}.html")
      printer = RubyProf::CallStackPrinter.new(result)
      printer.print(prof_file)
      prof_file.close
    end
  end

  def subscribe
    logger.debug "#subscribe..."
    # TODO: rename work_queue to consumer_queue
    @consumer = @work_queue.subscribe(manual_ack: true) do |delivery_info, metadata, payload|
      on_message(metadata.headers, payload)
      @channel.ack(delivery_info.delivery_tag)
      @processed += 1
    end
  end

  def on_message(headers, payload)
    logger.debug "#on_message: headers, payload"
    begin
      message = JSON.parse(payload)
      process_message(headers, message)
    rescue StandardError => e
      send_error_message(e, payload)
    end
  end

  def process_message(headers, message)
    msg = 'Method must be implemented by subclasses'
    logger.error "process_message: #{msg}"

    raise msg
  end

  def send_error_message(exception, payload)
    # According to real environment test, an error message from PG can up to 33M bytes.
    # To set max error message size is necessary
    # so far 10K per field, 1M per message is good enough
    max_field_size = 1024 * 10
    max_message_size = 1024 * 1024

    error_message = {error: exception.class.to_s[0..max_field_size],
                     message: exception.to_s[0..max_field_size],
                     backtrace: exception.backtrace.to_s[0..max_field_size]}
    error_message = JSON.pretty_generate(error_message)
    error_message = "[#{error_message},\n{\"input\": #{payload}}]"[0..max_message_size]

    @exchange.publish(error_message, routing_key: @error_queue.name, persistent: true)

    # after send error message to queue, log it

    logger.error "send_error_message: #{error_message}"
  end

end
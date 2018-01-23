class Session
  @reader : DatabaseReader

  def initialize(@db : Database, @client : IO)
    @fmt = IO::ByteFormat::LittleEndian
    @reader = @db.reader
  end

  def start
    loop do
      cmd = @client.read_bytes(Int8, @fmt) rescue nil
      break if cmd.nil?
      cmd = Commands.from_value(cmd)
      case cmd
      when .read?
        read
      when .create?
        create
      when .close?
        @client.write_bytes(Status::Ok.to_i8, @fmt)
        break
      end
    end
  ensure
    cleanup
  end

  private def read
    id = @client.read_bytes(UInt64, @fmt)
    @reader.seek_to(id)
    @client.write_bytes(Status::Ok.to_i8)
    @client.write_bytes(@reader.bytes_until_eof.to_u32, @fmt)
    @reader.read_into(@client)
    @client.flush
  end

  private def create
    first, last = @db.append(@client)
    @client.write_bytes(Status::Ok.to_i8, @fmt)
    @client.write_bytes(first, @fmt)
    @client.write_bytes(last, @fmt)
    @client.flush
  end

  private def cleanup
    @client.close
    @reader.close
  end
end

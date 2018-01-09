class Session
  @reader : DatabaseReader

  def initialize(@db : Database, @client : IO)
    @fmt = IO::ByteFormat::LittleEndian
    @reader = @db.reader
  end

  def start
    loop do
      cmd = @client.read_bytes(Int8, @fmt)
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
  rescue IO::EOFError
  ensure
    cleanup
  end

  private def read
    id = @client.read_bytes(UInt64, @fmt)
    @reader.seek_to(id)
    @client.write_bytes(Status::Ok.to_i8)
    @reader.read_into(@client)
    @client.flush
  end

  private def create
    id = @db.append(@client)
    @client.write_bytes(Status::Ok.to_i8, @fmt)
    @client.write_bytes(id, @fmt)
    @client.flush
  end

  private def cleanup
    @client.close
    @reader.close
  end
end

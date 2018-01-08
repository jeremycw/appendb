require "socket"

enum Commands
  Create
  Read
  Close
  Connect
end

enum Status
  Ok
  InvalidCommand
  NotConnected
end

class Index
  def initialize(@id : UInt32)
    @density = 10_u16
    @file = File.open("#{Dir.current}/#{@id}.idx", "a+")
    @index = Array(Tuple(UInt64, UInt64)).new
    if @file.size > 0
      bytes_read = 0_u64
      while bytes_read < @file.size
        id = @file.read_bytes(UInt64, @fmt)
        offset = @file.read_bytes(UInt64, @fmt)
        bytes_read += 16
        @index << {id, offset}
      end
    end
    @fmt = IO::ByteFormat::LittleEndian
  end

  def find(id)
    entry = @index.bsearch { |a| a[0] >= id }
    return entry[1] if entry && entry[0] == id
  end

  def add(id, offset)
    return if id % @density != 1
    @index.push({id, offset})
    @file.write_bytes(id, @fmt)
    @file.write_bytes(offset, @fmt)
    @file.flush
  end

  def close
    @file.close
  end
end

class Database
  MAX_INDEX_SIZE = 4_000_000

  getter index

  def initialize(@id : UInt32)
    @db = File.open("#{Dir.current}/#{@id}.dat", "a+")
    @fmt = IO::ByteFormat::LittleEndian
    @index = Deque(Tuple(UInt64, UInt64)).new
    @autoinc = 0_u64
    if @db.size > 0
      @db.seek(-2, IO::Seek::End)
      size = @db.read_bytes(Int16, @fmt)
      @db.seek(-size, IO::Seek::Current)
      @autoinc = @db.read_bytes(UInt64, @fmt)
    end
  end

  def append(client)
    size = client.read_bytes(Int16, @fmt)
    @autoinc += 1
    @index.push({@autoinc, @db.size})
    if @index.size > MAX_INDEX_SIZE
      @index.shift
    end
    @db.write_bytes(@autoinc, @fmt)
    @db.write_bytes(size, @fmt)
    IO.copy(client, @db, size)
    @db.write_bytes(size + 12, @fmt)
    @db.flush
    return @autoinc
  end

  def readonly
    File.open("#{Dir.current}/#{@id}.dat", "r")
  end

  def close
    @db.close
  end
end

class Session
  @readdb : IO

  def initialize(@db : Database, @client : IO)
    @fmt = IO::ByteFormat::LittleEndian
    @readdb = @db.readonly
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
    index_entry = @db.index.bsearch { |a| a[0] >= id }
    if index_entry && index_entry[0] == id
      @readdb.seek(index_entry[1], IO::Seek::Set)
    else
      @readdb.seek(-2, IO::Seek::End)
      loop do
        size = @readdb.read_bytes(Int16, @fmt)
        @readdb.seek(-size, IO::Seek::Current)
        break if id == @readdb.read_bytes(UInt64, @fmt)
        @readdb.seek(-10, IO::Seek::Current)
      end
      @readdb.seek(-8, IO::Seek::Current)
    end
    @client.write_bytes(Status::Ok.to_i8)
    IO.copy(@readdb, @client)
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
    @db.close
    @readdb.close
  end
end

class Server
  def initialize(@port : UInt16)
    @databases = {} of UInt32 => Database
  end

  def listen
    server = TCPServer.new("localhost", @port)
    while client = server.accept?
      spawn accept(client)
    end
  end

  private def accept(client)
    cmd = client.read_bytes(Int8)
    cmd = Commands.from_value(cmd)
    if !cmd.connect?
      client.write_bytes(Status::NotConnected.to_i8)
      client.close
    else
      db_id = client.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      @databases[db_id] ||= Database.new(db_id)
      client.write_bytes(Status::Ok.to_i8)
      client.flush
      session = Session.new(@databases[db_id], client)
      session.start
    end
  end
end

server = Server.new(1234_u16)
server.listen

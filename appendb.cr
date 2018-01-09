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

class DatabaseReader
  def initialize(@id : UInt32, @index : Index)
    @fmt = IO::ByteFormat::LittleEndian
    @file = File.open("#{Dir.current}/#{@id}.dat", "r")
  end

  def seek_to(id)
    offset = @index.find(id)
    @file.seek(offset, IO::Seek::Set)
    loop do
      found = @file.read_bytes(UInt64, @fmt)
      break if found == id
      size = @file.read_bytes(UInt16, @fmt)
      @file.seek(@file.pos + size, IO::Seek::Set)
    end
    @file.seek(@file.pos - sizeof(typeof(id)), IO::Seek::Set)
  end

  def last_id
    return 0_u64 if @file.size == 0
    @file.seek(@index.last[1], IO::Seek::Set)
    loop do
      id = @file.read_bytes(UInt64, @fmt)
      size = @file.read_bytes(UInt16, @fmt)
      return id if @file.pos + size == @file.size
      @file.seek(@file.pos + size, IO::Seek::Set)
    end
  end

  def read_into(io)
    IO.copy(@file, io)
  end

  def close
    @file.close
  end
end

class Index
  def initialize(@id : UInt32)
    @fmt = IO::ByteFormat::LittleEndian
    @density = 10_u16
    @file = File.open("#{Dir.current}/#{@id}.idx", "a+")
    @index = Array(Tuple(UInt64, UInt64)).new
    if @file.size > 0
      bytes_read = 0_u64
      while bytes_read < @file.size
        id = @file.read_bytes(UInt64, @fmt)
        offset = @file.read_bytes(UInt64, @fmt)
        bytes_read += sizeof(typeof(id)) + sizeof(typeof(offset))
        @index << {id, offset}
      end
    end
  end

  def find(id)
    i = @index.bsearch_index { |a| a[0] >= id }
    return @index[i][1] if i && @index[i][0] == id
    return @index[i-1][1] if i && i - 1 >= 0
    return @index[@index.size-1][1] if @index.size > 0
    return 0_u64
  end

  def add(id, offset)
    return if id % @density != 1
    @index.push({id, offset})
    @file.seek(0, IO::Seek::End)
    @file.write_bytes(id, @fmt)
    @file.write_bytes(offset, @fmt)
    @file.flush
  end

  def last
    @index.last? || {0_u64, 0_u64}
  end

  def close
    @file.close
  end
end

class Database
  @autoinc : UInt64

  def initialize(@id : UInt32)
    filename = "#{Dir.current}/#{@id}.dat"
    if !File.exists?(filename)
      File.touch(filename)
    end
    @db = File.open(filename, "a+")
    @fmt = IO::ByteFormat::LittleEndian
    @index = Index.new(@id)
    @autoinc = reader.last_id
  end

  def append(client)
    size = client.read_bytes(UInt16, @fmt)
    @autoinc += 1
    @index.add(@autoinc, @db.size)
    @db.write_bytes(@autoinc, @fmt)
    @db.write_bytes(size, @fmt)
    IO.copy(client, @db, size)
    @db.flush
    return @autoinc
  end

  def reader
    DatabaseReader.new(@id, @index)
  end

  def close
    @db.close
    @index.close
  end
end

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

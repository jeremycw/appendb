require "socket"
require "./database.cr"
require "./index.cr"
require "./database_reader.cr"
require "./cached_append_file.cr"
require "./session.cr"
require "../commands.cr"

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


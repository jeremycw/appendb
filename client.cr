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

client = TCPSocket.new("localhost", 1234)
client.write_bytes(Commands::Connect.to_i8)
client.write_bytes(0_u32, IO::ByteFormat::LittleEndian)
client.flush
puts Status.from_value(client.read_byte)
client.write_bytes(Commands::Create.to_i8)
msg = "Hello, World!"
client.write_bytes(msg.bytesize.to_i16, IO::ByteFormat::LittleEndian)
client << msg
client.flush
puts Status.from_value(client.read_byte)
puts client.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
client.write_bytes(Commands::Read.to_i8)
client.write_bytes(3_u64, IO::ByteFormat::LittleEndian)
client.flush

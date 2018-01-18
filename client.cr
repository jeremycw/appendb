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

fmt = IO::ByteFormat::LittleEndian
client = TCPSocket.new("localhost", 1234)
client.write_bytes(Commands::Connect.to_i8)
client.write_bytes(0_u32, fmt)
client.flush
puts "Connect: #{Status.from_value(client.read_byte)}"
client.write_bytes(Commands::Create.to_i8)
client.write_bytes(2_u16)
msg = "Hello, World!"
client.write_bytes(msg.bytesize.to_i16, fmt)
client << msg
msg = "Goodbye, Galaxy!"
client.write_bytes(msg.bytesize.to_i16, fmt)
client << msg
client.flush
puts "Create: #{Status.from_value(client.read_byte)}"
id = client.read_bytes(UInt64, fmt)
puts id
puts client.read_bytes(UInt64, fmt)
client.write_bytes(Commands::Read.to_i8)
client.write_bytes(id, fmt)
client.flush
puts Status.from_value(client.read_byte)
incoming_bytes = client.read_bytes(UInt32, fmt)
bytes = 0_u32
loop do
  id = client.read_bytes(UInt64, fmt)
  size = client.read_bytes(UInt16, fmt)
  puts "\nreading #{id}, #{size} bytes"
  IO.copy(client, STDOUT, size)
  bytes += sizeof(typeof(id)) + sizeof(typeof(size)) + size
  STDOUT.flush
  break if bytes == incoming_bytes
end

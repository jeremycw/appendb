require "./server/server.cr"
require "./extensions/array.cr"

server = Server.new(1234_u16)
server.listen

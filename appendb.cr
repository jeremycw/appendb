require "./server/server.cr"

server = Server.new(1234_u16)
server.listen

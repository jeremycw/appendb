class Database

  @file : CachedFile

  def initialize(id)
    @fmt = IO::ByteFormat::LittleEndian
    path = "#{Dir.current}/#{id}.dat"
    File.touch(path) if !File.exists?(path)
    @index = Index.new(id)
    @in = Channel(IO?).new
    @out = Channel(Tuple(UInt64, UInt64)).new
    @file = get_consistent_file(path)
    spawn writer(@file)
  end

  def append(client)
    @in.send(client)
    return @out.receive
  end

  def reader
    DatabaseReader.new(@file.readonly_copy, @index)
  end

  def close
    @in.send(nil)
    @index.close
  end

  private def writer(file)
    reader = self.reader
    id = reader.last_id
    reader.close
    loop do
      client = @in.receive
      break if client.nil?
      pos = file.pos
      begin
        count = client.read_bytes(UInt16, @fmt)
        written = 0
        first = id + 1
        count.times do
          size = client.read_bytes(UInt16, @fmt)
          id += 1
          @index.add(id, file.size + written)
          file.write_bytes(id, @fmt)
          file.write_bytes(size, @fmt)
          written += size + sizeof(typeof(id))
          IO.copy(client, file, size)
        end
        file.flush
        @out.send({first, id})
      rescue
        id -= 1
        file.truncate(pos)
        @out.send({0_u64, 0_u64})
      end
    end
    file.close
  end

  private def get_consistent_file(path)
    file = CachedFile.open(path)
    return file if file.size == 0
    file.seek(@index.last[1], IO::Seek::Set)
    loop do
      pos = file.pos
      begin
        file.read_bytes(UInt64, @fmt)
        size = file.read_bytes(UInt16, @fmt)
        return file if file.pos + size == file.size
        file.seek(file.pos + size, IO::Seek::Set)
      rescue IO::EOFError
        file.truncate(pos)
        return file
      end
    end
  end
end

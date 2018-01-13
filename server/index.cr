require "./indexes/compressed_sparse_index.cr"
require "./indexes/simple_array_index.cr"

class Index
  def initialize(id)
    @fmt = IO::ByteFormat::LittleEndian
    @file = File.open("#{Dir.current}/#{id}.idx", "a+")
    @index = CompressedSparseIndex.new(10)
    if @file.size > 0
      bytes_read = 0_u64
      id = 1_u64
      while bytes_read < @file.size
        offset = @file.read_bytes(UInt64, @fmt)
        bytes_read += sizeof(typeof(offset))
        @index.add(id, offset)
        id += 1
      end
    end
  end

  def find(id)
    return @index.find(id)
  end

  def add(id, offset)
    @index.add(id, offset)
    @file.write_bytes(offset, @fmt)
    @file.flush
  end

  def last
    @index.last
  end

  def close
    @file.close
  end
end

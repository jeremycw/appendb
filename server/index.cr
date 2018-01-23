require "./indexes/compressed_sparse_index.cr"
require "./indexes/simple_array_index.cr"

class Index
  @rollback_pos : UInt64

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
    @rollback_pos = @file.size
  end

  def find(id)
    return @index.find(id)
  end

  def add(id, offset)
    @index.add(id, offset)
    @file.write_bytes(offset, @fmt)
    @file.flush
  end

  def mark_rollback
    @rollback_pos = @file.size
  end

  def rollback!
    @file.truncate(@rollback_pos)
  end

  def last
    @file.seek(-8, IO::Seek::End)
    offset = @file.read_bytes(UInt64, @fmt)
    {@file.size / 8, offset}
  end

  def close
    @file.close
  end
end

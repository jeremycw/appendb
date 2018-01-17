#require "./caches/slab_ring_buffer.cr"
#require "./caches/ring_buffer.cr"

class CachedFile < IO

  @cache_offset : UInt64

  delegate pos, seek, flush, close, size, truncate, to: @file

  def self.open(path, size = 134217728_u64)
    file = File.open(path, "a+")
    self.new(file, size)
  end

  protected def initialize(@file : File, @cache_max_size : UInt64)
    slab_size = 2048 * 1024
    @cache = IO::Memory.new #SlabRingBuffer.new(slab_size, (@cache_max_size / slab_size).to_i32)
    @cache_offset = @file.size
  end

  protected def initialize(@file : File, @cache_max_size : UInt64, @cache : IO, @cache_offset : UInt64)
  end

  def write(slice : Bytes)
    @cache.seek(0, IO::Seek::End)
    @cache.write(slice)
    @file.write(slice)
  end

  def read(slice : Bytes)
    if @file.pos >= @cache_offset
      @cache.seek(@file.pos - @cache_offset)
      read = @cache.read(slice)
      @file.pos += read
      return read
    else
      return @file.read(slice)
    end
  end

  def readonly_copy
    file = File.open(@file.path, "r")
    CachedFile.new(file, @cache_max_size, @cache, @cache_offset)
  end
end

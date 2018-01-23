require "./cache.cr"

class RingBuffer < Cache
  def initialize(@buffer_size : Int32)
    @storage = Slice(UInt8).new(@buffer_size, 0_u8)
    @pos = 0_i64
    @size = 0_i64
  end

  getter pos
  getter size

  def pos=(value)
    raise "Invalid position" if value < tail
    @pos = value
  end

  def seek(offset, whence : IO::Seek = IO::Seek::Set)
    case whence
    when .set?
      @pos = offset.to_i64
    when .end?
      @pos = @size + offset.to_i64
    when .current?
      @pos += offset.to_i64
    end
  end

  def write(slice : Bytes)
    slice.each do |b|
      @storage[@pos % @buffer_size] = b
      @pos += 1
    end
    @size += slice.size
    nil
  end

  def read(slice : Bytes)
    idx = 0
    slice.size.times do
      return idx if @pos >= @size
      slice[idx] = @storage[@pos % @buffer_size]
      @pos += 1
      idx += 1
    end
    slice.size
  end

  def tail
    tmp = @size - @buffer_size
    tmp > 0 ? tmp : 0
  end

end

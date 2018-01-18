class SlabRingBuffer < IO
  def initialize(@slab_size : Int32, @max_slabs : Int32)
    @pos = 0_i64
    @storage = Deque(Slice(UInt8)).new
    @storage << Slice(UInt8).new(@slab_size, 0_u8)
    @size = 0_i64
  end

  property pos
  getter size

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
      i = @pos / @slab_size
      if i >= @storage.size
        if @storage.size >= @max_slabs
          @storage.rotate!
        else
          @storage << Slice(UInt8).new(@slab_size, 0_u8)
        end
      end
      slab = @storage[i]
      slab[@pos % @slab_size] = b
      @pos += 1
    end
    @size += slice.size
    nil
  end

  def read(slice : Bytes)
    idx = 0
    slice.size.times do
      raise IO::EOFError.new if @pos >= @size
      i = @pos / @slab_size
      slab = @storage[i]
      slice[idx] = slab[@pos % @slab_size]
      @pos += 1
      idx += 1
    end
    slice.size
  end

end

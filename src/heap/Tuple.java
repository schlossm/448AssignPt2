package heap;

/**
 * Created by michaelschloss on 2/21/17.
 */
public class Tuple
{
	private int _length;
	private byte[] _bytes;

	public Tuple()
	{
		_length = 0;
	}

	public Tuple(byte[] data, int offset, int length)
	{
		_length = length;
		_bytes = data;
	}

	public int getLength()
	{
		return _length;
	}

	public byte[] getTupleByteArray()
	{
		return _bytes;
	}
}

package nl.vu.datalayer.hbase.operations;

public class ByteArrayInputStream extends java.io.ByteArrayInputStream {
	
	public ByteArrayInputStream(byte[] buf, int offset, int length) {
		super(buf, offset, length);
	}

	public ByteArrayInputStream(byte[] buf) {
		super(buf);
	}
	
	public void setArray(byte []array){
		super.buf = array;
        super.pos = 0;
        super.count = array.length;
        super.mark = 0;
	}
}

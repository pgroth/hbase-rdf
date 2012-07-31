package nl.vu.datalayer.hbase.id;

public abstract class Id {
	
	protected byte []id;
	
	public Id() {
		id = null;
	}

	public Id(byte[] id) {
		this.id = id;
	}
	
	public static Id build(byte []idBytes){
		switch (idBytes.length){
			case 8:
				return new BaseId(idBytes); 
			case 9:
				return new TypedId(idBytes);
			default:
				throw new RuntimeException("Unexpected id length");
		}
	}

	public byte[] getBytes()
	{
		return id;
	}

	public void set(byte[] content) {
		this.id = content;
	}
}

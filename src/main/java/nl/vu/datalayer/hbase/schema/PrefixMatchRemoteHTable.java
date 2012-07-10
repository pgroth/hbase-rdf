package nl.vu.datalayer.hbase.schema;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;

public class PrefixMatchRemoteHTable extends RemoteHTable {
	
	public PrefixMatchRemoteHTable(Client client, Configuration conf, byte[] name, String accessToken) {
		super(client, conf, name, accessToken);
	}

	public PrefixMatchRemoteHTable(Client client, Configuration conf, String name, String accessToken) {
		super(client, conf, name, accessToken);
	}

	public PrefixMatchRemoteHTable(Client client, String name, String accessToken) {
		super(client, name, accessToken);
	}

	public PrefixMatchRemoteHTable(Client client, String name) {
		super(client, name);
	}

	@Override
	public Result get(Get get) throws IOException {
		Scan scan = new Scan(get.getRow());
		scan.addFamily(get.familySet().iterator().next());
		ResultScanner scanner = getScanner(scan);
		Result ret = scanner.next();
		scanner.close();
		return ret;
	}

	@Override
	public Result[] get(List<Get> gets) throws IOException {
		Result []results = new Result[gets.size()];
		int i=0;
		for (Get get : gets) {
			results[i++] = get(get);
		}
		return results;
	}

	@Override
	public void close() throws IOException {
		//do nothing so that the client is not closed
		//when the client has to be closed we should close it explicitly
	}
	

}

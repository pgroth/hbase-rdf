package nl.vu.jena.assembler;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.jena.graph.HBaseGraph;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.assembler.Mode;
import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

public class HBaseAssembler extends AssemblerBase {

	@Override
	public Model openModel( Resource root )
	{ 
		try {
			return createHBaseModel();

		} catch (IOException e) {
			e.printStackTrace();	
		}
		return null;
	}

	@Override
	public Object open(Assembler a, Resource root, Mode mode) {
		try {
			return createHBaseModel();

		} catch (IOException e) {
			e.printStackTrace();	
		}
		return null;
	}

	private Model createHBaseModel() throws IOException {
		HBaseConnection con;
		con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);

		HBaseClientSolution hbaseSol = HBaseFactory.getHBaseSolution(
				"local-" + HBPrefixMatchSchema.SCHEMA_NAME, con, null);

		Properties prop = new Properties();
		try{
			prop.load(new FileInputStream("config.properties"));
		}
		catch (IOException e) {
			//continue to use the default properties
		}
		String caching = prop.getProperty("engine_caching", "off");
		
		Graph g;
		if (caching.equals("on")){
			g = new HBaseGraph(hbaseSol, HBaseGraph.CACHING_ON);
		}
		else{
			g = new HBaseGraph(hbaseSol, HBaseGraph.CACHING_OFF);
		}
		
		return ModelFactory.createModelForGraph(g);
	}

	

}

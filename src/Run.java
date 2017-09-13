import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryResult;

import com.franz.agraph.repository.AGCatalog;
import com.franz.agraph.repository.AGRepository;
import com.franz.agraph.repository.AGRepositoryConnection;
import com.franz.agraph.repository.AGServer;
import com.franz.agraph.repository.AGSpinFunction;
import com.franz.agraph.repository.AGTupleQuery;
import com.franz.agraph.repository.AGValueFactory;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.function.FunctionBase1;

public class Run {

	private static final String SERVER_URL = "http://ec2-35-158-95-85.eu-central-1.compute.amazonaws.com:10035";
	private static final String REPOSITORY_ID = "test";
	private static final String USERNAME = "test";
	private static final String PASSWORD = "santorini";

	public static void main(String[] args) {
		// Instantiate the Allegro server.
		AGServer server = new AGServer(SERVER_URL, USERNAME, PASSWORD);

		// Get the root catalog. A catalog is a list of repositories.
		AGCatalog rootCatalog = server.getRootCatalog();

		// Get the repository of interest from the root catalog.
		AGRepository repo = new AGRepository(rootCatalog, REPOSITORY_ID);

		// Get the connection to the repository.
		AGRepositoryConnection connection = repo.getConnection();

		System.out.println("Triple count before inserts: " + (connection.size()));

		// AGValueFactory contains useful methods to create values.
		AGValueFactory vf = repo.getValueFactory();

		// Assembling URIs for the new triples
		IRI alice = vf.createIRI("http://example.org/people/alice");
		IRI bob = vf.createIRI("http://example.org/people/bob");
		IRI name = vf.createIRI("http://example.org/ontology/name");
		IRI person = vf.createIRI("http://example.org/ontology/Person");

		// Create the literal values
		Literal bobsName = vf.createLiteral("Bob");
		Literal alicesName = vf.createLiteral("Alice");

		// Alice is a person
		connection.add(alice, RDF.TYPE, person);
		// Bob is a person, too.
		connection.add(bob, RDF.TYPE, person);
		// Alice's name is "Alice"
		connection.add(alice, name, alicesName);
		// Bob's name is "Bob"
		connection.add(bob, name, bobsName);

		System.out.println("Triple count after inserts: " + (connection.size()));

		// Get all the graph statements (a statement is a triple, context).
		System.out.println("\nList of all the statements:");
		RepositoryResult<Statement> result = connection.getStatements(null, null, null, false);
		while (result.hasNext()) {
			Statement st = result.next();
			System.out.println(st);
		}

		// Do a SPARQL query.
		String queryString = "SELECT ?s ?p ?o  WHERE {?s ?p ?o .}";
		AGTupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result2 = tupleQuery.evaluate();

		System.out.println("\nList of all the triples:");
		try {
			while (result2.hasNext()) {
				BindingSet bindingSet = result2.next();
				Value s = bindingSet.getValue("s");
				Value p = bindingSet.getValue("p");
				Value o = bindingSet.getValue("o");
				System.out.format("%s %s %s\n", s, p, o);
			}
		} finally {
			result2.close();
		}
		
		// Define a SPIN function.
		String subSparql = "SELECT ?diff WHERE{ BIND((xsd:integer(?v1)-xsd:integer(?v2))AS ?diff) . }";
		AGSpinFunction spinFunc = new AGSpinFunction("http://example.org/functions/subtract", new String[] {"v1", "v2"}, subSparql);
		connection.putSpinFunction(spinFunc);
		
		System.out.println("\nList of all the SPIN functions:");
		System.out.println(connection.listSpinFunctions());
		
		// Test the defined SPIN function.
		String testQuery = "PREFIX func:<http://example.org/functions/>\n"
				+ "SELECT ?res\n"
				+ "WHERE {\n"
		    		+ "BIND(func:subtract(5, 1) as ?res) .\n"
		    	+ "}";
		tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, testQuery);
		TupleQueryResult result3 = tupleQuery.evaluate();
		
		System.out.println("\nResult of the SPIN function:");
		try {
			while (result3.hasNext()) {
				BindingSet bindingSet = result3.next();
				Value res = bindingSet.getValue("res");
				System.out.println("SPIN function result: " + res);
			}
		} finally {
			result3.close();
		}
		
		// Remove triples from the graph.
		connection.remove(bob, name, bobsName);
		connection.remove(alice, name, alicesName);
		connection.remove(bob, RDF.TYPE, person);
		connection.remove(alice, RDF.TYPE, person);
		System.out.println("\nTriple count after deletion: " + (connection.size()));
	}
	
	/*
	public class SqrtFunc extends FunctionBase1 {

		public SqrtFunc() {
			super();
		}
		
		@Override
		public NodeValue exec(NodeValue v) {
			return Math.sqrt(v.getDouble());
		}
		
	}*/

}

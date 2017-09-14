package org.example.functions;

import com.franz.agraph.jena.AGGraph;
import com.franz.agraph.jena.AGGraphMaker;
import com.franz.agraph.jena.AGModel;
import com.franz.agraph.jena.AGQuery;
import com.franz.agraph.jena.AGQueryExecutionFactory;
import com.franz.agraph.jena.AGQueryFactory;
import com.franz.agraph.repository.AGCatalog;
import com.franz.agraph.repository.AGRepository;
import com.franz.agraph.repository.AGRepositoryConnection;
import com.franz.agraph.repository.AGServer;
import com.franz.agraph.repository.AGSpinFunction;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDF;

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

		// When using the Java Jena API, it is necessary to create a GraphMaker
		// object on the connection. This object will let us create graphs in
		// the connection's repository.
		AGGraphMaker graphMaker = new AGGraphMaker(connection);

		// Get the Allegro Graph
		AGGraph graph = graphMaker.getGraph();

		// Get the Allegro Graph Model
		AGModel model = new AGModel(graph);

		System.out.println("Triple count before inserts: " + (connection.size()));

		// Create the Resources
		Resource alice = model.createResource("http://example.org/people/alice");
		Resource bob = model.createResource("http://example.org/people/bob");
		Resource person = model.createResource("http://example.org/ontology/Person");

		// Create the Property name
		Property name = model.createProperty("http://example.org/ontology/name");

		// Create Literals
		Literal bobsName = model.createLiteral("Bob");
		Literal alicesName = model.createLiteral("Alice");

		// Alice's name is "Alice"
		model.add(alice, name, alicesName);
		// Alice is a person
		model.add(alice, RDF.type, person);
		// Bob's name is "Bob"
		model.add(bob, name, bobsName);
		// Bob is a person, too.
		model.add(bob, RDF.type, person);

		System.out.println("Triple count after inserts: " + (connection.size()));

		// Get all the graph statements (a statement is a (triple, context)).
		System.out.println("\nList of all the statements:");
		StmtIterator result = model.listStatements();
		while (result.hasNext()) {
			Statement st = result.next();
			System.out.println(st);
		}

		// Do a SPARQL query.
		String queryString = "SELECT ?s ?p ?o  WHERE {?s ?p ?o .}";
		AGQuery query = AGQueryFactory.create(queryString);
		QueryExecution qe = AGQueryExecutionFactory.create(query, model);

		System.out.println("\nList of all the triples:");
		try {
			ResultSet results = qe.execSelect();
			while (results.hasNext()) {
				QuerySolution r = results.next();
				RDFNode s = r.get("s");
				RDFNode p = r.get("p");
				RDFNode o = r.get("o");
				System.out.println(" { " + s + " " + p + " " + o + " . }");
			}
		} finally {
			qe.close();
		}

		// Define a SPIN function.
		String subSparql = "SELECT ?diff WHERE{ BIND((xsd:integer(?v1)-xsd:integer(?v2))AS ?diff) . }";
		AGSpinFunction spinFunc = new AGSpinFunction("http://example.org/functions/subtract",
				new String[] { "v1", "v2" }, subSparql);
		connection.putSpinFunction(spinFunc);

		System.out.println("\nList of all the SPIN functions:");
		System.out.println(connection.listSpinFunctions());

		// Test the defined SPIN function.
		/*
		 * // Register with the global registry.
		 * FunctionRegistry.get().put("http://example.org/functions/SqrtFunc",
		 * SqrtFunc.class);
		 */

		String testQuery = "PREFIX func:<http://example.org/functions/>\n" + "SELECT ?res\n" + "WHERE {\n"
				+ "BIND(func:subtract(4, 2) as ?res) .\n" + "}";
		query = AGQueryFactory.create(testQuery);
		qe = AGQueryExecutionFactory.create(query, model);

		System.out.println("\nResult of the SPIN function:");
		try {
			ResultSet result3 = qe.execSelect();
			while (result3.hasNext()) {
				QuerySolution qs = result3.next();
				System.out.println("SPIN function result: " + qs.getLiteral("res"));
			}
		} finally {
			qe.close();
		}

		// Remove triples from the graph.
		model.remove(bob, name, bobsName);
		model.remove(alice, name, alicesName);
		model.remove(bob, RDF.type, person);
		model.remove(alice, RDF.type, person);
		System.out.println("\nTriple count after deletion: " + (connection.size()));
	}

}

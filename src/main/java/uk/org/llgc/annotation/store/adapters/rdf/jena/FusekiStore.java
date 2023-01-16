package uk.org.llgc.annotation.store.adapters.rdf.jena;

import org.apache.jena.query.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFuseki;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.Lang;

import uk.org.llgc.annotation.store.AnnotationUtils;
import uk.org.llgc.annotation.store.data.PageAnnoCount;
import uk.org.llgc.annotation.store.adapters.rdf.AbstractRDFStore;
import uk.org.llgc.annotation.store.adapters.StoreAdapter;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;

import com.github.jsonldjava.utils.JsonUtils;

import java.nio.charset.Charset;

// Based on details at:
//    https://jena.apache.org/documentation/rdfconnection/

public class FusekiStore extends AbstractRDFStore implements StoreAdapter {
    protected static Logger _logger = LogManager.getLogger(FusekiStore.class.getName());
    
    //protected Dataset _dataset = null;
    protected RDFConnection _conn = null;
	  
    public FusekiStore(final AnnotationUtils pUtils, final String pRepositoryURL) {
	super(pUtils);
	// //_dataset = TDBFactory.createDataset(pDataDir);
	// _conn = RDFConnection.connect(pRepositoryURL);

	//RDFConnectionRemoteBuilder builder = RDFConnectionRemote.newBuilder()
	
	//RDFConnectionRemoteBuilder builder = RDFConnectionFuseki.create()
        //    .destination(pRepositoryURL)
        //    .queryEndpoint("/greenstone/sparql")
        //    .updateEndpoint("/greenstone/update")  /* make null for readonly */
        //    .gspEndpoint("/greenstone/data");      /* graph store protocol: "get" => read-only, "data" => read/write */

	//_conn = builder.build();
		
	System.err.println("**** FusekiStore::FusekiStore() pRepositoryURL = " + pRepositoryURL);

	_conn = RDFConnectionFuseki.newBuilder()
	    //.queryEndpoint(pRepositoryURL+"greenstone/query")
	    //.updateEndpoint(pRepositoryURL+"greenstone/update")
	    //.gspEndpoint(pRepositoryURL+"greenstone/data")
	    .queryEndpoint(pRepositoryURL+"greenstone")
	    .updateEndpoint(pRepositoryURL+"greenstone")
	    .gspEndpoint(pRepositoryURL+"greenstone")
	    .build();
	 

    }

    protected Model addAnnotationSafe(final Map<String,Object> pJson) throws IOException {
	String tJson = JsonUtils.toString(pJson);

        _logger.debug("Converting: " + tJson);
	Model tJsonLDModel = ModelFactory.createDefaultModel();

	RDFDataMgr.read(tJsonLDModel, new ByteArrayInputStream(tJson.getBytes(Charset.forName("UTF-8"))), Lang.JSONLD);
		
	// //_dataset.begin(ReadWrite.WRITE) ;
	// //_dataset.addNamedModel((String)pJson.get("@id"), tJsonLDModel);
	// //_dataset.commit();

	_conn.begin(ReadWrite.WRITE);
	System.err.println("**** FusekiStore.addAnnotationSafe() load graphID: " + (String)pJson.get("@id"));
	_conn.load((String)pJson.get("@id"), tJsonLDModel);
	_conn.commit();
	_conn.end();
	
	return tJsonLDModel;
    }

    protected void storeCanvas(final String pGraphName, final Model pModel) throws IOException {
		
	// //_dataset.begin(ReadWrite.WRITE) ;
	// //_dataset.addNamedModel(pGraphName, pModel);
	// //_dataset.commit();
	
	_conn.begin(ReadWrite.WRITE);
	System.err.println("**** FusekiStore.storeCans() load graphID: " + pGraphName);
	_conn.load(pGraphName, pModel);
	_conn.commit();
	_conn.end();	
    }
    
    public void deleteAnnotation(final String pAnnoId) throws IOException {
	// //_dataset.begin(ReadWrite.WRITE) ; // should probably move this to deleted state
	// //_dataset.removeNamedModel(pAnnoId);
	// //_dataset.commit();

	_conn.begin(ReadWrite.WRITE); // should probably move this to deleted state
	System.err.println("**** FusekiStore.deleteAnnotation() delete annoId: " + pAnnoId);
	try {
	    _conn.delete(pAnnoId);
	}
	catch (Exception e) {
	    // Javadoc on .delete() rather terse.
	    // At the time of writing, Javaoc did not specify what Exceptions it can throw
	    // Emperically it was found that deleting a graphName that did not exist
	    // was treated as a 404 error
	    //
	    // org.apache.jena.atlas.web.HttpException: 404 - Not Found
		
	    System.err.println("***** FusekiStore::deleteAnnotation(): " + pAnnoId + " did not exist");
	}
	_conn.commit();	
	_conn.end();

    }
    
    protected void begin(final ReadWrite pWrite) {
	_conn.begin(pWrite);
    }
    protected void end() {
	_conn.end();
    }

    protected QueryExecution getQueryExe(final String pQuery) {
	// // return QueryExecutionFactory.create(pQuery, _dataset);

	// return new QueryEngineHTTP(((HTTPRepository)_repo).getRepositoryURL(),pQuery);
	
	//Model tJsonLDModel = ModelFactory.createDefaultModel();
	//Query query = QueryFactory.create(pQuery) ;
	// //try {
	// QueryExecution qexec = QueryExecutionFactory.create(query, tJsonLDModel);
	/*    
	} catch (Exception tExcpt) {
	    _logger.error("Problem executing query " + tExcpt.getMessage());
	    tExcpt.printStackTrace();
	    throw new IOException("Problem connecting to Fuseki " + tExcpt.getMessage());
	} finally {
	}
	*/

	//_conn.begin(ReadWrite.WRITE); // err on the side of caution
	System.err.println("**** FusekiStore.getQueryExe() _conn.query() pQuery = " + pQuery);
	QueryExecution qexec = _conn.query(pQuery);
	//_conn.commit();
	//_conn.end();	

	return qexec;
	//Query query = QueryFactory.create(s2); //s2 = the query above
        //QueryExecution qExe = QueryExecutionFactory.sparqlService( "http://dbpedia.org/sparql", query );
        //ResultSet results = qExe.execSelect();
        //ResultSetFormatter.out(System.out, results, query) ;
	
    }
    
    protected Model getNamedModel(final String pContext) throws IOException {		
	/*boolean tLocaltransaction = !_dataset.isInTransaction();
	if (tLocaltransaction) {
	    _dataset.begin(ReadWrite.READ);
	}
	Model tAnnotation = _dataset.getNamedModel(pContext);
	if (tAnnotation.isEmpty()) {
	    tAnnotation = null; // annotation wasn't found
	}
	if (tLocaltransaction) {
	    _dataset.end();
	    }*/


	boolean tLocaltransaction = !_conn.isInTransaction();
	if (tLocaltransaction) {
	    _conn.begin(ReadWrite.READ);
	}
	System.err.println("***** FusekiStore::getNamedModel() pContext = " + pContext);
	Model tAnnotation = null;
	try {
	    tAnnotation = _conn.fetch(pContext);
	}
	catch (Exception e) {
	    // Javadoc on .fetch() rather terse.
	    // At the time of writing, Javaoc did not specify what Exceptions it can throw
	    // Emperically it was found that requesting a graphName that did not exist
	    // was treated as a 404 error
	    //
	    // org.apache.jena.atlas.web.HttpException: 404 - Not Found
		
	    System.err.println("***** FusekiStore::getNamedModel(): " + pContext + " did not exist");
	}

	if (tLocaltransaction) {
	    _conn.end();
	}
	
        return tAnnotation;
    }

    /**
     * index manifest but no need to check if the short id is unique as this has been checked else where
     */
    protected String indexManifestOnly(final String pShortId, Map<String,Object> pManifest) throws IOException {
	Model tJsonLDModel = ModelFactory.createDefaultModel();
	RDFDataMgr.read(tJsonLDModel, new ByteArrayInputStream(JsonUtils.toString(pManifest).getBytes(Charset.forName("UTF-8"))), Lang.JSONLD);
	
	// // //RDFDataMgr.write(System.out, tJsonLDModel, Lang.NQUADS);
	// //_dataset.begin(ReadWrite.WRITE) ;
	// //_dataset.addNamedModel((String)pManifest.get("@id"), tJsonLDModel);
	
	// //_dataset.commit();

	_conn.begin(ReadWrite.WRITE) ;
	System.err.println("**** FusekiStore.indexManifestOnly() load graphID: " + (String)pManifest.get("@id"));
	_conn.load((String)pManifest.get("@id"), tJsonLDModel);
	_conn.commit();
	_conn.end();	

	return pShortId;
    }

    public static void main(String[] args) {

	String pRepositoryURL = "https://intermuse.sowemustthink.space/greenstone3-lod3/";
	String jsonld_ifilename = "/tmp/openannotation-list.json";
	String at_id = "http-greenstone://intermuse/programmes-and-performers/HASH012cd965c3e83d504f4a78cd/openannotation-list.json";

	String pContext = "http-greenstone://intermuse/programmes-and-performers/HASH012cd965c3e83d504f4a78cd/annotation/gv-block-1";
	
	RDFConnection conn = RDFConnectionFuseki.newBuilder()
	    //.queryEndpoint(pRepositoryURL+"greenstone/query")
	    //.updateEndpoint(pRepositoryURL+"greenstone/update")
	    //.gspEndpoint(pRepositoryURL+"greenstone/data")
	    .queryEndpoint(pRepositoryURL+"greenstone")
	    .updateEndpoint(pRepositoryURL+"greenstone")
	    .gspEndpoint(pRepositoryURL+"greenstone")
	    .build();
	
	Model tJsonLDModel = ModelFactory.createDefaultModel();
	RDFDataMgr.read(tJsonLDModel, jsonld_ifilename, Lang.JSONLD);

	System.err.println("*** away to ingest JSON input");
	conn.begin(ReadWrite.WRITE);
	conn.load(at_id, tJsonLDModel);	
	conn.commit();
	conn.end();


	//System.err.println("*** away to conn.fetch(): " + pContext);
	System.err.println("*** away to conn.fetch(): " + at_id);
	conn.begin(ReadWrite.READ);
	//Model tAnnotation = conn.fetch(pContext);
	Model tAnnotation = conn.fetch(at_id);
	if (tAnnotation.isEmpty()) {
	    tAnnotation = null; // annotation wasn't found
	}
	conn.end();
	
    }
    
}

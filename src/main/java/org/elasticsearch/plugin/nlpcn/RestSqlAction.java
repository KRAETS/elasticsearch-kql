package org.elasticsearch.plugin.nlpcn;

import edu.mit.ll.aexp.AExpProcessor;
import edu.mit.ll.aexp.Result;
import edu.mit.ll.execution.QueryExecutor;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.nlpcn.executors.ActionRequestRestExecuterFactory;
import org.elasticsearch.plugin.nlpcn.executors.RestExecutor;
import org.elasticsearch.rest.*;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.query.QueryAction;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class RestSqlAction extends BaseRestHandler {

	@Inject
	public RestSqlAction(Settings settings, Client client, RestController restController) {
		super(settings, restController, client);
		restController.registerHandler(RestRequest.Method.POST, "/_kql/_explain", this);
		restController.registerHandler(RestRequest.Method.GET, "/_kql/_explain", this);
		restController.registerHandler(RestRequest.Method.POST, "/_kql/_explain_aexp", this);
        restController.registerHandler(RestRequest.Method.GET, "/_kql/_translateAexp", this);
		restController.registerHandler(RestRequest.Method.POST, "/_kql", this);
		restController.registerHandler(RestRequest.Method.GET, "/_kql", this);
	}

	@Override
	protected void handleRequest(RestRequest request, RestChannel channel, final Client client) throws Exception {
        if(request.path().endsWith("/_kql/_translateAexp")){
            String aexp = request.param("aexp");
            String translation = translateAexp(aexp);
            if(translation==null){
                BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, "");
                channel.sendResponse(bytesRestResponse);
            }
            BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, translation);
            channel.sendResponse(bytesRestResponse);
            return;
        }
        String sql = request.param("kql");
        if (sql == null) {
            sql = request.content().toUtf8();
        }
        String originalSQL = new String(sql);
        //KQL Extension
        try {
            sql = translateKQL(sql);
        } catch (Exception e) {
            System.out.println("Could not parse kql");
            BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "No parse" + e.toString());
            channel.sendResponse(bytesRestResponse);
        }

        if (request.param("limit") == null)
            sql += " limit 10000";
        else
            sql += " limit " + request.param("limit");

        //		sql += " limit 5000";
        //		BytesRestResponse bytesRestResponse1 = new BytesRestResponse(RestStatus.OK, "Parsed:"+sql);
        //		channel.sendResponse(bytesRestResponse1);

        //END Kql Extension
        SearchDao searchDao = new SearchDao(client);
        QueryAction queryAction = searchDao.explain(sql);


        // TODO add unittests to explain. (rest level?)
        if (request.path().endsWith("/_explain")) {
            String jsonExplanation = queryAction.explain().explain();
            BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, jsonExplanation);
            channel.sendResponse(bytesRestResponse);
        } else if (request.path().endsWith("/_explain_aexp")) {
            String aExpExplanation = explainKQL(originalSQL);
            BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, aExpExplanation);
            channel.sendResponse(bytesRestResponse);
        }
        else {
            Map<String, String> params = request.params();
            RestExecutor restExecutor = ActionRequestRestExecuterFactory.createExecutor(params.get("format"));
            restExecutor.execute(client, params, queryAction, channel);
        }

	}

    private String translateAexp(String aexp) throws Exception{
        final String kqlQueryFinal = aexp;
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }
        String user = AccessController.doPrivileged(
            new  PrivilegedExceptionAction<String>() {
                public String run() throws Exception{
                    try {
                        QueryExecutor ex = new QueryExecutor();
                        ex.setFolderLocation("jsonnads/");
                        ex.setAexpMapFolderLocation("plugins/kql/jsonnads/");
                        ex.enableCaseInsensitive(true);
                        ex.enableDebug(true);
                        ex.setReturnResultObject(true);

                        List<Result> res = ex.evaluateAexp(kqlQueryFinal);
                        System.out.println(res);
                        if (res.get(0) != null) {
                            return res.get(0).toJsonString();
                        } else return "ERROR";
                    }catch (Exception e){
                        return e.toString();
                    }
                }
            }
        );
        return user;
    }

    private String explainKQL(String kqlQuery) throws Exception{
		final String kqlQueryFinal = kqlQuery;
		SecurityManager sm = System.getSecurityManager();
		if (sm != null) {
			// unprivileged code such as scripts do not have SpecialPermission
			sm.checkPermission(new SpecialPermission());
		}

		String user = AccessController.doPrivileged(
			new  PrivilegedExceptionAction<String>() {
				public String run() throws Exception {
					QueryExecutor executor = new QueryExecutor();
					executor.enableDebug(true);

					executor.setQuery(kqlQueryFinal);
					System.err.println("Trying to set up the folder location");
					executor.setFolderLocation("jsonnads/");
					executor.setAexpMapFolderLocation("plugins/kql/jsonnads/");
					executor.enableCaseInsensitive(true);
					String conversion = "";
					conversion = executor.queryProvenance();
					return conversion;
				}
			}
		);
		return  user;

	}
	private String translateKQL(String kqlQuery) throws Exception{
		final String kqlQueryFinal = kqlQuery;
		SecurityManager sm = System.getSecurityManager();
		if (sm != null) {
			// unprivileged code such as scripts do not have SpecialPermission
			sm.checkPermission(new SpecialPermission());
		}

		String user = AccessController.doPrivileged(
			new  PrivilegedExceptionAction<String>() {
				public String run() throws Exception{
					Logger log = Logger.getLogger(RestSqlAction.class.getName());
					log.log(Level.INFO,"Hola");
					System.err.println("Creating the object");
					System.err.println("KQL Query"+kqlQueryFinal);
					QueryExecutor executor = new QueryExecutor();
					executor.enableDebug(true);

					executor.setQuery(kqlQueryFinal);
					System.err.println("Trying to set up the folder location");
					executor.setFolderLocation("jsonnads/");
					executor.setAexpMapFolderLocation("plugins/kql/jsonnads/");
					executor.enableCaseInsensitive(true);
					String conversion = "";
					conversion = executor.translateQuery(kqlQueryFinal);
					conversion=conversion.trim();
					System.out.println("Converted query to:"+conversion);
					return conversion;
				}
			}
		);
		return user;
	}
}
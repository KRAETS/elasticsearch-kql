package org.elasticsearch.plugin.nlpcn;

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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class RestSqlAction extends BaseRestHandler {

	@Inject
	public RestSqlAction(Settings settings, Client client, RestController restController) {
		super(settings, restController, client);
		restController.registerHandler(RestRequest.Method.POST, "/_kql/_explain", this);
		restController.registerHandler(RestRequest.Method.GET, "/_kql/_explain", this);
		restController.registerHandler(RestRequest.Method.POST, "/_kql", this);
		restController.registerHandler(RestRequest.Method.GET, "/_kql", this);
	}

	@Override
	protected void handleRequest(RestRequest request, RestChannel channel, final Client client) throws Exception {

		String sql = request.param("kql");

		if (sql == null) {
			sql = request.content().toUtf8();
		}
		//KQL Extension
		try {
			sql = translateKQL(sql);
		}
		catch (Exception e){
			System.out.println("Could not parse kql");
		}
		sql += " limit "+request.param("limit");
		//END Kql Extension
		SearchDao searchDao = new SearchDao(client);
        QueryAction queryAction= searchDao.explain(sql);

		// TODO add unittests to explain. (rest level?)
		if (request.path().endsWith("/_explain")) {
			String jsonExplanation = queryAction.explain().explain();
			BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, jsonExplanation);
			channel.sendResponse(bytesRestResponse);
		} else {
            Map<String, String> params = request.params();
            RestExecutor restExecutor = ActionRequestRestExecuterFactory.createExecutor(params.get("format"));
			restExecutor.execute(client,params,queryAction,channel);
		}
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
//				String[] ov = {};
//				List<String> querylist = new ArrayList<>(Arrays.asList(ov));
//				executor.setQueryList(querylist);

//				executor.setOutputFile(new File(cmd.getOptionValue("out")));

					executor.enableCaseInsensitive(true);
					String conversion = "";
//					try {
						conversion = executor.translateQuery(kqlQueryFinal);
//					}
//					catch (Exception e){
//						throw new Exception("Struggling"+e.toString());
//					}
					conversion=conversion.trim();
//					conversion = conversion.replaceAll("'","");
					System.out.println("Converted query to:"+conversion);
					return conversion;
				}
			}
		);
		return user;
	}
}
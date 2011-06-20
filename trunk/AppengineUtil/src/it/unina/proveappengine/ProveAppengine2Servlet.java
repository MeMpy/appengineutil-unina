package it.unina.proveappengine;

import it.unina.tools.datastore.ByteArrayDataClass;
import it.unina.tools.datastore.DatastoreLoadAndSave;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.TaskOptions;


@SuppressWarnings("serial")
public class ProveAppengine2Servlet extends HttpServlet {

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		
		Queue q= QueueFactory.getDefaultQueue();
		TaskOptions t=TaskOptions.Builder.withUrl("/deletetask");
		
		q.add(t);
		
		resp.setContentType("text/plain");
		resp.getWriter().print("cancellazione accodata ");
		
	}
	
}

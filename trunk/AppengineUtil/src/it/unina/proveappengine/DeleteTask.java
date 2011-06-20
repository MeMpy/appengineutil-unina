package it.unina.proveappengine;

import it.unina.tools.datastore.ByteArrayDataClass;
import it.unina.tools.datastore.DatastoreLoadAndSave;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

	@SuppressWarnings("serial")
	public class DeleteTask extends HttpServlet {

		@Override
		protected void doPost(HttpServletRequest req, HttpServletResponse resp)
				throws ServletException, IOException {
			
			Logger log= Logger.getLogger(DeleteTask.class.getName());
			
			DatastoreLoadAndSave s1b=new DatastoreLoadAndSave();
			Long num=s1b.removeAllViaQuery(ByteArrayDataClass.class.getName());
			
			log.warning("Entità cancellate: "+ num);
		}
		
	}
	
	

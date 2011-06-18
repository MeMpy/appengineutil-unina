package it.unina.proveappengine;

import it.unina.model.Chromosome;
import it.unina.model.TestCaseChromosome;
import it.unina.model.TestCaseInt;
import it.unina.tools.datastore.ByteArrayDataClass;
import it.unina.tools.datastore.DatastoreLoadAndSave;
import it.unina.tools.datastore.DatastoreLoadAndSaveWithTransaction;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.*;

import org.datanucleus.store.appengine.DatastoreManager;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

@SuppressWarnings("serial")
public class ProveAppengineServlet extends HttpServlet {
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		resp.setContentType("text/plain");
		resp.getWriter().println("Hello, world");
		
		ByteArrayDataClass b=new ByteArrayDataClass(new byte[10], "cazzo");
//		b.setNumRows(2);
		ByteArrayDataClass b2=new ByteArrayDataClass(new byte[10], "cazzo");
//		System.out.println(b.getKey());
		
		List<ByteArrayDataClass> lb= new LinkedList<ByteArrayDataClass>();
		lb.add(b);
		lb.add(b2);
		DatastoreLoadAndSave s=new DatastoreLoadAndSave();
		DatastoreLoadAndSaveWithTransaction s2=new DatastoreLoadAndSaveWithTransaction();
		s.saveAll(lb);
				
		s2.openTransaction();
		try{
			ByteArrayDataClass b3=s2.loadObjectById(ByteArrayDataClass.class, b.getKey().getId());
			
//			ByteArrayDataClass b4=s2.loadObjectById(ByteArrayDataClass.class, b2.getKey().getId());
			
			System.out.println(b3.getKey());
//			System.out.println(b4.getKey());
			
			s2.commitTransaction();
		}finally{
			s2.rollbackTransaction();
		}
		
//		byte[] data=s.load("prova");
		
		
		
//		List<ByteArrayDataClass> data=s.load("title", "prova", ByteArrayDataClass.class);
		
//		Map<String, Object> m = new HashMap<String, Object>();
//		Map<String, Object> m2 = null;
//		m.put("title", "prova");
//		m.put("numRows", 2);
//		DatastoreLoadAndSave s2=new DatastoreLoadAndSave();
//		List<ByteArrayDataClass> data=(List<ByteArrayDataClass>)s2.load(m2, ByteArrayDataClass.class);
//		System.out.println(data.size());
//		data.size();
//		ByteArrayDataClass b2= data.get(0);
		
//		ByteArrayDataClass b2=s2.loadObjectById(ByteArrayDataClass.class, 1);
		
//		b2.setTitle("porci");
//		DatastoreLoadAndSave s0=new DatastoreLoadAndSave();
//		s0.save(b2);
		
//		s0.removeById(ByteArrayDataClass.class, b2.getKey().getId());
		
//		DatastoreLoadAndSave s3=new DatastoreLoadAndSave();
//		
//		ByteArrayDataClass b3=s3.getObjectById(ByteArrayDataClass.class, 1);
//		
//		System.out.println(b3.getTitle());
		
//		URL u=this.getServletContext().getResource("/configurations/prova.xml");
//		InputStreamReader reader= new InputStreamReader(u.openStream());
//		BufferedReader br= new BufferedReader(reader);
//		String line= br.readLine();
//		while (line!=null){
//			System.out.println(line);
//			line=br.readLine();
//		}
		
//		ByteArrayDataClass b4=new ByteArrayDataClass(new byte[10], "prova1");
//		DatastoreLoadAndSave s4=new DatastoreLoadAndSave();
//		s4.save(b4);
//		ByteArrayDataClass btemp=s4.loadObjectById(ByteArrayDataClass.class,1);
//		ByteArrayDataClass b5= new ByteArrayDataClass(new byte[10], "provadiprova");
//		b5.setKey(btemp.getKey());
//		s4.save(b5);
		
//		DatastoreLoadAndSave s5=new DatastoreLoadAndSave();
//		Chromosome c= new TestCaseChromosome(new LinkedList<TestCaseInt>(),5d,1,1,new byte[10],"1","prova",1d);
//		
//		s5.save(c);
//		
//		DatastoreService db=DatastoreServiceFactory.getDatastoreService();
//		Entity ent=null;
//		try {
//			ent= db.get(KeyFactory.createKey(TestCaseChromosome.class.getSimpleName(), 2));
//		} catch (EntityNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		System.out.println(ent.getKey());
//		
//		Chromosome c2=convertEntityToChromosome(ent);
//		System.out.println(c);
		
		
		
		
		
		
		
	}
	
	
	public Chromosome convertEntityToChromosome(Entity ent){
		
		TestCaseChromosome realType= new TestCaseChromosome();
		
		
		realType.setKey((Key)ent.getProperty("key"));
		realType.setFitness((Double)ent.getProperty("fitness"));
		realType.setGenerationId(((Long)ent.getProperty("generationId")).intValue());
		realType.setGenes((List<TestCaseInt>)ent.getProperty("genes"));
		realType.setIslandId(((Long)ent.getProperty("islandId")).intValue());
		realType.setMutationProbabiliy((Double)ent.getProperty("mutationProbability"));
		realType.setTestSessionContainerAfterTestBytes((byte[])ent.getProperty("sessionContainerAfterTestBytes"));
		realType.setTestSessionContainerFileName((String)ent.getProperty("testSessionContainerFileName"));
		realType.setTestSessionContainerId((String)ent.getProperty("testSessionContainerId"));
		
			
		return realType;
	}
	
}

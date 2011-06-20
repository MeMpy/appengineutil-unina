package it.unina.proveappengine;

import it.unina.tools.datastore.ByteArrayDataClass;
import it.unina.tools.datastore.DatastoreLoadAndSave;
import it.unina.tools.datastore.DatastoreLoadAndSaveWithTransaction;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

@SuppressWarnings("serial")
public class ProveAppengineServlet extends HttpServlet {
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		resp.setContentType("text/plain");
		resp.getWriter().println("Hello, world");
		
	
//		b.setNOumRows(2);
//		ByteArrayDataClass b2=new ByteArrayDataClass(new byte[10], "cazzo");
//		System.out.println(b.getKey());
		List<ByteArrayDataClass> lb= new LinkedList<ByteArrayDataClass>();
		for(int i=0;i<50;i++){
			ByteArrayDataClass b=new ByteArrayDataClass(new byte[10], "cazzo");
			lb.add(b);
		}
		
		
//		lb.add(b2);
		DatastoreLoadAndSave s=new DatastoreLoadAndSave();
//		DatastoreLoadAndSaveWithTransaction s2=new DatastoreLoadAndSaveWithTransaction();
		s.saveAll(lb);
		
		List<Key> lk= new LinkedList<Key>();
		for(ByteArrayDataClass b : lb){
			lk.add(b.getKey());
		}
		
		s.removeAllByKeys(lk,50);
		
		lb = s.loadWithGenerics(null, ByteArrayDataClass.class);

		System.out.println(lb.size());
//		
		
//		s.removeByKey(lb.get(0).getKey());
		
		
//		DatastoreLoadAndSave s1b=new DatastoreLoadAndSave();
//		System.out.println(s1b.removeAllViaQuery(ByteArrayDataClass.class.getName()));
//				
//		s2.openTransaction();
//		try{
//			ByteArrayDataClass b3=s2.loadObjectById(ByteArrayDataClass.class, b.getKey().getId());
//			
////			ByteArrayDataClass b4=s2.loadObjectById(ByteArrayDataClass.class, b2.getKey().getId());
//			
//			System.out.println(b3.getKey());
////			System.out.println(b4.getKey());
//			
//			s2.commitTransaction();
//		}finally{
//			s2.rollbackTransaction();
//		}
//		
//		byte[] data=s.load("prova");
		
//		lb = s.loadWithGenerics(null, ByteArrayDataClass.class);
//		System.out.println(lb.get(0));
//		
//		System.out.println(lb.size());
//		
		
		
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

//			e.printStackTrace();
//		}
//		
//		System.out.println(ent.getKey());
//		
//		Chromosome c2=convertEntityToChromosome(ent);
//		System.out.println(c);
		
		
		
		
		
		
		
	}
	
	
//	public Chromosome convertEntityToChromosome(Entity ent){
//		
//		TestCaseChromosome realType= new TestCaseChromosome();
//		
//		
//		realType.setKey((Key)ent.getProperty("key"));
//		realType.setFitness((Double)ent.getProperty("fitness"));
//		realType.setGenerationId(((Long)ent.getProperty("generationId")).intValue());
//		realType.setGenes((List<TestCaseInt>)ent.getProperty("genes"));
//		realType.setIslandId(((Long)ent.getProperty("islandId")).intValue());
//		realType.setMutationProbabiliy((Double)ent.getProperty("mutationProbability"));
//		realType.setTestSessionContainerAfterTestBytes((byte[])ent.getProperty("sessionContainerAfterTestBytes"));
//		realType.setTestSessionContainerFileName((String)ent.getProperty("testSessionContainerFileName"));
//		realType.setTestSessionContainerId((String)ent.getProperty("testSessionContainerId"));
//		
//			
//		return realType;
//	}
	
}

package it.unina.tools.datastore;

import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

public class DatastoreLoadAndSaveForByteArrayDataClass {

	
	
	/**
	 * Metodo per caricare un array di byte dato il suo identificativo
	 * 
	 * @param name
	 *            (identificativo human-readable dell'array di byte)
	 * @return
	 */
	public byte[] load(String name) {

		PersistenceManager pm = PMF.get().getPersistenceManager();
		Query query = pm.newQuery(ByteArrayDataClass.class);
		query.setFilter("title == titleParam");
		query.declareParameters("String titleParam");

		ByteArrayDataClass dataToLoad = null;

		try {
			List<ByteArrayDataClass> results = (List<ByteArrayDataClass>) query
					.execute(name);
			if (!results.isEmpty()) {
				for (ByteArrayDataClass e : results) {
					dataToLoad = e;
				}
			} else {
				// ... no results ...
			}
		} finally {
			query.closeAll();
			pm.close();
		}

		return dataToLoad.getFile();

	}
	
	
	/**
	 * Metodo per salvare un array di byte con il relativo nome Crea un entità
	 * di tipo ByteArrayDataClass
	 * 
	 * @param bytes
	 * @param name
	 *            (identificativo human-readable dell'array di byte)
	 */
	public void save(byte[] bytes, String name) {

		ByteArrayDataClass dataToStore = new ByteArrayDataClass(bytes, name);
		PersistenceManager pm = PMF.get().getPersistenceManager();

		try {
			pm.makePersistent(dataToStore);
		} finally {
			pm.close();
		}
	}

	
}

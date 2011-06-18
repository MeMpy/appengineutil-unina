package it.unina.tools.datastore;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.datanucleus.store.appengine.query.JDOCursorHelper;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Key;

/**
 * La classe fornisce i metodi load and save per caricare e salvare dal e nel
 * datastore entità che mantengono array di byte oppure oggetti generici.
 * 
 * @author ross
 * 
 */
public class DatastoreLoadAndSave {

	/**
	 * Questo attributo mantiene l'istanza del persistencemanager qunado si fa
	 * lazyload E' necessario memorizzarlo per poterlo chiudere successivamente.
	 */
	PersistenceManager pmToclose = null;

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
	 * Metodo generico per salvare un oggetto
	 * 
	 * @param obj
	 */
	public void save(Object obj) {

		PersistenceManager pm = PMF.get().getPersistenceManager();

		try {
			pm.makePersistent(obj);
		} finally {
			pm.close();
		}
	}

	/**
	 * Metodo generico per salvare una lista di oggetti
	 * 
	 * @param obj
	 */
	public void saveAll(List<?> obj) {

		PersistenceManager pm = PMF.get().getPersistenceManager();

		try {
			pm.makePersistentAll(obj);
		} finally {
			pm.close();
		}
	}

	/**
	 * Metodo generico per caricare uno o più oggetti data una mappa di
	 * parametri rappresentanti coppie (attributo, valore) che serviranno per
	 * filtrare la query. Il filtro creato sarà un and di equivalenze.
	 * 
	 * @param <T>
	 *            Tipo degli oggetti restituiti da load
	 * @param params
	 *            Mappa contenente la coppia chiave valore per costruire il
	 *            filtro la coppia chiave valore della mappa è così formata:
	 *            chiave è una stringa rappresentnate il nome del campo
	 *            dell'oggetto il valore corrispondente è il valore che gli
	 *            oggetti cercati devono matchare il valore può essere di tipo
	 *            stringa o numerico.
	 * @return la lista di oggetti che condividono lo stesso identificativo
	 */
	public <T> List<T> loadWithGenerics(Map<String, Object> params,
			Class<T> type) {

		PersistenceManager pm = PMF.get().getPersistenceManager();
		Query query = pm.newQuery(type);

		// Definiamo il cursore per scorrere gli oggetti della query 500 alla
		// volta
		Map<String, Object> extensionMap = new HashMap<String, Object>();
		Cursor cursor;

		// creiamo il filtro e settiamolo se sono stati passati i parametri
		if (params != null) {
			String filter = createFilter(params);
			query.setFilter(filter);
		}// altrimenti la query viene effettuata senza filtro su tutte le entità

		List<T> results = null;
		List<T> returnList = null;
		try {

			query.setRange(0, 500);

			results = (List<T>) query.execute();
			returnList = new LinkedList<T>();
			while (results.size() != 0) {
				// Use the first 500 results...
				returnList.addAll(results);

				cursor = JDOCursorHelper.getCursor(results);

				extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, cursor);

				query.setExtensions(extensionMap);

				query.setRange(0, 500);

				results = (List<T>) query.execute();

			}
		}

		finally {
			query.closeAll();
			pm.close();

		}

		return returnList;

	}

	/**
	 * Metodo per caricare uno o più oggetti data una mappa di parametri
	 * rappresentanti coppie (attributo, valore) che serviranno per filtrare la
	 * query. Il filtro creato sarà un and di equivalenze. Se la mappa è null il
	 * filtro nn verrà creato e si farà una query che recupererà tutti gli
	 * oggetti della classe. Il metodo non effettua il lazy loading in quanto
	 * verrà invocato il metodo size() della lista dei risultati. Questo è
	 * necessario per poter effettuare salvataggi degli oggetti recuperati in
	 * momenti immediatamente successivi.
	 * 
	 * @param Class
	 *            <?> Tipo degli oggetti restituiti da load
	 * @param params
	 *            Mappa contenente la coppia chiave valore per costruire il
	 *            filtro la coppia chiave valore della mappa è così formata:
	 *            chiave è una stringa rappresentnate il nome del campo
	 *            dell'oggetto il valore corrispondente è il valore che gli
	 *            oggetti cercati devono matchare il valore può essere di tipo
	 *            stringa o numerico.
	 * @return la lista di oggetti che condividono lo stesso identificativo
	 */
	public Object load(Map<String, Object> params, Class<?> type) {

		PersistenceManager pm = PMF.get().getPersistenceManager();
		Query query = pm.newQuery(type);

		// Definiamo il cursore per scorrere gli oggetti della query 500 alla
		// volta
		Map<String, Object> extensionMap = new HashMap<String, Object>();
		Cursor cursor;

		// creiamo il filtro e settiamolo se sono stati passati i parametri
		if (params != null) {
			String filter = createFilter(params);
			query.setFilter(filter);
		}// altrimenti la query viene effettuata senza filtro su tutte le entità

		List<Object> results = null;
		List<Object> returnList = null;
		try {

			query.setRange(0, 500);

			results = (List<Object>) query.execute();
			returnList = new LinkedList<Object>();
			while (results.size() != 0) {
				// Use the first 500 results...
				returnList.addAll(results);

				cursor = JDOCursorHelper.getCursor(results);

				extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, cursor);

				query.setExtensions(extensionMap);

				query.setRange(0, 500);

				results = (List<Object>) query.execute();

			}

		} finally {
			query.closeAll();
			pm.close();

		}

		return returnList;

	}

	private String createFilter(Map<String, Object> params) {
		String filter = "";

		if (params != null) {

			// Creazione filtro per la query
			Set<String> fieldsName = params.keySet();
			Object value;

			for (String field : fieldsName) {
				value = params.get(field);
				if (value instanceof String) {
					filter += field + " == '" + value + "' && ";
				} else {
					filter += field + " == " + value + " && ";
				}
			}

			// elimino l'and finale
			filter = filter.substring(0, filter.length() - 4);

		}
		return filter;
	}

	/**
	 * Metodo utilizzato per ritornare un oggetto generico dato il suo id
	 * Tipicamente l'id è l'identificativo incluso all'interno della chiave
	 * creata da GAE. Tipicamente è un long ma può essere una stringa o un
	 * oggetto a discrezione del programmatore.
	 * 
	 * @param <T>
	 *            Tipo dell'oggetto ritornato
	 * @param type
	 *            Classe dell'oggetto da cercare
	 * @param id
	 *            Identificativo dell'oggetto.
	 * @return l'oggetto dal datastore
	 */
	public <T> T loadObjectById(Class<T> type, Object id) {
		PersistenceManager pm = PMF.get().getPersistenceManager();

		T dataToLoad = null;

		try {
			dataToLoad = pm.getObjectById(type, id);
		} finally {
			pm.close();
		}

		return dataToLoad;

	}

	/**
	 * Metodo per rimuovere l'oggetto passato in input TODO: Per ora il metodo
	 * recupera l'oggetto dal db e lo cancella, problema di efficienza in quanto
	 * si fa una query in più al db.
	 * 
	 * @param objToRemove
	 */
	public void removeById(Class<?> classToRemove, Object id) {
		PersistenceManager pm = PMF.get().getPersistenceManager();

		try {
			pm.deletePersistent(pm.getObjectById(classToRemove, id));

		} finally {
			pm.close();
		}

	}

	/**
	 * Il metodo cancella tutte le entità della classe classToRemove, utilizza
	 * un cursore per scorrerle tutte, in modo da poter cancellare anche un
	 * numero molto grande di entità.
	 * 
	 * @param classToRemove
	 */
	public void removeAll(String classToRemove) {
		String kindToDelete = classToRemove;

		PersistenceManager pm = PMF.get().getPersistenceManager();
		Map<String, Object> extensionMap = new HashMap<String, Object>();
		Cursor cursor;

		try {

			Query query = pm.newQuery("select from " + kindToDelete);
			query.setRange(0, 500);

			List<Object> results = (List<Object>) query.execute();
			while (results.size() != 0) {
				// Use the first 500 results...
				pm.deletePersistentAll(results);

				cursor = JDOCursorHelper.getCursor(results);

				extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, cursor);

				query.setExtensions(extensionMap);

				query.setRange(0, 500);

				results = (List<Object>) query.execute();

			}

		} finally {

			pm.close();
		}

	}

	/**
	 * Il metodo chiude il persistencemanager utilizzato per fare il lazyload Il
	 * metodo funziona solo dopo aver utilizzato il metodo lazyload altrimenti
	 * non chiuderà nulla dato che nulla può essere chiuso.
	 */
	public void closePm() {
		if (pmToclose != null) {
			if (!pmToclose.isClosed()) {
				pmToclose.close();
			}
		}
	}

	/**
	 * Metodo per caricare uno o più oggetti data una mappa di parametri
	 * rappresentanti coppie (attributo, valore) che serviranno per filtrare la
	 * query. Il filtro creato sarà un and di equivalenze. Se la mappa è null il
	 * filtro nn verrà creato e si farà una query che recupererà tutti gli
	 * oggetti della classe. Il metodo non effettua il lazy loading in quanto
	 * verrà invocato il metodo size() della lista dei risultati. Questo è
	 * necessario per poter effettuare salvataggi degli oggetti recuperati in
	 * momenti immediatamente successivi.
	 * 
	 * @param Class
	 *            <?> Tipo degli oggetti restituiti da load
	 * @param params
	 *            Mappa contenente la coppia chiave valore per costruire il
	 *            filtro la coppia chiave valore della mappa è così formata:
	 *            chiave è una stringa rappresentnate il nome del campo
	 *            dell'oggetto il valore corrispondente è il valore che gli
	 *            oggetti cercati devono matchare il valore può essere di tipo
	 *            stringa o numerico.
	 * @return la lista di oggetti che condividono lo stesso identificativo
	 */
	public Object lazyLoad(Map<String, Object> params, Class<?> type) {

		PersistenceManager pm = PMF.get().getPersistenceManager();
		Query query = pm.newQuery(type);

		String filter = "";

		if (params != null) {

			// Creazione filtro per la query
			Set<String> fieldsName = params.keySet();
			Object value;

			for (String field : fieldsName) {
				value = params.get(field);
				if (value instanceof String) {
					filter += field + " == '" + value + "' && ";
				} else {
					filter += field + " == " + value + " && ";
				}
			}

			// elimino l'and finale
			filter = filter.substring(0, filter.length() - 4);
			query.setFilter(filter);
		}

		Object results = null;
		try {
			results = query.execute();

		} finally {

			// pm.close()
			query.closeAll();
			pmToclose = pm;

		}

		return results;

	}

}

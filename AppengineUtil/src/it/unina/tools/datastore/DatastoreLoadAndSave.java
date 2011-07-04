package it.unina.tools.datastore;


import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.datanucleus.store.appengine.query.JDOCursorHelper;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.mapreduce.MapReduceState;

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
	 * Metodo per costruire una chiave data la classe e un long; Tale metodo
	 * costruisce una chiave simile a quella di default di google Permette però
	 * di utilizzare un proprio id univoco e non quello fornito da appengine
	 * 
	 * @param kind
	 *            oggetto class dell'entità cui generare la chiave
	 * @param id
	 *            univoco.
	 * @return
	 */
	public static Key generateKey(Class<?> kind, Long id) {
		Key key = KeyFactory.createKey(kind.getSimpleName(), id);
		return key;
	}
	
	
	/**
	 * Metodo per costruire una lista di chiavi data la classe, il punto iniziale
	 * da cui partire per enumerare gli id e il numero totale di chiavi da creare
	 * 
	 * @param startPoint indice da cui iniziare a numerare le chiavi
	 * @param numKeys numero totale di chiavi
	 * @param kind oggetto class dell'entità cui generare la chiave
	 * @return
	 */
	public static List<Key> generateKeys(Long startPoint, Integer numKeys,
			Class<?> kind) {
		List<Key> keyList = new LinkedList<Key>();
		for (Long i = startPoint; i < startPoint + numKeys; i++) {
			keyList.add(generateKey(kind, i));
		}
		return keyList;
	}
	
	
	
	
	/**
	 * Il metodo serve per retrocompatibilità, assegna automaticamente il valore 500
	 * al parametro range.
	 * Per ulteriori informazioni vedi documentazione del metodo 
	 * load(Map<String, Object> params, Class<?> type, Integer range)
	 * 
	 * @param params
	 * @param type
	 * @return
	 */
	public Object load(Map<String, Object> params, Class<?> type) {
		return load(params, type, 500);
	}

	/**
	 * Metodo per caricare uno o più oggetti data una mappa di parametri
	 * rappresentanti coppie (attributo, valore) che serviranno per filtrare la
	 * query. Se la mappa è null il
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
	 *            stringa o numerico oppure può essere di tipo
	 *            {@link OpAndValue} se si desidera effettuare una query in cui
	 *            non si usa per il campo corrispondente un operatore di
	 *            eguaglianza
	 * @param range
	 *            indica quante entità alla volta devono essere caricate
	 * @return la lista di oggetti che condividono lo stesso identificativo
	 */
	public Object load(Map<String, Object> params, Class<?> type, Integer range) {

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

			query.setRange(0, range);

			results = (List<Object>) query.execute();
			returnList = new LinkedList<Object>();
			while (results.size() != 0) {
				// Use the first range results...
				returnList.addAll(results);

				cursor = JDOCursorHelper.getCursor(results);

				extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, cursor);

				query.setExtensions(extensionMap);

				query.setRange(0, range);

				results = (List<Object>) query.execute();

			}

		} finally {
			query.closeAll();
			pm.close();

		}

		return returnList;

	}

	/**
	 * Metodo per caricare uno o più oggetti data una mappa di parametri
	 * rappresentanti coppie (attributo, valore) che serviranno per filtrare la
	 * query e dato un ordine. Se
	 * la mappa è null il filtro nn verrà creato e si farà una query che
	 * recupererà tutti gli oggetti della classe. Il metodo non effettua il lazy
	 * loading in quanto verrà invocato il metodo size() della lista dei
	 * risultati. Questo è necessario per poter effettuare salvataggi degli
	 * oggetti recuperati in momenti immediatamente successivi.
	 * 
	 * @param Class
	 *            <?> Tipo degli oggetti restituiti da load
	 * @param params
	 *            Mappa contenente la coppia chiave valore per costruire il
	 *            filtro la coppia chiave valore della mappa è così formata:
	 *            chiave è una stringa rappresentnate il nome del campo
	 *            dell'oggetto il valore corrispondente è il valore che gli
	 *            oggetti cercati devono matchare il valore può essere di tipo
	 *            stringa o numerico oppure può essere di tipo
	 *            {@link OpAndValue} se si desidera effettuare una query in cui
	 *            non si usa per il campo corrispondente un operatore di
	 *            eguaglianza
	 * @param range
	 *            indica quante entità alla volta devono essere caricate
	 * @param ordering
	 *            specifica che tipo di ordinamento devono avere le entità
	 * 
	 * @return la lista di oggetti che condividono lo stesso identificativo
	 */
	public Object load(Map<String, Object> params, Class<?> type,
			Integer range, String ordering) {

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

		// Settiamo l'ordine se è stato inserito.
		if (ordering != null) {
			ordering.trim();
			if (!ordering.equals("")) {
				query.setOrdering(ordering);
			}
		}

		List<Object> results = null;
		List<Object> returnList = null;
		try {

			query.setRange(0, range);

			results = (List<Object>) query.execute();
			returnList = new LinkedList<Object>();
			while (results.size() != 0) {
				// Use the first range results...
				returnList.addAll(results);

				cursor = JDOCursorHelper.getCursor(results);

				extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, cursor);

				query.setExtensions(extensionMap);

				query.setRange(0, range);

				results = (List<Object>) query.execute();

			}

		} finally {
			query.closeAll();
			pm.close();

		}

		return returnList;

	}

	/**
	 * Metodo ausiliario per costruire il filtro dati i parametri.
	 * 
	 * @param params
	 * @return
	 */
	private String createFilter(Map<String, Object> params) {
		String filter = "";

		if (params != null) {

			// Creazione filtro per la query
			Set<String> fieldsName = params.keySet();
			Object value;

			// Per ogni entri della mappa prendiamo il campo su cui cercare
			for (String field : fieldsName) {
				// prendiamo il valore da cercare
				value = params.get(field);
				// se tale valore è una coppia (operatore,valore) OpAndValue
				if (value instanceof OpAndValue) {
					// allora richiamiamo il metodo per creare il filtro con
					// tale coppia
					OpAndValue opAndValue = (OpAndValue) value;
					filter += createFilterWithOperators(field, opAndValue);
				} else
				// altrimenti eseguiamo i controlli di routine per settare il
				// filtro
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
	 * Metodo ausiliario per costruire la parte del filtro che richiede anche un
	 * operatore diverso da quello di uguaglianza
	 * 
	 * @param field
	 * @param opAndValue
	 *            è la coppia operatore valore da settare.
	 * @return
	 */
	private String createFilterWithOperators(String field, OpAndValue opAndValue) {
		String filter = "";
		String op = opAndValue.getOperator();
		Object value = opAndValue.getValue();
		if (value instanceof String) {
			filter += field + " " + op + " '" + value + "' && ";
		} else {
			filter += field + " " + op + " " + value + " && ";
		}

		return filter;
	}

	/**
	 * Metodo utilizzato per ritornare un oggetto generico dato il suo id
	 * Tipicamente l'id è l'identificativo incluso all'interno della chiave
	 * creata da GAE. Tipicamente è un long ma può essere una stringa o un
	 * oggetto a discrezione del programmatore.
	 * 
	 * @param <T>TODO: handle exception
	 *            Tipo dell'oggetto ritornato
	 * @param type
	 *            Classe dell'oggetto da cercare
	 * @param id
	 *            Identificativo dell'oggetto.
	 * @return l'oggetto dal datastore, se tale oggetto non è presente allora ritorna null.
	 */
	public <T> T loadObjectById(Class<T> type, Object id) {
		PersistenceManager pm = PMF.get().getPersistenceManager();

		T dataToLoad = null;

		try {
			dataToLoad = pm.getObjectById(type, id);
		} catch (JDOObjectNotFoundException e) {
			dataToLoad=null;
		}finally {
			pm.close();
		}

		return dataToLoad;

	}

	/**
	 * Metodo per rimuovere l'oggetto passato in input recupera l'oggetto dal db
	 * e lo cancella, problema di efficienza in quanto si fa una query in più al
	 * db.
	 * 
	 * @param classToRemove
	 *            , la classe dell'oggetto da rimuovere, serve per effettuare la
	 *            query per recuperare l'oggetto
	 * @param id
	 *            key.getId() solitamente un Long utilizzato per identificare
	 *            l'oggetto.
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
	 * Metodo per rimuovere l'oggetto passato in input. Accetta la chiave
	 * dell'oggetto e lo rimuove utilizzando una low-level api per risolvere il
	 * problema di efficienza
	 * 
	 * @param keyToRemove
	 */
	public void removeByKey(Key key) {

		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

		ds.delete(key);

	}

	/**
	 * Il metodo cancella tutte le entità della classe classToRemove, utilizza
	 * un cursore per scorrerle tutte, in modo da poter cancellare anche un
	 * numero molto grande di entità. Poco efficinete poichè prima recupera le
	 * entità da cancellare e poi le cancella
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
	 * Il metodo cancella tutte le entità identificate dalle chiavi passate come
	 * argomento. Se nessuna entità corrisponde ad una chiave passata si passa
	 * oltre. Cancella le entità a blocchi, la grandezza di tali blocchi è
	 * definita da range Effettua quindi un'operazione sul datastore per blocco.
	 * E' il metodo più efficiente di cancellazione poichè utilizza le Low-Level
	 * API bisogna però conoscere le chiavi delle entità da cancellare
	 * 
	 * @param keysToRemove
	 *            Lista delle chiavi corrispondenti alle entità da rimuovere
	 * @param range
	 *            indica quante entità alla volta devono essere rimosse
	 */
	public void removeAllByKeys(List<Key> keysToRemove, Integer range) {

		List<Key> cursorToRemove = null;
		// Indice da cui si comincia a cancellare
		Integer fromIndex = 0;

		/*
		 * indice in cui si finisce di cancellare l'oggetto di indice toIndex
		 * NON viene cancellato
		 */
		Integer toIndex = range;

		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

		try {
			while (keysToRemove.size() > 0) {
				cursorToRemove = keysToRemove.subList(fromIndex, toIndex);
				ds.delete(cursorToRemove);
				fromIndex = toIndex;
				toIndex += range;
			}
		} catch (IndexOutOfBoundsException e) {
			if (fromIndex < keysToRemove.size()) {
				toIndex = keysToRemove.size();
				cursorToRemove = keysToRemove.subList(fromIndex, toIndex);
				ds.delete(cursorToRemove);
			}
		}

	}

	/**
	 * Il metodo cancella tutte le entità della classe classToRemove, utilizza
	 * query.deletePersistentAll() in modo da evitare che le entità vengano
	 * prima recuperate dal datastore e poi cancellate. Utilizza una singola
	 * operazione sul datastore. Se bisogna cancellare un grandissimo numero di
	 * entità potrebbe causare eccezioni di deadline. (potrebbe metterci più
	 * tempo di quello a disposizione)
	 * 
	 * @param classToRemove
	 */
	public Long removeAllViaQuery(String classToRemove) {
		String kindToDelete = classToRemove;
		Long entitiesRemoved = 0l;

		PersistenceManager pm = PMF.get().getPersistenceManager();

		try {

			Query query = pm.newQuery("select from " + kindToDelete);
			entitiesRemoved = query.deletePersistentAll();

		} finally {

			pm.close();
		}

		return entitiesRemoved;

	}

	/**
	 * Il metodo cancella tutte le entità della classe type che rispondono alla
	 * query i cui parametri sono contenuti in params. Utilizza
	 * query.deletePersistentAll() in modo da evitare che le entità vengano
	 * prima recuperate dal datastore e poi cancellate. Utilizza una singola
	 * operazione sul datastore. Se bisogna cancellare un grandissimo numero di
	 * entità potrebbe causare eccezioni di deadline. (potrebbe metterci più
	 * tempo di quello a disposizione)
	 * 
	 * @param params
	 *            Mappa contenente la coppia chiave valore per costruire il
	 *            filtro la coppia chiave valore della mappa è così formata:
	 *            chiave è una stringa rappresentnate il nome del campo
	 *            dell'oggetto il valore corrispondente è il valore che gli
	 *            oggetti cercati devono matchare il valore può essere di tipo
	 *            stringa o numerico oppure può essere di tipo
	 *            {@link OpAndValue} se si desidera effettuare una query in cui
	 *            non si usa per il campo corrispondente un operatore di
	 *            eguaglianza
	 * @param type
	 *            tipo dell'entità da cancellare
	 */
	public Long removeViaQuery(Map<String, Object> params, Class<?> type) {

		PersistenceManager pm = PMF.get().getPersistenceManager();
		Query query = pm.newQuery(type);
		Long entitiesRemoved = 0l;

		// creiamo il filtro e settiamolo se sono stati passati i parametri
		if (params != null) {
			String filter = createFilter(params);
			query.setFilter(filter);
		}// altrimenti la query viene effettuata senza filtro su tutte le entità

		try {

			entitiesRemoved = query.deletePersistentAll();

		} finally {
			query.closeAll();
			pm.close();

		}

		return entitiesRemoved;

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
	 * ------------------------------------------------------------------
	 * API PER MAP-REDUCE NECESSITANO DELLA LIBRERIA MAPREDUCE DI GOOGLE APPENGINE
	 * -------------------------------------------------------------------
	 * 
	 */


	/**
	 * Il metodo si occupa di recuperare tutte le entità MapRecudeState (100 alla volta)
	 * e le cancella tutte e 100 in un'unica chiamata al datastore 
	 */
	public void deleteAllMapReduceState(){
		
		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
		List<MapReduceState> mapsToDelete= new LinkedList<MapReduceState>();
		List<Key> mapsKeysToDelete= new LinkedList<Key>();


		//Recupero i primi 100 MapReduceState
		MapReduceState.getMapReduceStates(ds, null, 100, mapsToDelete);
		
		while (mapsToDelete.size()>0){
		
		//Creo la lista delle chiavi degli oggetti MapReduceState recuperati
		for(MapReduceState map: mapsToDelete){

			Key key = KeyFactory.createKey("MapReduceState", map.getJobID());
			mapsKeysToDelete.add(key);
		}

		//Li cancello con un'unica chiamata al datastore
		ds.delete(mapsKeysToDelete);
		
		//Pulisco le liste
		mapsToDelete.clear();
		mapsKeysToDelete.clear();
		
		//Recupero i successivi 100
		MapReduceState.getMapReduceStates(ds, null, 100, mapsToDelete);
		
		}
	}

}

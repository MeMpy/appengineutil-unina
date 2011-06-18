package it.unina.tools.datastore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.datanucleus.store.appengine.query.JDOCursorHelper;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Key;

public class DatastoreLoadAndSaveWithTransaction {
	
	private PersistenceManager pm=null;
	private Transaction tx=null;
	
	
	/**
	 * Costruttore, inizializza il PersistenceManager
	 */
	public DatastoreLoadAndSaveWithTransaction() {
		pm= PMF.get().getPersistenceManager();
		tx= pm.currentTransaction();
	}
	
	/**
	 * Apre una transazione, è necessario invocare questo metodo PRIMA di eseguire
	 * una qualsiasi operazione sul datastore utilizzando questa classe
	 */
	public void openTransaction(){
		tx.begin();
	}
	
	/**
	 * Ritorna true se la transazione è già stata aperta
	 * @return 
	 */
	public Boolean isOpen(){
		return tx.isActive();
	}
	
	/**
	 * Effettua il commit della transazione e la chiusura del PersistanceManager
	 */
	public void commitTransaction(){
		tx.commit();
		if (!pm.isClosed()){
			pm.close();
		}
	}
	
	
	/**
	 * Effettua il rollback della transazione e la chiusura del PersistanceManager
	 */
	public void rollbackTransaction(){
		if (tx.isActive()){
			tx.rollback();
		}
		if (!pm.isClosed()){
			pm.close();
		}
	}
	
	
	
	/**
	 * Metodo generico per salvare un oggetto
	 * 
	 * @param obj
	 */
	public void save(Object obj){
		
		pm.makePersistent(obj);
		
	}
	
	
	/**
	 * Metodo generico per salvare una lista di oggetti
	 * 
	 * @param obj
	 */
	public void saveAll(List<?> obj ){
		
		
		
			
		pm.makePersistentAll(obj);
		
	}
	
	
	
	
	/**
	 * Metodo per caricare uno o più oggetti data una mappa di
	 * parametri rappresentanti coppie (attributo, valore) che serviranno
	 * per filtrare la query.
	 * Il filtro creato sarà un and di equivalenze.
	 * Se la mappa è null il filtro nn verrà creato e si farà una query che 
	 * recupererà tutti gli oggetti della classe.
	 * Il metodo non effettua il lazy loading in quanto verrà invocato il metodo
	 * size() della lista dei risultati. Questo è necessario per poter effettuare
	 * salvataggi degli oggetti recuperati in momenti immediatamente successivi.
	 * @param Class<?> Tipo degli oggetti restituiti da load
	 * @param params Mappa contenente la coppia chiave valore per costruire il filtro
	 * la coppia chiave valore della mappa è così formata:
	 * chiave è una stringa rappresentnate il nome del campo dell'oggetto
	 * il valore corrispondente è il valore che gli oggetti cercati devono matchare
	 * il valore può essere di tipo stringa o numerico.
	 * @return la lista di oggetti che condividono lo stesso identificativo
	 */
	public Object load(Map<String, Object> params, Class<?> type){
		
		Query query = pm.newQuery(type);
	
		String filter="";
		
		if(params!=null){
			
			//Creazione filtro per la query
			Set<String> fieldsName=params.keySet();
			Object value;
			
			for(String field: fieldsName){
				value=params.get(field);
				if(value instanceof String){
					filter+=field+" == '"+ value + "' && ";
				}
				else{
					filter+=field+" == "+ value + " && " ;
				}
			}
			
			//elimino l'and finale
			filter= filter.substring(0, filter.length()-4);
			query.setFilter(filter);
		}
		
	    Object results=null;
	    //necessario per poter annullare il lazyload
	    List<Object> r=null;
	    try {
	        results = query.execute();
	        r=(List<Object>) results;
		    //necessario per poter annullare il lazyload
	        r.size();
	        
	    } finally {
	        query.closeAll();
	        
	    }
		
		
		return r;
	
		
	}
	
	
	/**
	 * Metodo utilizzato per ritornare un oggetto generico dato il suo id
	 * Tipicamente l'id è l'identificativo incluso all'interno della chiave
	 * creata da GAE. Tipicamente è un long ma può essere una stringa o un oggetto
	 * a discrezione del programmatore.
	 * @param <T> Tipo dell'oggetto ritornato
	 * @param type Classe dell'oggetto da cercare
	 * @param id Identificativo dell'oggetto.
	 * @return l'oggetto dal datastore
	 */
	public <T> T loadObjectById(Class<T> type, Object id){
		
		T dataToLoad=null;
		
		dataToLoad=pm.getObjectById(type, id);
		
		
		return dataToLoad;
		
	}
	
	
	
	/**
	 * Metodo per rimuovere l'oggetto passato in input
	 * TODO: Per ora il metodo recupera l'oggetto dal db e lo cancella, problema di efficienza in quanto si fa una query in più al db.
	 * @param objToRemove
	 */
	public void removeById(Class<?> classToRemove, Object id){
		
		pm.deletePersistent(pm.getObjectById(classToRemove, id));
			
			
	}
	
	
	/**
	 * Il metodo cancella tutte le entità della classe classToRemove,
	 * utilizza un cursore per scorrerle tutte, in modo da poter cancellare
	 * anche un numero molto grande di entità.
	 * @param classToRemove
	 */
	public void removeAll(String classToRemove){
		String kindToDelete= classToRemove;
		
		
		Map<String, Object> extensionMap= new HashMap<String, Object>();
		Cursor cursor;
		
					
		Query query = pm.newQuery("select from "+kindToDelete);
		query.setRange(0, 500);

		List<Object> results = (List<Object>) query.execute();
		while(results.size()!=0){
			// Use the first 500 results...
			pm.deletePersistentAll(results);

			cursor = JDOCursorHelper.getCursor(results);
			
			extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, cursor);

			query.setExtensions(extensionMap);
			
			query.setRange(0, 500);

			results = (List<Object>) query.execute();
			
		}
		

	}

}

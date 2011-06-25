package it.unina.tools.datastore;


import java.io.Serializable;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Key;


@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class ByteArrayDataClass implements Serializable{

	
	@PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key key;

    @Persistent
    private String title;
    
    @Persistent
    private Integer numRows;


    @Persistent
    private Blob file;

    
    public ByteArrayDataClass(byte[] dataToStore, String name){
    	this.title=name;
    	this.file= new Blob(dataToStore);
    }

    public Long getId() {
        return key.getId();
    }

    public String getTitle() {
        return title;
    }

    public char[] getFileToChar() {
        if (file == null) {
            return null;
        }
        byte[] bytes= file.getBytes();
        char[] chars= ByteUtil.bytesToChars(bytes);
        

        return chars;
    }

    public void setTitle(String title) {
        
        this.title = title;
    }


    public void setFileFromChar(char[] chars) {
    	byte[] bytes = ByteUtil.charsToBytes(chars);
        this.file = new Blob(bytes);
    }


    public Key getKey() {
            return key;
    }

    public void setKey(Key key) {
            this.key = key;
    }

	public byte[] getFile() {
		return file.getBytes();
	}

	public void setFile(byte[] file) {
		this.file = new Blob(file);
	}

	public void setNumRows(Integer numRows) {
		this.numRows = numRows;
	}

	public Integer getNumRows() {
		return numRows;
	}

//	public Integer getNumRows() {
//		return numRows;
//	}
//
//	public void setNumRows(Integer numRows) {
//		this.numRows = numRows;
//	}
	
//	public void setNumRows(){
//		BufferedReader fReader = new BufferedReader(new CharArrayReader(this.getFile()));
//		Integer num=0;
//        try {
//        	
//        	while(fReader.readLine()!=null)num++;
//        	fReader.close();
//        } catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//        
//        numRows=num;
//	}
}
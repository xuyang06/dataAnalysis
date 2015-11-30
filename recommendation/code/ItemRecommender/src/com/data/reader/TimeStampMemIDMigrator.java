package com.data.reader;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.AbstractIDMigrator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TimeStampMemIDMigrator extends AbstractIDMigrator {
    
    private final FastByIDMap<String> longToString;
   
    public TimeStampMemIDMigrator() {
      this.longToString = new FastByIDMap<String>(100);
    }
   
    //@Override
    public void storeMapping(long longID, String stringID) {
      synchronized (longToString) {
        longToString.put(longID, stringID);
      }
    }
   
    @Override
    public String toStringID(long longID) {
      synchronized (longToString) {
        return longToString.get(longID);
      }
    }
    
    public void singleInit(String stringID) throws TasteException, ParseException {
    	SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
    	Date d = f.parse(stringID);
        storeMapping(d.getTime(), stringID);
    }

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
	    	Date d = f.parse("2007-05-17");
	    	System.out.println(d.getTime());
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}

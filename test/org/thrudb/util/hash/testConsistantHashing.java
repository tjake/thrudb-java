package org.thrudb.util.hash;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

public class testConsistantHashing extends TestCase {

	public void testHash(){
		ConsistentHash ch = new ConsistentHash();
		
		for(int i=0; i<100; i++){
			ch.add(""+i, 1.0);
		}
		
		Map<String,Integer> counts = new HashMap<String,Integer>();
		
		for(int i=0; i<20000; i++){
			String s = ch.get(""+i);
			
			if(counts.containsKey(s)){
				counts.put(s, new Integer(counts.get(s).intValue()+1));
			}else{
				counts.put(s, new Integer(0));
			}		
		}
		
		for(Map.Entry<String, Integer> ent : counts.entrySet()){
			System.out.println(ent.getKey() + " = " + ent.getValue());
		}
	}
	
}

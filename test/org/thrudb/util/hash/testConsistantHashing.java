package org.thrudb.util.hash;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

public class testConsistantHashing extends TestCase {

	public void testHash(){
		ConsistentHash ch = new ConsistentHash();
		
		for(int i=0; i<3; i++){
			ch.add(""+i, 1.0);
		}
		
		Map<String,Integer> counts = new HashMap<String,Integer>();
		Map<String,String>  routes = new HashMap<String,String>();
		
		for(int i=0; i<200; i++){
			String s = ch.get(""+i);
			
			routes.put(""+i, s);
			
			if(counts.containsKey(s)){
				counts.put(s, new Integer(counts.get(s).intValue()+1));
			}else{
				counts.put(s, new Integer(0));
			}		
		}
		
		for(Map.Entry<String, Integer> ent : counts.entrySet()){
			System.out.println(ent.getKey() + " = " + ent.getValue());
		}
		
		//now remove one of the shards and see how "consistent" it is
		ch.remove("0");
		
		int hit = 0, miss = 0;
		for(int i=0; i<200; i++){
			String s = ch.get(""+i);
			if(routes.get(""+i).equals(s)){
				hit++;
			}else{
				miss++;
			}
		}
		
		System.out.println("hits = "+hit+", misses = "+miss);
		assertEquals(80,miss);
		
		ch.add("0", 1.0);
		hit = 0; miss = 0;
		for(int i=0; i<200; i++){
			String s = ch.get(""+i);
			if(routes.get(""+i).equals(s)){
				hit++;
			}else{
				miss++;
			}
		}
		assertEquals(0,miss);
		
		
		
		ch.add("4", 1.0);
		hit = 0; miss = 0;
		for(int i=0; i<200; i++){
			String s = ch.get(""+i);
			if(routes.get(""+i).equals(s)){
				hit++;
			}else{
				miss++;
			}
		}
		
		System.out.println("hits = "+hit+", misses = "+miss);
		assertEquals(47,miss);
	}
	
}

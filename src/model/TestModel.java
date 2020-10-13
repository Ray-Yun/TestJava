package model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestModel {
	public static void main(String[] argv) {
        System.out.println("==============List寫法");
        List<Object> list = new ArrayList<Object>();
        list.add("dd");
        list.add("aa");
        list.add("cc");
        for (Iterator<Object> iterator = list.iterator(); iterator.hasNext();) {
            Object string = iterator.next();
            System.out.println(string);
        }
        System.out.println("==============Map寫法");
        Map<String, String> map0 = new HashMap<String, String>();
        map0.put("name", "zhangsan");
        map0.put("***", "female");
        String nameString = map0.get("name");
        String String = map0.get("***");
        System.out.println(nameString + "\n" + String);
        
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("name", "ddd");
        map.put("age", 23);
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("name", "ccc");
        map1.put("age", 43);
        
        Iterator<Map<String, Object>> it = lists.iterator();
        for (; it.hasNext();) {
            Map<String, Object> map2 = it.next();
            String name = (String) map2.get("name");
            Object age = map2.get("age");
            System.out.println("name=" + name + "\nage=" + age);
        }
    }
	}
}
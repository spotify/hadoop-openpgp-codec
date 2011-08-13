package com.spotify.hadoop.openpgp;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

public class EnumUtils {
	public static <T> Map<String, Integer> getStaticFinalFieldMapping(Class<T> cls) {
		Map<String, Integer> ret = new HashMap<String, Integer>();

		for (Field f : cls.getDeclaredFields()) {
			int mod = f.getModifiers();

			if (Modifier.isPublic(mod) &&
					Modifier.isStatic(mod) &&
					Modifier.isFinal(mod) && (
						int.class.isAssignableFrom(f.getType()) ||
						Integer.class.isAssignableFrom(f.getType()))) {
				try {
					ret.put(f.getName(), (Integer) f.get(null));
				} catch (IllegalAccessException ex) {
				}
			}
		}

		return ret;
	}
}

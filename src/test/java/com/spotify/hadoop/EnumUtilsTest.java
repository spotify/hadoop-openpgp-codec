package com.spotify.hadoop.openpgp;

import java.util.Map;

import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;


public class EnumUtilsTest {
	@Test
	public void getStaticFinalFieldMapping() {
		Map<String, Integer> m = EnumUtils.getStaticFinalFieldMapping(TestFields.class);

		assertEquals(2, m.size());
		assertEquals(1, (int) m.get("A"));
		assertEquals(Integer.valueOf(2), m.get("B"));
	}

	public static class TestFields {
		public static final int A = 1;
		public static final Integer B = 2;

		// Not static
		public final int C = 3;

		// Not public
		private static final int D = 4;

		/// Not an integer
		public static final Class<TestFields> E = TestFields.class;
	}
}

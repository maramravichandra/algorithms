
public class Singleton {

	private static int[] intvalues = new int[100];
	private static Singleton instance = null;
	
	private Singleton() {
		
	}

	public static Singleton getInstance() {
		if( instance == null ) {
			instance = new Singleton();
		}
		
		return instance;
	}
	
	private static int index = 0;
	public static Boolean addValue( int value ) {
		intvalues[index] = value;
		index++;
		return true;
	}
}


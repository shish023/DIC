import java.util.Comparator;
import java.util.Map;

class ValueComparator implements Comparator<Object> {

	Map<String, Double> base;

	public ValueComparator(Map<String, Double> base) {
		this.base = base;
	}

	public int compare(Object a, Object b) {

		if (((Double) base.get(a)).doubleValue() < ((Double) base.get(b)).doubleValue()) {
			return 1;
		} else if ( ((Double) base.get(a)).doubleValue() == ((Double) base.get(b)).doubleValue()) {
			return ((String)a).compareTo(((String)b));
		} else {
			return -1;
		}
	}
}
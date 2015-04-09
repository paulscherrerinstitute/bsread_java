package ch.psi.bsread.message;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * Complete data message send from a BSREAD source
 */
public class Message {
	private MainHeader mainHeader = null;
	private DataHeader dataHeader = null;
	private Multimap<String, Value> values = ArrayListMultimap.create();

	public void setDataHeader(DataHeader dataHeader) {
		this.dataHeader = dataHeader;
	}

	public DataHeader getDataHeader() {
		return dataHeader;
	}

	public void setMainHeader(MainHeader mainHeader) {
		this.mainHeader = mainHeader;
	}

	public MainHeader getMainHeader() {
		return mainHeader;
	}

	public void setValues(Multimap<String, Value> values) {
		this.values = values;
	}

	public Multimap<String, Value> getValues() {
		return values;
	}
}

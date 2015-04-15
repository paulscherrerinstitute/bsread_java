package ch.psi.bsread.message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Complete data message send from a BSREAD source
 */
public class Message implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private MainHeader mainHeader = null;
	private DataHeader dataHeader = null;
	
	/**
	 * Map holding all values of a channel - key: channel name value: value
	 */
	private Map<String, Value> values = new HashMap<>();

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

	public void setValues(Map<String, Value> values) {
		this.values = values;
	}

	public Map<String, Value> getValues() {
		return values;
	}
}

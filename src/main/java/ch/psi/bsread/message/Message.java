package ch.psi.bsread.message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Complete data message send from a BSREAD source
 */
public class Message<V> implements Serializable {
	private static final long serialVersionUID = -5438664019568662396L;

	private MainHeader mainHeader = null;
	private boolean dataHeaderChanged = false;
	private DataHeader dataHeader = null;

	/**
	 * Map holding all values of a channel - key: channel name value: value
	 */
	private Map<String, Value<V>> values = new HashMap<>();

	public void setMainHeader(MainHeader mainHeader) {
		this.mainHeader = mainHeader;
	}

	public MainHeader getMainHeader() {
		return mainHeader;
	}
	
	public boolean isDataHeaderChanged() {
		return dataHeaderChanged;
	}

	public void setDataHeaderChanged(boolean dataHeaderChanged) {
		this.dataHeaderChanged = dataHeaderChanged;
	}
	
	public void setDataHeader(DataHeader dataHeader) {
		this.dataHeader = dataHeader;
	}

	public DataHeader getDataHeader() {
		return dataHeader;
	}

	public void setValues(Map<String, Value<V>> values) {
		this.values = values;
	}

	public Map<String, Value<V>> getValues() {
		return values;
	}
}

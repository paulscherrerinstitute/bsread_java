package ch.psi.bsread.basic;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;

/**
 * Complete data message send from a BSREAD source
 */
public class BasicMessage implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private MainHeader mainHeader = null;
	private DataHeader dataHeader = null;
	
	/**
	 * Map holding all values of a channel - key: channel name value: value
	 */
	private Map<String, BasicValue<?>> values = new HashMap<>();

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

	public void setValues(Map<String, BasicValue<?>> values) {
		this.values = values;
	}

	public Map<String, BasicValue<?>> getValues() {
		return values;
	}
}

package ch.psi.bsread.message;

import java.util.ArrayList;
import java.util.List;

/**
 * Complete data message send from a BSREAD source
 */
public class Message {
	private MainHeader mainHeader = null;
	private DataHeader dataHeader = null;
	private List<Value> values = new ArrayList<>();

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

	public void setValues(List<Value> values) {
		this.values = values;
	}

	public List<Value> getValues() {
		return values;
	}
}

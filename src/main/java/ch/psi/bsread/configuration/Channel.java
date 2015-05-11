package ch.psi.bsread.configuration;

public class Channel {
	
	private String name;
	private int offset = 0;
	private double frequency = 100;
	
	public Channel(){
	}
	
	public Channel(String name){
		this.name = name;
	}
	
	public Channel(String name, int offset, double frequency){
		this.name = name;
		this.offset = offset;
		this.frequency = frequency;
	}
	
	public int getOffset() {
		return offset;
	}
	
	public void setOffset(int offset) {
		this.offset = offset;
	}
	
	public double getFrequency() {
		return frequency;
	}

	public void setFrequency(double frequency) {
		this.frequency = frequency;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	
}

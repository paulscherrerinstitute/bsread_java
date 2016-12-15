package ch.psi.bsread.monitors;

import java.util.UUID;

import org.zeromq.ZMQ.Context;

import com.fasterxml.jackson.databind.ObjectMapper;

import zmq.SocketBase;

public class MonitorConfig {
	private Context context;
	private SocketBase socket;
	private String monitorItentifier;
	private ObjectMapper objectMapper;
	private int socketType;

	public MonitorConfig(Context context, SocketBase socket, ObjectMapper objectMapper, int socketType) {
		this(context, socket, objectMapper, socketType, UUID.randomUUID().toString());
	}

	public MonitorConfig(Context context, SocketBase socket, ObjectMapper objectMapper, int socketType, String monitorItentifier) {
		this.context = context;
		this.socket = socket;
		this.monitorItentifier = monitorItentifier;
		this.objectMapper = objectMapper;
		this.socketType = socketType;
	}

	public Context getContext() {
		return context;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public SocketBase getSocket() {
		return socket;
	}

	public void setSocket(SocketBase socket) {
		this.socket = socket;
	}

	public String getMonitorItentifier() {
		return monitorItentifier;
	}

	public void setMonitorItentifier(String monitorItentifier) {
		this.monitorItentifier = monitorItentifier;
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public int getSocketType() {
		return socketType;
	}

	public void setSocketType(int socketType) {
		this.socketType = socketType;
	}
}

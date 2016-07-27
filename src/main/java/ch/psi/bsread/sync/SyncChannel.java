package ch.psi.bsread.sync;

// Interface to allow other Classes than ch.psi.bsread.configuration.Channel to be synchronized
public interface SyncChannel {

   String getName();

   int getModulo();

   int getOffset();
}

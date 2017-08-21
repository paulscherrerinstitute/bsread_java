# Overview
This is the Java implementation of the beam synchronous data acquisition protocol (bsread) for SwissFEL. This project provides a `Receiver` as well as an `Sender` implementation to send and receive BSREAD compliant messages.

The specification of bsread can be found [here](https://docs.google.com/document/d/1BynCjz5Ax-onDW0y8PVQnYmSssb6fAyHkdDl1zh21yY/edit?usp=sharing).

This library does not provide the code needed to access the SwissFEL dispatching layer!


# Usage

To use the library from a project managed with Gradle use this to get this library as dependency:

```
repositories {
    maven { url "https://artifacts.psi.ch/artifactory/libs-snapshots"}
}


dependencies {
    compile('ch.psi:bsread:3.4.4')
}
```

The most simple receiver looks something like this:

```java
   public static void main(String[] args) {

       ReceiverConfig<Object> config = new ReceiverConfig<>();
       config.setAddress("tcp://localhost:9999");
       config.setSocketType(ZMQ.SUB);

       IReceiver<Object> receiver = new BasicReceiver(config);

       // Terminate program with ctrl+c
       Runtime.getRuntime().addShutdownHook(new Thread(() -> receiver.close()));

       receiver.connect();

       try {
           Message<Object> message;
           // Due to https://github.com/zeromq/jeromq/issues/116 you must not use Thread.interrupt() to stop the receiving thread!

           long lastPulseId = 0L;
           while ((message = receiver.receive()) != null) {
               System.out.println(message.getMainHeader());
           }

       } finally {
           receiver.close();
       }
   }
```

## Matlab

The library can also be used within Matlab to receive data. The documentation on how to use the library can be found in [Matlab.md](Matlab.md)

# Development

This project can be build by executing

```bash
./gradlew build
```

To upload the built jar to the artifact repository use:

```bash
./gradlew uploadArchives
```

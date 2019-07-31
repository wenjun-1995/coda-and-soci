# CoflowSim README

This code is built on the CoflowSim code published at the website https://github.com/coflow/coflowsim. 

The simulator implements the CODA (Sigcomm 2016) and SOCI (My Mechanism). Compared to previous COflowSim, the changed places are file CoflowSimulator.java and CoflowBenchmarkTraceProducer.java. The comments in CoflowSimulator.java show how to enable CODA and SOCI.
I generate the trace using python specified for CODA and SOCI for the simulator to read (is not uploaded yet).

1. **FAIR**: Per-flow fair sharing
2. **PFP**: Per-flow prioritization, i.e., SRTF for minimizing time and EDF for meeting deadlines. Examples include PDQ and pFabric.

You are using Eclipse to import the project from pom.xml

To run the code about CODA or SOCI, you should configure the command parameter by clicking "Run->Run Configurations...->Arguments" and adding 
DARK COFLOW-BENCHMARK the-trace-path" in "Program arguments". Then you can click "Run". 
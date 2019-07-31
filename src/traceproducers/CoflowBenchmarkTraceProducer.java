package coflowsim.traceproducers;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import coflowsim.datastructures.Job;
import coflowsim.datastructures.Machine;
import coflowsim.datastructures.MapTask;
import coflowsim.datastructures.ReduceTask;
import coflowsim.datastructures.Task;
import coflowsim.utils.Constants;

/**
 * Reads a trace from the <a href="https://github.com/coflow/coflow-benchmark">coflow-benchmark</a>
 * project.
 * <p>
 * Expected trace format:
 * <ul>
 * <li>Line 1: &lt;Number of Racks&gt; &lt;Number of Jobs&gt;
 * <li>Line i: &lt;Job ID&gt; &lt;Job Arrival Time &gt; &lt;Number of Mappers&gt; &lt;Location of
 * each Mapper&gt; &lt;Number of Reducers&gt; &lt;Location:ShuffleMB of each Reducer&gt;
 * </ul>
 * 
 * <p>
 * Characteristics of the generated trace:
 * <ul>
 * <li>Each rack has at most one mapper and at most one reducer. Historically, this was because
 * production clusters at Facebook and Microsoft are oversubscribed in core-rack links; essentially,
 * simulating rack-level was enough for them. For full-bisection bandwidth networks, setting to the
 * number of machines should result in desired outcome.
 * <li>All tasks of a phase are known when that phase starts, meaning all mappers start together and
 * all reducers do the same.
 * <li>Mapper arrival times are ignored because they are assumed to be over before reducers start;
 * i.e., shuffle start time is job arrival time.
 * <li>Each reducer's shuffle is equally divided across mappers; i.e., reduce-side skew is intact,
 * while map-side skew is lost. This is because shuffle size is logged only at the reducer end.
 * <li>All times are in milliseconds.
 * </ul>
 */
public class CoflowBenchmarkTraceProducer extends TraceProducer {

  private int NUM_RACKS;
  private final int MACHINES_PER_RACK = 1;

  public int numJobs;

  private String pathToCoflowBenchmarkTraceFile;

  public int priChangeNum;
  /**
   * @param pathToCoflowBenchmarkTraceFile
   *          Path to the file containing the trace.
   */
  public CoflowBenchmarkTraceProducer(String pathToCoflowBenchmarkTraceFile) {
    this.pathToCoflowBenchmarkTraceFile = pathToCoflowBenchmarkTraceFile;
  }

  /**
   * Read trace from file.
   */
  //
  //public int getPriChangeNum() {
	  
	//  return priChangeNum;
  //}
  
  @Override
  public void prepareTrace() {
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(pathToCoflowBenchmarkTraceFile);
    } catch (FileNotFoundException e) {
      System.err.println("Couldn't open " + pathToCoflowBenchmarkTraceFile);
      System.exit(1);
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(fis));

    // Read number of racks and number of jobs in the trace
    try {
      String line = br.readLine();
      String[] splits = line.split("\\s+");

      NUM_RACKS = Integer.parseInt(splits[0]);
      numJobs = Integer.parseInt(splits[1]);
      
    } catch (IOException e) {
      System.err.println("Missing trace description in " + pathToCoflowBenchmarkTraceFile);
      System.exit(1);
    }

    // Read numJobs jobs from the trace file
    
    for (int j = 0; j < numJobs; j++) {
      try {
    	
        String line = br.readLine();
        String[] splits = line.split("\\s+");
        int lIndex = 0;
        
        int identjobID = 0;
        if(splits[splits.length-1].equals("identerrorlb")) {
        	identjobID = Integer.parseInt(splits[splits.length-2]);
        	//job.setIdentJobID(identjobID);

        }
        //System.out.println(splits[splits.length-1]+" ");
        
        //System.out.println(splits[splits.length-1].equals("identerrorlb"));
        //System.in.read();
        int origID = Integer.parseInt(splits[0]);
        String jobName = "JOB-" + splits[lIndex++];
        
        //ruan
        int jobArrivalTime = 0, mindex=0, rindex=0;
        //double tempjobArrivalTime=0;
        System.out.println(line); 
        double arrivalTime = Double.parseDouble(splits[lIndex++]);
        jobArrivalTime=(int)arrivalTime;
        
        //System.out.println(line);  
        
        int numMappers = Integer.parseInt(splits[lIndex++]);
        
        Job job=null;
        if(identjobID>0) {
        	String tempJobName = "JOB-" + splits[splits.length-2];
        	job = jobs.getOrAddJob(tempJobName);
        	//System.out.println(jobName);
        	if(job.jobExist) {
        		job.jobExist=false; //job exists
        		
        		job = jobs.getOrAddJob(jobName);
        		job.jobExist = true;
        //n    	job.jobID=origID;
            	job.identjobID = Integer.parseInt(splits[splits.length-2]);
        //n    	
            	job.jobID=identjobID;
//my1            	
//            	if( job.numMappers <= 10  && job.numMappers + numMappers >10) {
//            	//if( job.numMappers <= 10  && job.numMappers + numMappers >15) {
//            		job.jobExist = false;
//            		if(splits[splits.length-3].equals("identerror")) {
//            			job.extJobMark=true;
//            			job.identjobID = Integer.parseInt(splits[splits.length-4]);
//            		}
//            		else
//            			job.identjobID = job.jobID;
//                	priChangeNum++;
//                	System.out.println("PriChangeNum: "+priChangeNum);
//                	//.in.read();
//        		}

            	
        	}
        	else {
        		System.out.println("A same job is splitted "+job.jobName);
        		job.jobExist = true;
          //n  	job.jobID=origID;
            	job.identjobID = Integer.parseInt(splits[splits.length-2]);
            	//n    	
            	job.jobID=identjobID;
        	}
        		
        		
        } 
        else {
        	job = jobs.getOrAddJob(jobName);
        	job.jobID=origID;
        	job.identjobID = job.jobID;
        }
        

        if(job.jobExist) {
        	//ruan
        	if(splits[splits.length-3].equals("identerror")) {
            	job.extJobMark=true;
            	job.preIdentjobID = Integer.parseInt(splits[splits.length-4]);
            }
        	else
        		job.preIdentjobID = origID;
        		
        	job.setIdentJobID(identjobID);
        }
        job.origJobID = origID;
        
        //System.out.println(job.jobName+" "+job.jobID+" "+job.origJobID);
        // #region: Create mappers
        //System.out.println(line);
        //System.out.println(splits[lIndex]);
        for (int mID = mindex; mID < numMappers; mID++) {
          String taskName = "MAPPER-" + mID;
 
          int taskID = mID;

          // 1 <= rackIndex <= NUM_RACKS
          int rackIndex = Integer.parseInt(splits[lIndex++]) + 1;

          // Create map task
          Task task = new MapTask(taskName, taskID, job, jobArrivalTime, Constants.VALUE_IGNORED,
                  new Machine(rackIndex));
         

          // Add task to corresponding job
          job.addTask(task);
        }
        // #endregion

        // #region: Create reducers
        int numReducers = Integer.parseInt(splits[lIndex++]);
        for (int rID = rindex; rID < numReducers; rID++) {
          String taskName = "REDUCER-" + rID;
          int taskID = rID;

          // 1 <= rackIndex <= NUM_RACKS
          String rack_MB = splits[lIndex++];

          int rackIndex = Integer.parseInt(rack_MB.split(":")[0]) + 1;
          double shuffleBytes = Double.parseDouble(rack_MB.split(":")[1]) * 1048576.0;

          // Create reduce task
          Task  task = new ReduceTask(taskName, taskID, job, jobArrivalTime, Constants.VALUE_IGNORED,
                  new Machine(rackIndex), shuffleBytes, Constants.VALUE_IGNORED);
          
          //if(!job.jobExist)
        	//  task = new ReduceTask(taskName, taskID, job, jobArrivalTime, Constants.VALUE_IGNORED,
           //   new Machine(rackIndex), shuffleBytes, Constants.VALUE_IGNORED);
          //else
        //	  task = new ReduceTask(taskName, taskID, job, tempjobArrivalTime, Constants.VALUE_IGNORED,
          //            new Machine(rackIndex), shuffleBytes, Constants.VALUE_IGNORED);

          // Add task to corresponding job
          job.addTask(task);
        }
        // #endregion
        
        //ruan
        //int originalid = Integer.parseInt(splits[splits.length-1]);
        //job.StartTimeOfJobs.put(originalid, tempjobArrivalTime);

      } catch (IOException e) {
        System.err.println("Missing job in " + pathToCoflowBenchmarkTraceFile + ". " + j + "/"
            + numJobs + " read.");
      }
    }
    System.out.println("Do not change priority num: "+priChangeNum);
  }

  /** {@inheritDoc} */
  @Override
  public int getNumRacks() {
    return NUM_RACKS;
  }

  /** {@inheritDoc} */
  @Override
  public int getMachinesPerRack() {
    return MACHINES_PER_RACK;
  }
}

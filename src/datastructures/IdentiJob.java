package coflowsim.datastructures;

import java.util.Vector;

public class IdentiJob extends Job {
	public final Vector<Job> listOfJobs;
	private int identiJobID;
	public String identiJobName;
	private Vector<Job> listOfIdentiJob;
	private int jobnum;
	private int allMapNum;

	public IdentiJob(Job j, String jobName, int jobID) 
	{
		super(jobName, jobID);
		identiJobID = jobID;
		//System.out.println("Identijob: "+this.identjobID+" "+jobID);
		identiJobName = jobName;
		listOfJobs = new Vector<Job>();
		listOfIdentiJob = new Vector<Job>();
		listOfJobs.add(j);
		//this.shuffleBytesCompleted =j.shuffleBytesCompleted;
		allMapNum=j.numMappers;
	}
		
	public void addJob(Job j) {
		listOfJobs.add(j);
		//this.shuffleBytesCompleted +=j.shuffleBytesCompleted;
		allMapNum +=j.numMappers;
	}
	
	public int getJobnum() {
		jobnum = listOfJobs.size();
		return jobnum;
	}
	
	public int getAllMapnum() {
		return allMapNum;
	}
	
	public int getShuffleBytesCompleted() {
		int shuffleBytesCompleted=0;
		for(Job j: listOfJobs)
			shuffleBytesCompleted +=j.shuffleBytesCompleted;
		return shuffleBytesCompleted;
	}
	
	public int compareTo(IdentiJob arg0) {
		if(identiJobID == arg0.identiJobID)
			return 0;
		else if(identiJobID > arg0.identiJobID)
		    return 1;
		else
			return -1;
	}

}

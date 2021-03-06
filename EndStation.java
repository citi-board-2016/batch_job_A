package citiboard.batch;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

import java.util.HashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.reflect.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 





public class StartStation{
	//check if string is a number
	public static boolean isNumeric(String str)  
	{  
	  try  
	  {  
	    double d = Double.parseDouble(str);  
	  }  
	  catch(NumberFormatException nfe)  
	  {  
	    return false;  
	  }  
	  return true;  
	}
	//format output KV pairs as strings for the output text file
	public static class FormatAsTextFn extends DoFn<KV<String, Long>, String> {
	    @Override
	    public void processElement(ProcessContext c) {
	      c.output(c.element().getKey() + ": " + c.element().getValue());
	    }
	  }
	//holds input 
	static Input in;
	
	
		
	
	//read helper
	public static interface BatchOptions extends PipelineOptions {
	    @Description("Path of the file to read from")
	    //this needs to be filled in
	    @Default.String("gs://_______________________.csv")
	    String getInputFile();
	    void setInputFile(String value);

	    @Description("Path of the file to write to")
	    @Default.InstanceFactory(OutputFactory.class)
	    String getOutput();
	    void setOutput(String value);
	    
	    public static class OutputFactory implements DefaultValueFactory<String> {
	      @Override
	      public String create(PipelineOptions options) {
	        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
	        if (dataflowOptions.getStagingLocation() != null) {
	          return GcsPath.fromUri(dataflowOptions.getStagingLocation())
	              .resolve("____output_file______").toString();
	        } else {
	          throw new IllegalArgumentException("Must specify --output or --stagingLocation");
	        }
	      }
	    }

	}
	//route class
	@DefaultCoder(AvroCoder.class)
	static class Route {
		@Nullable String startStation;
		@Nullable String endStation;
		@Nullable Double startTime;
		@Nullable String startStationName;
		@Nullable String endStationName;
		
		public Route() {}

	    public Route(String startStationId, String endStationId, Double startTime, String startStationName, String endStationName) {
	      this.startStation = startStationId;
	      this.endStation = endStationId;
	      this.startTime = startTime;
	      this.startStationName = startStationName;
	      this.endStationName = endStationName;
	      
	    }

	}
	//Input class
	@DefaultCoder(AvroCoder.class)
	static class Input {
		@Nullable String endStation;
		@Nullable int timeStart;
		@Nullable int timeEnd;
		
		public Input(){}
	}

	//start station job
	public static void main(String[] args) {


		

		BatchOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
     	 .as(BatchOptions.class);
		// Create a pipeline parameterized by commandline flags.
		Pipeline p = Pipeline.create(options);

		///////////////////////////////////////////////////////////
		/////////////read and parse .csv input for demo////////////
		///////////////////////////////////////////////////////////
		
		
		//input txt will have a single line in the form:
		//"start station id","start time","end time"
		//separated only by commas
		
		//read user input file
		PCollection<String> lines = p.apply(TextIO.Read.named("ReadLines").from("gs://input-b-10001/input.csv"));
		//extract words into KV pairs
		PCollection<KV<String, String>> input = lines.apply(ParDo.named("ExtractWords").of(new DoFn<String, KV<String, String>>() {
			     @Override
			     public void processElement(ProcessContext c) {
			    	 int i = 0;
			    	 KV<String, String> station = KV.of("no", "0");
			       for (String word : c.element().split(",")) {
			    	   if (i == 0){
			    		   station = KV.of("endStation", word);
			    	   }
			    	   else if(i == 1){
			    		   station = KV.of("timeStart", word);
			    	   }
			    	   else if(i == 2){
			    		   station = KV.of("timeEnd", word);
			    	   }
			    	   else{
			    		   String key = "" + i;
			    		   station = KV.of(key, word);
			    	   }
			    	   i++;
			    	   
					c.output(station);
				 
			       }
			     }
			  }));
	
		
		//as Java Map to be passed in as side input to filter
		PCollectionView<Map<String, String>> user_input = input.apply(View.<String, String>asMap());
		
		
		//read data file
		PCollection<String> data = p.apply(TextIO.Read.from("gs://input-b-10001/citidata.csv"));
	
		//split data file into route objects    
		PCollection<Route> routes = data.apply(MapElements.via((String line) -> {
		    //create new route
		    Route route = new Route();
		    //split input into parts by comma
		    String[] parts = line.split(",");
			//get start and end station
		    route.startStation = parts[3];
		    route.endStation = parts[7];
		    
		    
		    //split date and time
		    String[] commas = parts[0].split("\"");
		    String datetimehash = "0";
			//ensure numeric
		    if(!isNumeric(commas[1])){

		    }
		    else{	
                	//split by double quotes
        	    String[] datetime = parts[1].split("\"");
			    //split by space to get date and time separate
        	    datetime = datetime[1].split(" ");  
			    //split date
        	    String[] date = datetime[0].split("/");
			    //get month, add prefix zero if single digit
        	    String month = date[0];
        	    if(!month.equals("10") || !month.equals("11") || !month.equals("12")){
        	    	month = "0" + month;
        	    }
        	    String[] time = datetime[1].split(":");
			    //create hash of date and time
        	    datetimehash = date[2] + month + date[1] + time[0] + time[1] + time[2];

            	}
		    
		    
		    
		    //add date/time hash and station names
		    route.startTime = Double.parseDouble(datetimehash);
		    route.startStationName = parts[4];
		    route.endStationName = parts[8];
		    
		    return route;
		}).withOutputType(new TypeDescriptor<Route>() {}));


		// filter based on start time and end location
			//returns PCollection of route objects
		PCollection<Route> filtered = routes.apply(ParDo.named("filterStations").withSideInputs(user_input).of(new DoFn<Route, Route>(){
			private final Logger LOG = LoggerFactory.getLogger(StartStation.class);

			public void processElement(ProcessContext c) {
				Route curr = c.element();
				
				Double thisStart = curr.startTime;
				//get side input
				Map<String, String> inputStation = c.sideInput(user_input);
				
				Double start = Double.parseDouble(inputStation.get("timeStart"));
				Double end = Double.parseDouble(inputStation.get("timeEnd"));
				String station = inputStation.get("endStation");

				if ((start < thisStart) && (thisStart < end)) {
				
					c.output(curr);

				}
			}
		}));

		// reduce to just a list of start stations
			//returns a PCollection of just start station ids
		PCollection<String> startStations = filtered.apply(ParDo.named("Map to end stations").of(new DoFn<Route, String>() {
			public void processElement(ProcessContext c) {
				Route curr = c.element();
				
				c.output(curr.endStation);
		}}));
		
		// count duplicates
			//returns a PCollection of key/value pairs
			// key: start station
			// value: count
		//returns PCollection<KV<String, Long>>
		PCollection<KV<String, Long>> counts = startStations.apply(Count.<String>perElement());

		
		// output this collection of key/value pairs
		//put in output file
		PCollection<String> out = counts.apply(ParDo.named("Write to Output").of(new FormatAsTextFn()));
		out.apply(TextIO.Write.to("gs://final-bike-output/output.txt"));   // Write output.

		// Run the pipeline.
		p.run();
	}


	
	
	
}

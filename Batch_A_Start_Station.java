import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import java.util.HashMap;
import java.util.Map;

 





public class StartStation{

	static class FilterTimeStation extends DoFn<PCollection<Object>, PCollection<Object>> {
		public void processElement(ProcessContext c) {
			Object curr = c.element();
			// In our DoFn, access the side input.
			int start = c.sideInput(timeRangeStartView).timeStart;
			int end = c.sideInput(timeRangeEndView).timeEnd;
			String station = c.sideInput(stationView).startStation;
			if (start <= curr.startTime && curr.startTime <= end) {
				if(curr.startStation == station){
					c.output(curr);
				}
			}
		}

	}

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
	    /**
	     * Returns "gs://${YOUR_STAGING_DIRECTORY}/filtered_stations_file" as the default destination.
	     */
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

	//start station job
	public static void main(String[] args) {


		

		WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
     	 .as(WordCountOptions.class);
		// Create a pipeline parameterized by commandline flags.
		Pipeline p = Pipeline.create(options);

		///////////////////////////////////////////////////////////
		/////////////read and parse .csv input for demo////////////
		///////////////////////////////////////////////////////////
		class Route {
		    public String startStation;
		    public String endStation;
		    public int startTime;
		    public String startStationName;
		    public String endStationName;

		}	
		class Input {
		    public String startStation;
		    public int timeStart;
		    public int timeEnd;
		}

		//input txt will have a single line in the form:
		//"start station id","start time","end time"
		//separated only by commas
		PCollection<String> lines = p.apply(TextIO.Read.from("gs://______filterdata.txt__"));
		PCollection<Input> input = lines.apply(MapElements.via((String line) -> {
		    Input input = new Input();
		    String[] parts = line.split(",");
		    input.startStation = parts[0];
		    input.timeStart = Integer.parseInt(parts[1]);
		    input.timeEnd = Integer.parseInt(parts[2]);
		    return input;
		}).withOutputType(new TypeDescriptor<Input>() {});)

		//side inputs
		final PCollectionView<Input> userInputView = input.asSingletonView();
		

		p.apply(TextIO.Read.from("gs://___INPUT_FILE___.csv"));
		p.apply(MapElements.via((String line) -> {
		    Route route = new Route();
		    String[] parts = line.split(",");
		    route.startStation = parts[3];
		    route.endStation = parts[7];
		    String[] datetime = parts[1].split(" ");
		    String[] date = datetime[0].split("/");
		    String[] time = datetime[1].split(":");
		    //creates a hash of the time (ex: 8/1/2016 00:06:11 will be: 201681000611)
		    String datetimehash = "" + date[2] + date[0] + date[1] + time[0] + time[1] + time[2];
		    route.startTime = Integer.parseInt(datetimehash);
		    route.startStationName = parts[4];
		    route.endStationName = parts[8];
		    
		    return route;
		}).withOutputType(new TypeDescriptor<Route>() {});)


		/////////////////////////////			/////
		// What are we inputting and outputting?? ///
		//////////////////////////////			/////
		//p.apply(TextIO.Read.from("gs://..."));

		// filter based on start time
			//returns PCollection of route object things
		// filter based on start station
			//returns PCollection of route object things
		p.apply(ParDo.withSideInputs(userInputView).of(new FilterTimeStation()));

		// reduce to just a list of end stations
			//returns a PCollection of just end station ids
		p.apply(ParDo.of(new DoFn<PCollection<String>, PCollection<Object>>() {
			public void processElement(ProcessContext c) {
				Object curr = c.element();
				
				c.output(curr.endStation);
		}}));
		// count duplicates
			//returns a PCollection of key/value pairs
			// key: end station
			// value: count
		//returns PCollection<KV<String, Long>>
		p.apply(Count.<String>perElement());

		// sort the key/value collection ?
		
		// output this collection of key/value pairs
		/////////////////////////////			/////
		// What are we inputting and outputting?? ///
		//////////////////////////////			/////


		//put in output file
		p.apply(TextIO.Write.to("gs://____output_file______"));   // Write output.

		// Run the pipeline.
		p.run();
	}


	
	
	
}

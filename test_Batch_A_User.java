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



public class UserRoutes{

	//start station job
	public static void main(String[] args) {

		//side inputs
		final PCollectionView<Integer> timeRangeStartView = start.asSingletonView();
		final PCollectionView<Integer> timeRangeEndView = end.asSingletonView();
		final PCollectionView<String> userView = user.asSingletonView();

		// Create a pipeline parameterized by commandline flags.
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(arg));

		//read input

		/////////////////////////////			/////
		// What are we inputting and outputting?? ///
		//////////////////////////////			/////
		//p.apply(TextIO.Read.from("gs://..."));


		// filter based on start time
			//returns PCollection of route object things
		// filter based on start station
			//returns PCollection of route object things
		p.apply(ParDo.withSideInputs(timeRangeStartView, timeRangeEndView, userView).of(new FilterTimeStation()));

		// reduce to just a list of start/end id hashes
			//returns a PCollection of just hashes
		p.apply(ParDo.of(new DoFn<PCollection<String>, PCollection<Object>>() {
			public void processElement(ProcessContext c) {
				Object curr = c.element();
				String hash = curr.startStation + curr.endStation;
				c.output(hash);
		}}));
		// count duplicates
			//returns a PCollection of key/value pairs
			// key: start/end hash
			// value: count
		//returns PCollection<KV<String, Long>>
		p.apply(Count.<String>perElement());


		// output this collection of key/value pairs
		/////////////////////////////				/////
		// What are we inputting and outputting?? 	  ///
		// in this case we can either output 		  ///
		// the hash or split it into an Object 		  ///
		// or something easily readable by the        ///
		// UI, or the UI can do that on visualization ///
		//////////////////////////////		 	    /////

		//p.apply(TextIO.Write.to("gs://..."));   // Write output.

		// Run the pipeline.
		p.run();
	}


	
	static class FilterTimeUser extends DoFn<PCollection<Object>, PCollection<Object>> {
		public void processElement(ProcessContext c) {
			Object curr = c.element();
			// In our DoFn, access the side input.
			int start = c.sideInput(timeRangeStartView);
			int end = c.sideInput(timeRangeEndView);
			String user = c.sideInput(userView);
			if (start <= curr.startTime && curr.startTime <= end) {
				if(curr.user == user){
					c.output(curr);
				}
			}
		}

	}
	
}

package com.aamir.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;





public class EssentialStreams {
	
    private void applicationTemplate() throws Exception {
    
    //start the stream execution environment	
      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
      //in the mid any computation
		DataStreamSource<Integer> intStream = env.fromElements(1,2,3,4);
		
		//print the stream
		intStream.print();
		
		//at the end	
        env.execute();
    }
    
    private void demoTransformation() throws Exception {
    	//start the stream execution environment	
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  		
        //in the mid any computation
  		DataStreamSource<Integer> intStream = env.fromElements(1,2,3,4);
  		
  		//doubled Numbers
  		DataStream<Integer> doubledNumbers = intStream.map(x -> x*2);
  		
  		DataStream<Integer> expandedNumbers = intStream.flatMap(new FlatMapFunction<Integer,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(Integer value, org.apache.flink.util.Collector<Integer> out) throws Exception {
				// TODO Auto-generated method stub
				out.collect(value);
				out.collect(value+1);
			}
  		});
  		//print the stream
  		intStream.print();
  		
  		DataStream<Integer> filterdNumbers = intStream.filter(x -> x % 2 == 0);
  		
  		//expandedNumbers.writeAsText("output/expandedStrea.txt");
  		
//  		final StreamingFileSink<String> sink = StreamingFileSink
//                .forRowFormat(new Path("/output/expanded"), new SimpleStringEncoder<String>("UTF-8"))
//                .build(); 
//  		
//  		
//  		final FileSink<String> sink1 = FileSink
//                .<String>forRowFormat(new Path("/output/expandedCheck"), new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(Duration.ofMinutes(15))
//                                .withInactivityInterval(Duration.ofMinutes(5))
//                                .withMaxPartSize(1024 * 1024 * 1024) // 1 GB
//                                .build())
//                .build();
//  		
  		//at the end	
          env.execute();
    }
    
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//new EssentialStreams().applicationTemplate();
		new EssentialStreams().demoTransformation();
		
	}

}

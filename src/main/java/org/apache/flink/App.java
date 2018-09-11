package org.apache.flink;

import java.sql.Timestamp;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class App 
{
	public static String path = "C:/Users/rdg3/OneDrive/Schweden/Uni/data driven support for cyber-physical systems/Project/testdata.txt";
    
	public static class DataRecord{
		private Timestamp date;
		private float consumption;
		private String method;
		
		public DataRecord() {}
		
		public DataRecord(String datetime, float consumption, String method) {
			datetime = datetime.replace(".", "-");
			String datetimeArray[] = datetime.split(" ");
			String dateArray[] = datetimeArray[0].split("-");
			String date = dateArray[2]+"-"+dateArray[1]+"-"+dateArray[0] + " " + datetimeArray[1];
			this.date = Timestamp.valueOf(date);
			this.consumption = consumption;
			this.method = method;
		}

		public float getConsumption() {
			return consumption;
		}

		public void setConsumption(float consumption) {
			this.consumption = consumption;
		}

		public String getMethod() {
			return method;
		}

		public void setMethod(String method) {
			this.method = method;
		}
		
		public Timestamp getDate() {
			return date;
		}

		public void setDate(Timestamp date) {
			this.date = date;
		}
		
		public String toString() {
			return "" + date + " ; " + consumption + " ; " + method;
		}
	}
	
	public static void main( String[] args ) throws Exception
    {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> text = env.readTextFile(path);
		
		text = text.filter(new FilterFunction<String>() {

			@Override
			public boolean filter(String value) throws Exception {
				return value.contains("2018");
			}
			
		});
		
		DataStream<DataRecord> dataset = text.flatMap(new FlatMapFunction<String, DataRecord>(){

			@Override
			public void flatMap(String value, Collector<DataRecord> out) throws Exception {
				String[] data = value.split(";");
				float f = -1;
				try {
					data[1] = data[1].replace(",", ".");
					f = Float.parseFloat(data[1]);
				} catch (Exception e) {
					return;
				}
				if(data.length < 3) {
					return;
				}else {
					DataRecord dr = new DataRecord(data[0], f, data[2]);
					out.collect(dr);
				}
			}
			
		});
		
		dataset.print();
		/*
		DataStream<DataRecord> filtered = dataset.filter(new FilterFunction<DataRecord>() {
			@Override
			public boolean filter(DataRecord value) throws Exception{
				return value.getMethod().equals("Beräknat");
			}
		});
		
		filtered.print();	*/	
		
		env.execute();
	}
}

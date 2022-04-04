package main;
import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

public class ParquetFileRead {

	public static void main(String[] args) {
		readParquetFile();
	}

	private static void readParquetFile() {
		ParquetReader<GenericData.Record> reader = null;
		Path path = new Path("devel_100_000_tfidf_results.parquet");
		try {
			reader = AvroParquetReader
					.<GenericData.Record>builder(path)
					.withConf(new Configuration())
					.build();
			GenericData.Record record;
			while ((record = reader.read()) != null) {
				System.out.println(record);
			}
		}catch(IOException e) {
			e.printStackTrace();
		}finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
package main;
import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

public class ParquetFileRead {

	public static void main(String[] args) {

		ParquetReader<GenericData.Record> reader = null;
		Path path = new Path("devel_10_000_tfidf_results.parquet");
		Configuration conf = new Configuration();
		try {
			InputFile in = HadoopInputFile.fromPath(path, conf);
			reader = AvroParquetReader
					.<GenericData.Record>builder(in)
					.withConf(conf)
					.build();
			GenericData.Record record;
			while ((record = reader.read()) != null) {
//				System.out.println(record);
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
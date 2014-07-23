package org.collamine.hbasestarter;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 * copy tables
 */
public class HBaseMapReduce {

    public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put>  {

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            // this example is just copying the data from the source table...
            context.write(row, resultToPut(row,value));
        }

        private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
            Put put = new Put(key.get());
            for (KeyValue kv : result.raw()) {
                put.add(kv);
            }
            return put;
        }
    }
    
    public static void main(String[] args) throws Exception {
        String sourceTable = args[0];
        String targetTable = args[1];

        HBaseConfiguration conf = new HBaseConfiguration();
        Job job = new Job(conf, "table copy");
        job.setJarByClass(HBaseMapReduce.class);
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        // set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
            sourceTable,      // input table
            scan,             // Scan instance to control CF and attribute selection
            MyMapper.class,   // mapper class
            null,             // mapper output key
            null,             // mapper output value
            job);
        TableMapReduceUtil.initTableReducerJob(
            targetTable,      // output table
            null,             // reducer class
            job);
        job.setNumReduceTasks(0);        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
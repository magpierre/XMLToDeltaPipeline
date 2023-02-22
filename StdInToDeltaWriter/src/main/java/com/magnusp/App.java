package com.magnusp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.slf4j.Logger;
import com.magnusp.Levels.Builder;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.*;
import io.delta.standalone.exceptions.DeltaConcurrentModificationException;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;

public class App {



    public static void main(String[] args) {
        Logger logger = org.slf4j.LoggerFactory.getLogger(App.class);
        if (args.length <= 0) {
            logger.error("No name for output parquet file.");
            return;
        }
        Scanner scanner = new Scanner(System.in);
        scanner.useDelimiter(Pattern.compile(",|\n"));
        // Use the generated class from avro schema
        Schema avroSchema = DataExport.getClassSchema();

        Path filePath = new Path(args[0]);
        Configuration conf = new Configuration();
        conf.addResource(new Path(args[1]));
        long val = 1;
        // Get or create a Delta Table
        DeltaLog log = DeltaLog.forTable(conf, filePath);
        Operation op = new Operation(Operation.Name.WRITE);
        // Initiate the transaction
        OptimisticTransaction txn = log.startTransaction();
        List<Action> totalCommitFiles = new ArrayList<>();
        if (log.tableExists()) {
            op = new Operation(Operation.Name.WRITE);
            Snapshot s = log.update();
            List<AddFile> l = s.getAllFiles();
            for (int i = 0; i < l.size(); i++) {
                String fileName = new Path(l.get(i).getPath()).getName();
                String numberOnly = fileName.replaceAll("[^0-9]", "");
                long current = Long.parseLong(numberOnly);
                if (current > val) {
                    val = current;
                }
                // Remove the prior files since we replace the content of the table
                totalCommitFiles.add(l.get(i).remove());
            }
            val++;
        }
        try {
            String blockFile = args[0] + "/part-" + String.format("%05d", val) + ".snappy.parquet";
            OutputFile output = HadoopOutputFile.fromPath(new Path(blockFile), conf);
            /* Write data to parquet file */
            ParquetWriter<Object> writer = AvroParquetWriter.builder(output)
                    .withConf(conf)
                    .withSchema(avroSchema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY).build();
            com.magnusp.DataExport e = new com.magnusp.DataExport();
            long rownr = 1L;
            while (scanner.hasNextLine()) {
                String str[] = scanner.nextLine().split(scanner.delimiter().pattern());
                e = new DataExport();
                e.setRownr(rownr++);
                String[] levels = str[0].split("\\¶");
                e.setLevels(createLevelStatement(levels));
                if(str.length < 2) {

                    System.out.println("Str:" + str[0]);
                }
                e.setKey(str[1]);
                e.setValue(str[2].replace("¡", ","));
                writer.write(e);
            }
            writer.close();
            /* Get File size from just written block */
            FileSystem fs = filePath.getFileSystem(conf);
            FileStatus[] fstat = fs.listStatus(new Path(blockFile));
            long size = fstat[0].getLen();
            /* Create transaction */
            totalCommitFiles.add(
                    new AddFile(
                            blockFile,
                            new HashMap<String, String>(),
                            size,
                            System.currentTimeMillis(),
                            true,
                            null,
                            new HashMap<String, String>()));
            // Add metadata to the transaction
            StructType schema = new StructType()
                    .add("rownr",  new LongType())
                    .add("levels", new StructType()
                        .add("L1", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L2", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L3", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L4", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L5", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L6", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L7", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L8", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L9", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L10", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L11", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L12", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L13", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L14", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L15", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L16", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L17", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L18", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L19", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType()))
                        .add("L20", new StructType().add("node", new StringType()).add("attr_key", new StringType()).add("attr_value", new StringType())))
                    .add("key", new StringType())
                    .add("value", new StringType());

            txn.updateMetadata(Metadata.builder().schema(schema).build());
            /* commit new files and remove old files */
            txn.commit(totalCommitFiles, op, "Magnus Pierre - XML Ingester/1.0.0");
        } catch (IOException e) {
            scanner.close();
            System.out.println(e.getMessage());
            e.printStackTrace();
            logger.error(e.getMessage());
            return;
        } catch (DeltaConcurrentModificationException e) {
            scanner.close();
            logger.error(e.getMessage());
            return;
        } catch (UnsupportedOperationException e) {

        }

        scanner.close();
    }

    private static Levels createLevelStatement(String[] levels) {
        Builder b = Levels.newBuilder();
        for (int i = 0; i < levels.length; i++) {
            String[] NodeAttr = levels[i].split("\\[|\\]");
            com.magnusp.Level.Builder blevel = Level.newBuilder().setNode(NodeAttr[0]);
            if(NodeAttr.length > 1) {
                String[] kval = NodeAttr[1].split("=");
                blevel = blevel.setAttrKey(kval[0]);
                if(kval.length > 1) {
                    blevel = blevel.setAttrValue(kval[1].replace("¡", ","));
                }
            }
            Level level = blevel.build();
            switch (i) {
                case 0:
                    b.setL1(level);
                    break;
                case 1:
                    b.setL2(level);
                    break;
                case 2:
                    b.setL3(level);
                    break;
                case 3:
                    b.setL4(level);
                    break;
                case 4:
                    b.setL5(level);
                    break;
                case 5:
                    b.setL6(level);
                    break;
                case 6:
                    b.setL7(level);
                    break;
                case 7:
                    b.setL8(level);
                    break;
                case 8:
                    b.setL9(level);
                    break;
                case 9:
                    b.setL10(level);
                    break;
                case 10:
                    b.setL11(level);
                    break;
                case 11:
                    b.setL12(level);
                    break;
                case 12:
                    b.setL13(level);
                    break;
                case 13:
                    b.setL14(level);
                    break;
                case 14:
                    b.setL15(level);
                    break;
                case 15:
                    b.setL16(level);
                    break;
                case 16:
                    b.setL17(level);
                    break;
                case 17:
                    b.setL18(level);
                    break;
                case 18:
                    b.setL19(level);
                    break;
                case 19:
                    b.setL20(level);
                    break;
            }
        }
        return b.build();
    }
}

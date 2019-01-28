package fileio;// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class FileIOCsv2Avro {

    public static void main(String[] args) {
        //runLocal();
        runS3();
    }

    private static Properties loadConfig() {
        Properties prop = new Properties();
        try (InputStream is = new FileInputStream(new File("aws.conf"))) {
            prop.load(is);
        } catch (Exception e) {
            System.err.println("Err : " + e.getMessage());
            e.printStackTrace();
        }

        return prop;
    }

    private static void runLocal() {
        PipelineOptions noOption = PipelineOptionsFactory.create();

        // Due to beam bug need to give aws region
        // since I have  'beam-sdks-java-io-amazon-web-services' in dependencies
        S3Options s3o = PipelineOptionsFactory.as(S3Options.class);
        s3o.setAwsRegion("eu-west-1");

        csv2avro(s3o, //noOption,
                "C:\\Devx\\Tmp\\Resources\\persons.csv");
    }

    private static void runS3() {
        // Configure AWS credentials
        Properties prop = loadConfig();
        BasicAWSCredentials baseCred =
                new BasicAWSCredentials(prop.getProperty("access_key"), prop.getProperty("secret_key"));
        System.out.println("Credentials : " + prop.getProperty("access_key") + "/" + prop.getProperty("secret_key"));

        AWSStaticCredentialsProvider cred = new AWSStaticCredentialsProvider(baseCred);
        S3Options s3o = PipelineOptionsFactory.as(S3Options.class);
        s3o.setAwsRegion(prop.getProperty("region"));
        s3o.setS3UploadBufferSizeBytes(5_242_880);
        s3o.setAwsCredentialsProvider(cred);

        csv2avro(s3o, "s3://ypiel/beam/persons.csv");
    }

    private static void csv2avro(PipelineOptions options, String pattern) {
        Pipeline p = Pipeline.create(options);
        //p.apply("Match", FileIO.match().filepattern(pattern));

        PCollection<FileIO.ReadableFile> files = p.apply(FileIO.match().filepattern(pattern))
                /*.continuously(Duration.standardSeconds(30),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardMinutes(2))))*/
                .apply("To Readable", FileIO.readMatches()); //.withCompression(GZIP));

        files.apply("to_string", TextIO.readFiles())
                .apply("filter_ignore_empty", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void onElement(@Element final String element, final OutputReceiver<String> out) {
                        if (!element.isEmpty()) {
                            out.output(element);
                        }
                    }
                }))
                .apply("map", ParDo.of(new DoFn<String, GenericRecord>() {
                    @ProcessElement
                    public void onElement(@Element final String element, final OutputReceiver<GenericRecord> out) {
                        final String[] segments = element.split(";"); // todo: use csv parser
                        out.output(Records.create(segments[0], Integer.parseInt(segments[1])));
                    }
                })).setCoder(AvroCoder.of(Records.SCHEMA))
                .apply(FileIO.<GenericRecord>write().to("c:\\beamout\\avro\\output")
                        .via(AvroIO.sinkViaGenericRecords(Records.SCHEMA, (AvroIO.RecordFormatter<GenericRecord>) (element, schema) -> element)));


        // Execute the pipeline
        p.run().waitUntilFinish();

    }

    private static class Records {

        public final static Schema SCHEMA = SchemaBuilder.record("fileio.FileIOCsv2Avro.Record")
                                                .fields().nullableString("name", "")
                                                .nullableInt("age", 0)
                                                .endRecord();

        static GenericRecord create(final String name, final int age) {
            final GenericData.Record record = new GenericData.Record(SCHEMA);
            record.put(0, name);
            record.put(1, age);
            return record;
        }
    }

}

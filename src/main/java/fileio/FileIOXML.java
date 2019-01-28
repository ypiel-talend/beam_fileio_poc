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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class FileIOXML {

    public static void main(String[] args) {
        runLocal();
        //runS3();
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

        readXML(s3o, //noOption,
                "C:\\Devx\\Tmp\\Resources\\tdi-nantes.xml");
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

        readXML(s3o, "s3://ypiel/beam/tdi-nantes.xml");
    }

    private static void readXML(PipelineOptions options, String pattern) {
        Pipeline p = Pipeline.create(options);
        p.apply("Configuration", FileIO.match().filepattern(pattern));

        PCollection<FileIO.ReadableFile> files = p.apply(FileIO.match().filepattern(pattern))
                /*.continuously(Duration.standardSeconds(30),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardMinutes(2))))*/
                .apply(FileIO.readMatches()); //.withCompression(GZIP));

        files.apply(XmlIO.<Record>readFiles().withRootElement("root")
                .withRecordElement("record")
                .withRecordClass(Record.class))
                .apply("get Names", ParDo.of(new DoFn<Record, String>() {

                    @DoFn.ProcessElement
                    public void processElement(
                            @DoFn.Element
                                    Record person, OutputReceiver<String> name) throws Exception {
                        name.output(person.getName());
                    }

                }))
                .apply("Count by name", Count.perElement())
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + "," + wordCount.getValue()))

                // those two implementations work almost the same with differents output files
                //.apply(FileIO.write().to("c:\\beamout\\names").via((FileIO.Sink)TextIO.sink()));
                .apply(TextIO.write().to("c:\\beamout\\names"));


        // Execute the pipeline
        p.run().waitUntilFinish();

    }

    @SuppressWarnings("unused")
    @XmlRootElement(name = "record")
    private static class Record {

        private String name;

        private int age;

        private String comment;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }
    }

}
